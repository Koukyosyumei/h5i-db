//! Shell around [`lab_kernel`]. It moves bytes between the kernel, PostgreSQL,
//! and the h5i-db worker. It does not decide who may do what.

mod deflated;
pub mod market;
mod measure;
mod runner;
pub mod strategy;
mod worker;

use std::sync::{Arc, Mutex};

use axum::extract::{FromRef, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use h5i_app_core::{Kernel, TenantId};
use h5i_app_http::{Actor, Api, Auth, error_body, reply};
use h5i_app_json::Value as Out;
use h5i_app_pg::outbox::{self, Deliver, Delivery};
use h5i_app_pg::{DbError, Engine, ReplyCodec, Store, Tx};
use lab_kernel as k;
use serde::Deserialize;

pub use runner::{
    Activity, AgentProc, BoxSpec, Runner, SandboxMode, activity, looks_looping, looks_stalled,
    pause_agent, resume_agent, start_agent, start_shell, stop_agent, wait_agent,
};
pub use worker::{
    DbWorker, STARTING_CASH, code_identity, evaluate_holdout_run, holdout_passed, kernel_claims,
};

pub struct Lab;

impl Kernel for Lab {
    type Principal = k::Principal;
    type Snapshot = k::Snapshot;
    type Command = k::Command;
    type WriteSet = Vec<k::Write>;
    type Reply = k::Reply;
    type Error = k::Error;

    fn tenant(actor: &k::Principal) -> TenantId {
        TenantId(actor.org)
    }

    fn transition(
        actor: &k::Principal,
        snap: &k::Snapshot,
        cmd: &k::Command,
    ) -> Result<(Vec<k::Write>, k::Reply), k::Error> {
        k::transition(actor, snap, cmd)
    }

    fn apply(snap: &k::Snapshot, ws: &Vec<k::Write>) -> k::Snapshot {
        k::apply(snap, ws)
    }
}

/// Pack a role and an id into the token's user field.
///
/// The h5i-app token carries two integers. The high bits are the role and the
/// low bits are the human or agent id, so a token cannot name a role the
/// issuer did not sign.
pub fn token_user(role: u64, id: u64) -> u64 {
    (role << 60) | (id & ((1 << 60) - 1))
}

pub fn principal_from_token(org: u64, packed: u64) -> k::Principal {
    let role = packed >> 60;
    let id = packed & ((1 << 60) - 1);
    k::Principal {
        org,
        user: id,
        role,
        agent: if role == k::ROLE_AGENT { id } else { 0 },
    }
}

k::lab_tables!(Lab);

pub struct LabStore;

impl Store<Lab> for LabStore {
    fn ddl() -> Vec<String> {
        schema_ddl()
    }

    fn tables() -> Vec<&'static str> {
        schema_tables()
    }

    async fn load(tx: &Tx<'_>, t: TenantId) -> Result<k::Snapshot, DbError> {
        schema_load(tx, t).await
    }

    async fn write(tx: &Tx<'_>, t: TenantId, ws: &Vec<k::Write>) -> Result<(), DbError> {
        schema_store(tx, t, ws).await?;
        for w in ws {
            if let k::Write::Emit(effect) = w {
                let payload = format!("{}:{}", effect.kind, effect.id).into_bytes();
                outbox::enqueue(tx, t, effect.dest, &payload).await?;
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case", deny_unknown_fields)]
enum CommandJson {
    Enroll {
        agent: u64,
        budget: u64,
        token_budget: u64,
        sandbox: u64,
        #[serde(default)]
        name: String,
    },
    PreRegister {
        hypothesis: String,
        method: String,
    },
    /// `code` is a program in the strategy language ([`strategy`]) and
    /// `params` a JSON object of numbers. Both are checked before the ledger
    /// sees them; the parameters are stored in one canonical text.
    RequestTrial {
        family: u64,
        name: String,
        code: String,
        #[serde(default)]
        params: serde_json::Value,
        decision_time: u64,
    },
    RecordTrial {
        pending: u64,
        data_version: u64,
        score: u64,
        corrected: u64,
        sharpe: u64,
        skew: u64,
        kurt: u64,
        samples: u64,
        tokens: u64,
        #[serde(default)]
        curve: String,
    },
    Freeze {
        family: u64,
        trial: u64,
    },
    SubmitHoldout {
        family: u64,
    },
    EvaluateHoldout {
        family: u64,
        passed: u64,
    },
    Promote {
        family: u64,
    },
    Reject {
        family: u64,
        text: String,
    },
    Comment {
        family: u64,
        text: String,
    },
    NoteAlert {
        kind: String,
        detail: String,
    },
    Report {
        family: u64,
    },
    Fleet,
}

/// Longest strategy name, hypothesis, or method name, in bytes.
const MAX_LABEL: usize = 4_096;

impl TryFrom<CommandJson> for k::Command {
    type Error = String;

    fn try_from(c: CommandJson) -> Result<Self, String> {
        Ok(match c {
            CommandJson::Enroll {
                agent,
                budget,
                token_budget,
                sandbox,
                name,
            } => k::Command::Enroll {
                agent,
                budget,
                token_budget,
                sandbox,
                name: name.trim().as_bytes().to_vec(),
            },
            CommandJson::PreRegister { hypothesis, method } => {
                if hypothesis.len() > MAX_LABEL || method.len() > MAX_LABEL {
                    return Err("hypothesis and method are at most 4096 bytes".into());
                }
                k::Command::PreRegister {
                    hypothesis: hypothesis.trim().as_bytes().to_vec(),
                    method: method.trim().as_bytes().to_vec(),
                }
            }
            CommandJson::RequestTrial {
                family,
                name,
                code,
                params,
                decision_time,
            } => {
                strategy::Program::compile(&code, &params)?;
                if name.trim().is_empty() || name.len() > 64 {
                    return Err("a strategy name is 1 to 64 bytes".into());
                }
                k::Command::RequestTrial {
                    family,
                    name: name.trim().as_bytes().to_vec(),
                    code: code.into_bytes(),
                    params: strategy::canonical_params(&params)?.into_bytes(),
                    decision_time,
                }
            }
            CommandJson::Freeze { family, trial } => k::Command::Freeze { family, trial },
            CommandJson::SubmitHoldout { family } => k::Command::SubmitHoldout { family },
            CommandJson::RecordTrial {
                pending,
                data_version,
                score,
                corrected,
                sharpe,
                skew,
                kurt,
                samples,
                tokens,
                curve,
            } => k::Command::RecordTrial {
                pending,
                data_version,
                score,
                corrected,
                sharpe,
                skew,
                kurt,
                samples,
                tokens,
                curve: curve.into_bytes(),
            },
            CommandJson::EvaluateHoldout { family, passed } => {
                k::Command::EvaluateHoldout { family, passed }
            }
            CommandJson::Promote { family } => k::Command::Promote { family },
            CommandJson::Reject { family, text } => k::Command::Reject {
                family,
                text: text.into_bytes(),
            },
            CommandJson::Comment { family, text } => k::Command::Comment {
                family,
                text: text.into_bytes(),
            },
            CommandJson::NoteAlert { kind, detail } => k::Command::NoteAlert {
                kind: kind.into_bytes(),
                detail: detail.into_bytes(),
            },
            CommandJson::Report { family } => k::Command::Report { family },
            CommandJson::Fleet => k::Command::Fleet,
        })
    }
}

fn text_of(texts: &[k::Text], id: u64) -> Option<&k::Text> {
    texts.iter().find(|row| row.id == id)
}

fn body_of(texts: &[k::Text], id: u64) -> String {
    text_of(texts, id)
        .map(|row| String::from_utf8_lossy(&row.body).into_owned())
        .unwrap_or_default()
}

fn kind_name(kind: u64) -> &'static str {
    match kind {
        k::TEXT_HYPOTHESIS => "hypothesis",
        k::TEXT_METHOD => "method",
        k::TEXT_CODE => "code",
        k::TEXT_PARAMS => "params",
        k::TEXT_AGENT => "agent",
        _ => "other",
    }
}

/// The agent's name. Label 0 is no name.
fn agent_name(a: &k::Agent, texts: &[k::Text]) -> String {
    match text_of(texts, a.label) {
        Some(row) if a.label != 0 && row.kind == k::TEXT_AGENT => {
            String::from_utf8_lossy(&row.body).into_owned()
        }
        _ => String::new(),
    }
}

fn agent_json(a: &k::Agent, texts: &[k::Text]) -> Out {
    Out::obj([
        ("id", a.id.into()),
        ("name", Out::str(agent_name(a, texts))),
        ("budget", a.budget.into()),
        ("used", a.used.into()),
        ("token_budget", a.token_budget.into()),
        ("tokens_used", a.tokens_used.into()),
        ("fork_id", a.fork_id.into()),
        ("sandbox", a.sandbox.into()),
        (
            "trust",
            Out::str(if a.sandbox == 0 {
                "advisory"
            } else {
                "enforced"
            }),
        ),
    ])
}

fn family_json(f: &k::Family, texts: &[k::Text]) -> Out {
    // Frozen code 0 is not frozen.
    let frozen_name = text_of(texts, f.frozen_code)
        .filter(|_| f.frozen_code != 0)
        .map(|row| String::from_utf8_lossy(&row.name).into_owned())
        .unwrap_or_default();
    Out::obj([
        ("id", f.id.into()),
        ("agent", f.agent.into()),
        ("task", f.task.into()),
        ("hypothesis", f.hypothesis.into()),
        ("hypothesis_text", Out::str(body_of(texts, f.hypothesis))),
        ("method", f.method.into()),
        ("method_text", Out::str(body_of(texts, f.method))),
        ("trials", f.trials.into()),
        ("corrected", f.corrected.into()),
        ("frozen_code", f.frozen_code.into()),
        ("frozen_name", Out::str(frozen_name)),
        ("frozen_params", f.frozen_params.into()),
        (
            "frozen_params_text",
            Out::str(if f.frozen_code != 0 {
                body_of(texts, f.frozen_params)
            } else {
                String::new()
            }),
        ),
        ("frozen_data", f.frozen_data.into()),
        ("holdout", f.holdout.into()),
        ("promoted", f.promoted.into()),
        ("rejected", f.rejected.into()),
    ])
}

fn trial_json(t: &k::Trial, texts: &[k::Text], curves: &[k::Curve]) -> Out {
    let code_name = text_of(texts, t.code)
        .map(|row| String::from_utf8_lossy(&row.name).into_owned())
        .unwrap_or_default();
    let curve = curves
        .iter()
        .find(|row| row.id == t.id)
        .map(|row| String::from_utf8_lossy(&row.body).into_owned())
        .unwrap_or_default();
    Out::obj([
        ("id", t.id.into()),
        ("family", t.family.into()),
        ("agent", t.agent.into()),
        ("code", t.code.into()),
        ("code_name", Out::str(code_name)),
        ("params", t.params.into()),
        ("params_text", Out::str(body_of(texts, t.params))),
        ("data_version", t.data_version.into()),
        ("decision_time", t.decision_time.into()),
        ("score", t.score.into()),
        ("sharpe", t.sharpe.into()),
        ("skew", t.skew.into()),
        ("kurt", t.kurt.into()),
        ("samples", t.samples.into()),
        ("tokens", t.tokens.into()),
        ("curve", Out::str(curve)),
    ])
}

fn text_json(row: &k::Text) -> Out {
    let mut fields = vec![
        ("id", row.id.into()),
        ("kind", Out::str(kind_name(row.kind))),
        ("name", Out::str(&row.name)),
        ("body", Out::str(&row.body)),
    ];
    if row.kind == k::TEXT_CODE {
        fields.push(("identity", Out::str(code_identity(&row.body))));
    }
    Out::obj(fields)
}

impl h5i_app_http::Api<Lab> for LabStore {
    fn decode_command(body: serde_json::Value) -> Result<k::Command, String> {
        serde_json::from_value::<CommandJson>(body)
            .map_err(|e| e.to_string())?
            .try_into()
    }

    fn encode_reply(r: &k::Reply) -> Out {
        match r {
            k::Reply::Ack => Out::obj([("ok", Out::str("true"))]),
            k::Reply::Created(id) => Out::obj([("id", (*id).into())]),
            k::Reply::Metric { trials, corrected } => Out::obj([
                ("trials", (*trials).into()),
                ("corrected", (*corrected).into()),
            ]),
            k::Reply::Fleet {
                agents,
                families,
                trials,
                texts,
                curves,
            } => Out::obj([
                (
                    "agents",
                    Out::Arr(agents.iter().map(|a| agent_json(a, texts)).collect()),
                ),
                (
                    "families",
                    Out::Arr(families.iter().map(|f| family_json(f, texts)).collect()),
                ),
                (
                    "trials",
                    Out::Arr(
                        trials
                            .iter()
                            .map(|t| trial_json(t, texts, curves))
                            .collect(),
                    ),
                ),
                ("texts", Out::Arr(texts.iter().map(text_json).collect())),
            ]),
        }
    }

    fn encode_error(e: &k::Error) -> (StatusCode, Out) {
        let (status, code) = match e {
            k::Error::Forbidden => (StatusCode::FORBIDDEN, "forbidden"),
            k::Error::NotFound => (StatusCode::NOT_FOUND, "not_found"),
            k::Error::Conflict => (StatusCode::CONFLICT, "conflict"),
            k::Error::Budget => (StatusCode::TOO_MANY_REQUESTS, "budget"),
            k::Error::Overflow => (StatusCode::UNPROCESSABLE_ENTITY, "overflow"),
            k::Error::Invalid => (StatusCode::UNPROCESSABLE_ENTITY, "invalid"),
        };
        (status, error_body(code))
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
enum StoredReply {
    Ack,
    Created(u64),
    Metric {
        trials: u64,
        corrected: u64,
    },
    // Kernel rows stay free of serde. The tuple order is the struct field order.
    Fleet {
        agents: Vec<AgentRow>,
        families: Vec<FamilyRow>,
        trials: Vec<TrialRow>,
        texts: Vec<TextRow>,
        curves: Vec<(u64, Vec<u8>)>,
    },
}

/// An agent row in field order.
type AgentRow = (u64, u64, u64, u64, u64, u64, u64, u64);
/// A text row in field order: id, kind, name, body.
type TextRow = (u64, u64, Vec<u8>, Vec<u8>);

#[derive(serde::Serialize, serde::Deserialize)]
struct FamilyRow {
    id: u64,
    agent: u64,
    task: u64,
    hypothesis: u64,
    method: u64,
    trials: u64,
    corrected: u64,
    frozen_code: u64,
    frozen_params: u64,
    frozen_data: u64,
    holdout: u64,
    promoted: u64,
    rejected: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct TrialRow {
    id: u64,
    family: u64,
    agent: u64,
    code: u64,
    params: u64,
    data_version: u64,
    decision_time: u64,
    score: u64,
    sharpe: u64,
    skew: u64,
    kurt: u64,
    samples: u64,
    tokens: u64,
}

impl ReplyCodec<Lab> for LabStore {
    fn fingerprint(cmd: &k::Command) -> Vec<u8> {
        format!("{cmd:?}").into_bytes()
    }

    fn scope(actor: &k::Principal) -> String {
        format!("r{}-{}", actor.role, actor.user)
    }

    fn encode(r: &k::Reply) -> Vec<u8> {
        let stored = match r {
            k::Reply::Ack => StoredReply::Ack,
            k::Reply::Created(id) => StoredReply::Created(*id),
            k::Reply::Metric { trials, corrected } => StoredReply::Metric {
                trials: *trials,
                corrected: *corrected,
            },
            k::Reply::Fleet {
                agents,
                families,
                trials,
                texts,
                curves,
            } => StoredReply::Fleet {
                agents: agents
                    .iter()
                    .map(|a| {
                        (
                            a.id,
                            a.budget,
                            a.used,
                            a.token_budget,
                            a.tokens_used,
                            a.fork_id,
                            a.sandbox,
                            a.label,
                        )
                    })
                    .collect(),
                texts: texts
                    .iter()
                    .map(|t| (t.id, t.kind, t.name.clone(), t.body.clone()))
                    .collect(),
                curves: curves.iter().map(|c| (c.id, c.body.clone())).collect(),
                families: families
                    .iter()
                    .map(|f| FamilyRow {
                        id: f.id,
                        agent: f.agent,
                        task: f.task,
                        hypothesis: f.hypothesis,
                        method: f.method,
                        trials: f.trials,
                        corrected: f.corrected,
                        frozen_code: f.frozen_code,
                        frozen_params: f.frozen_params,
                        frozen_data: f.frozen_data,
                        holdout: f.holdout,
                        promoted: f.promoted,
                        rejected: f.rejected,
                    })
                    .collect(),
                trials: trials
                    .iter()
                    .map(|t| TrialRow {
                        id: t.id,
                        family: t.family,
                        agent: t.agent,
                        code: t.code,
                        params: t.params,
                        data_version: t.data_version,
                        decision_time: t.decision_time,
                        score: t.score,
                        sharpe: t.sharpe,
                        skew: t.skew,
                        kurt: t.kurt,
                        samples: t.samples,
                        tokens: t.tokens,
                    })
                    .collect(),
            },
        };
        serde_json::to_vec(&stored).expect("stored replies serialize")
    }

    fn decode(bytes: &[u8]) -> Result<k::Reply, String> {
        Ok(
            match serde_json::from_slice(bytes).map_err(|e| e.to_string())? {
                StoredReply::Ack => k::Reply::Ack,
                StoredReply::Created(id) => k::Reply::Created(id),
                StoredReply::Metric { trials, corrected } => k::Reply::Metric { trials, corrected },
                StoredReply::Fleet {
                    agents,
                    families,
                    trials,
                    texts,
                    curves,
                } => k::Reply::Fleet {
                    agents: agents
                        .into_iter()
                        .map(
                            |(
                                id,
                                budget,
                                used,
                                token_budget,
                                tokens_used,
                                fork_id,
                                sandbox,
                                label,
                            )| {
                                k::Agent {
                                    id,
                                    budget,
                                    used,
                                    token_budget,
                                    tokens_used,
                                    fork_id,
                                    sandbox,
                                    label,
                                }
                            },
                        )
                        .collect(),
                    texts: texts
                        .into_iter()
                        .map(|(id, kind, name, body)| k::Text {
                            id,
                            kind,
                            name,
                            body,
                        })
                        .collect(),
                    curves: curves
                        .into_iter()
                        .map(|(id, body)| k::Curve { id, body })
                        .collect(),
                    families: families
                        .into_iter()
                        .map(|f| k::Family {
                            id: f.id,
                            agent: f.agent,
                            task: f.task,
                            hypothesis: f.hypothesis,
                            method: f.method,
                            trials: f.trials,
                            corrected: f.corrected,
                            frozen_code: f.frozen_code,
                            frozen_params: f.frozen_params,
                            frozen_data: f.frozen_data,
                            holdout: f.holdout,
                            promoted: f.promoted,
                            rejected: f.rejected,
                        })
                        .collect(),
                    trials: trials
                        .into_iter()
                        .map(|t| k::Trial {
                            id: t.id,
                            family: t.family,
                            agent: t.agent,
                            code: t.code,
                            params: t.params,
                            data_version: t.data_version,
                            decision_time: t.decision_time,
                            score: t.score,
                            sharpe: t.sharpe,
                            skew: t.skew,
                            kurt: t.kurt,
                            samples: t.samples,
                            tokens: t.tokens,
                        })
                        .collect(),
                },
            },
        )
    }
}

#[derive(Clone)]
pub struct App {
    pub engine: Arc<Engine<Lab, LabStore>>,
    pub auth: Auth<Lab>,
    /// HMAC secret used to mint the one-day token handed to a started agent.
    pub secret: Vec<u8>,
    pub db: Arc<h5i_db_service::Service>,
    pub runner: Arc<Mutex<Runner>>,
    pub seal_id: String,
    pub holdout_table: String,
    /// Ledger URL, including the schema search path. Used to replay dead effects.
    pub ledger_url: String,
}

impl FromRef<App> for Auth<Lab> {
    fn from_ref(app: &App) -> Self {
        app.auth.clone()
    }
}

impl App {
    /// Write an alert as the worker. The kernel keeps one row per kind and detail.
    pub async fn note_alert(&self, org: u64, kind: &str, detail: &str) {
        let kind_b = kind.as_bytes();
        let detail_b = detail.as_bytes();
        if let Ok(snap) = self.engine.snapshot(TenantId(org)).await {
            if snap
                .alerts
                .iter()
                .any(|row| row.kind.as_slice() == kind_b && row.detail.as_slice() == detail_b)
            {
                return;
            }
        }
        let actor = k::Principal {
            org,
            user: 1,
            role: k::ROLE_WORKER,
            agent: 0,
        };
        let cmd = k::Command::NoteAlert {
            kind: kind_b.to_vec(),
            detail: detail_b.to_vec(),
        };
        match self.engine.execute(&actor, &cmd).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => tracing::error!(?error, "alert refused"),
            Err(error) => tracing::error!(%error, "alert was not recorded"),
        }
    }
}

/// Latest decision time this agent has registered, as a handle ceiling.
///
/// Pending requests and recorded trials both count. An agent with neither
/// has no ceiling, and a query is refused instead of widening to "now".
pub fn agent_ceiling_ns(snap: &k::Snapshot, agent: u64) -> Option<i64> {
    let mut best: Option<u64> = None;
    for row in snap
        .pendings
        .iter()
        .filter(|row| row.agent == agent)
        .map(|row| row.decision_time)
        .chain(
            snap.trials
                .iter()
                .filter(|row| row.agent == agent)
                .map(|row| row.decision_time),
        )
    {
        best = Some(best.map(|have| have.max(row)).unwrap_or(row));
    }
    best.and_then(|value| i64::try_from(value).ok())
}

/// Three or more trials with the same family, code and parameters.
pub fn burst_details(trials: &[k::Trial]) -> Vec<String> {
    let mut counts: Vec<((u64, u64, u64), u64)> = Vec::new();
    for trial in trials {
        let key = (trial.family, trial.code, trial.params);
        if let Some(slot) = counts.iter_mut().find(|(found, _)| *found == key) {
            slot.1 += 1;
        } else {
            counts.push((key, 1));
        }
    }
    counts
        .into_iter()
        .filter(|(_, count)| *count >= 3)
        .map(|((family, code, params), count)| {
            format!("family {family} repeated code {code} params {params} {count} times")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn three_identical_trials_are_a_burst() {
        let trial = |id| k::Trial {
            id,
            family: 1,
            agent: 1,
            code: 4,
            params: 5,
            data_version: 1,
            decision_time: 1,
            score: 1,
            sharpe: 0,
            skew: 0,
            kurt: 0,
            samples: 0,
            tokens: 0,
        };
        assert!(burst_details(&[trial(1), trial(2)]).is_empty());
        let details = burst_details(&[trial(1), trial(2), trial(3)]);
        assert_eq!(details.len(), 1);
        assert!(details[0].contains("family 1"));
    }

    #[test]
    fn the_ceiling_is_the_latest_registered_decision_time() {
        let snap = k::Snapshot {
            pendings: vec![k::Pending {
                id: 1,
                family: 1,
                agent: 4,
                code: 1,
                params: 1,
                decision_time: 20,
                open: 1,
            }],
            trials: vec![k::Trial {
                id: 2,
                family: 1,
                agent: 4,
                code: 1,
                params: 1,
                data_version: 1,
                decision_time: 50,
                score: 1,
                sharpe: 0,
                skew: 0,
                kurt: 0,
                samples: 0,
                tokens: 0,
            }],
            ..k::Snapshot::default()
        };
        assert_eq!(agent_ceiling_ns(&snap, 4), Some(50));
        assert_eq!(agent_ceiling_ns(&snap, 9), None);
    }

    #[test]
    fn the_leaderboard_and_the_review_page_expose_the_stored_correction() {
        let family = k::Family {
            id: 1,
            agent: 1,
            task: 1,
            hypothesis: 1,
            method: 1,
            trials: 2,
            corrected: 250_000,
            frozen_code: 0,
            frozen_params: 0,
            frozen_data: 0,
            holdout: 0,
            promoted: 0,
            rejected: 0,
        };
        let text = String::from_utf8(
            LabStore::encode_reply(&k::Reply::Fleet {
                agents: vec![],
                families: vec![family],
                trials: vec![],
                texts: vec![],
                curves: vec![],
            })
            .to_bytes(),
        )
        .unwrap();
        assert!(text.contains("\"corrected\":250000"), "{text}");
        assert!(text.contains("\"trials\":2"), "{text}");
        assert!(!text.contains("\"best\""), "{text}");
        assert!(!text.contains("\"reported\""), "{text}");
        let metric = String::from_utf8(
            LabStore::encode_reply(&k::Reply::Metric {
                trials: 2,
                corrected: 250_000,
            })
            .to_bytes(),
        )
        .unwrap();
        assert!(metric.contains("\"trials\":2"), "{metric}");
        assert!(metric.contains("\"corrected\":250000"), "{metric}");
        assert!(!metric.contains("score"), "{metric}");
        assert!(INDEX_HTML.contains("f.corrected"));
        assert!(INDEX_HTML.contains("t.score"));
        assert!(!INDEX_HTML.contains("f.best"));
        assert!(!INDEX_HTML.contains("f.reported"));
        assert!(!INDEX_HTML.contains("raw best"));
    }

    #[test]
    fn the_page_can_replay_dead_effects() {
        assert!(INDEX_HTML.contains("id=\"replay\""));
        assert!(INDEX_HTML.contains("/api/outbox/replay"));
    }
}

const INDEX_HTML: &str = include_str!("../static/index.html");

pub fn router(app: App) -> Router {
    Router::new()
        .route("/", get(|| async { axum::response::Html(INDEX_HTML) }))
        .route("/healthz", get(|| async { "ok" }))
        .route("/rpc", post(rpc))
        .route("/api/query", post(query))
        .route("/api/alerts", get(alerts))
        .route("/api/activity", get(activity_route))
        .route("/api/agents/start", post(start))
        .route("/api/agents/stop", post(stop))
        .route("/api/agents/pause", post(pause))
        .route("/api/agents/resume", post(resume))
        .route("/api/comment", post(comment))
        .route("/api/comments", get(comments))
        .route("/api/holdouts", get(holdouts))
        .route("/api/outbox/replay", post(replay_outbox))
        .with_state(app)
}

/// Put dead outbox rows back on the queue.
///
/// The dispatcher skips a row once it is marked dead. This clears that flag
/// and the attempt count, so the row is claimed again. The delivery key is
/// the outbox id, and the worker's idempotency key is derived from it, so a
/// replay of a finished effect returns the first result.
pub async fn replay_dead_outbox(url: &str) -> Result<u64, tokio_postgres::Error> {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::error!(%error, "outbox replay connection ended");
        }
    });
    client
        .execute(
            "UPDATE h5i_outbox \
             SET dead = false, attempts = 0, last_error = NULL, lease_until = NULL \
             WHERE dead",
            &[],
        )
        .await
}

async fn replay_outbox(State(app): State<App>, actor: Actor<Lab>) -> axum::response::Response {
    if actor.principal().role != k::ROLE_HUMAN {
        return reply(StatusCode::FORBIDDEN, error_body("forbidden"));
    }
    match replay_dead_outbox(&app.ledger_url).await {
        Ok(revived) => reply(StatusCode::OK, Out::obj([("revived", revived.into())])),
        Err(error) => {
            tracing::error!(%error, "dead outbox rows were not replayed");
            reply(StatusCode::SERVICE_UNAVAILABLE, error_body("unavailable"))
        }
    }
}

async fn rpc(
    State(app): State<App>,
    actor: Actor<Lab>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> axum::response::Response {
    let cmd = match LabStore::decode_command(body) {
        Ok(cmd) => cmd,
        Err(message) => return reply(StatusCode::BAD_REQUEST, error_body(&message)),
    };
    let key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let org = actor.principal().org;
    let result = match &key {
        Some(k) => {
            app.engine
                .execute_idempotent(actor.principal(), k, &cmd)
                .await
        }
        None => app.engine.execute(actor.principal(), &cmd).await,
    };
    match result {
        Ok(Ok(value)) => reply(StatusCode::OK, LabStore::encode_reply(&value)),
        Ok(Err(refusal)) => {
            if matches!(refusal, k::Error::Forbidden) {
                app.note_alert(
                    org,
                    "denied",
                    &format!("{cmd:?} by role {}", actor.principal().role),
                )
                .await;
            }
            if matches!(refusal, k::Error::Budget) {
                app.note_alert(
                    org,
                    "budget",
                    &format!("{cmd:?} by role {}", actor.principal().role),
                )
                .await;
            }
            let (status, body) = LabStore::encode_error(&refusal);
            reply(status, body)
        }
        Err(DbError::IdempotencyConflict) => reply(
            StatusCode::UNPROCESSABLE_ENTITY,
            error_body("idempotency key reused"),
        ),
        Err(error) => {
            tracing::error!(%error, "ledger request failed");
            reply(StatusCode::SERVICE_UNAVAILABLE, error_body("unavailable"))
        }
    }
}

#[derive(Deserialize)]
struct QueryBody {
    sql: String,
    decision_time_ns: i64,
}

async fn query(
    State(app): State<App>,
    actor: Actor<Lab>,
    Json(body): Json<QueryBody>,
) -> axum::response::Response {
    let principal = actor.principal();
    if principal.role != k::ROLE_AGENT
        && principal.role != k::ROLE_WORKER
        && principal.role != k::ROLE_HUMAN
    {
        app.note_alert(principal.org, "denied", "query from an unknown role")
            .await;
        return reply(StatusCode::FORBIDDEN, error_body("forbidden"));
    }
    // An agent is pinned to its own fork and to the latest decision time it
    // registered. A human or worker query still has no holdout grant.
    let (fork, ceiling) = if principal.role == k::ROLE_AGENT {
        let snap = match app.engine.snapshot(TenantId(principal.org)).await {
            Ok(snap) => snap,
            Err(error) => {
                return reply(
                    StatusCode::SERVICE_UNAVAILABLE,
                    error_body(&error.to_string()),
                );
            }
        };
        let Some(ceiling) = agent_ceiling_ns(&snap, principal.agent) else {
            app.note_alert(
                principal.org,
                "lookahead",
                &format!(
                    "agent {} queried before registering a decision time",
                    principal.agent
                ),
            )
            .await;
            return reply(StatusCode::FORBIDDEN, error_body("scope_denied"));
        };
        if body.decision_time_ns > ceiling {
            app.note_alert(
                principal.org,
                "lookahead",
                &format!(
                    "agent {} asked for {} past ceiling {ceiling}",
                    principal.agent, body.decision_time_ns
                ),
            )
            .await;
            return reply(StatusCode::FORBIDDEN, error_body("scope_denied"));
        }
        (Some(format!("a-{}", principal.agent)), Some(ceiling))
    } else {
        (None, None)
    };
    let handle = match app.db.handle(fork.as_deref(), ceiling).await {
        Ok(handle) => handle,
        Err(error) => {
            app.note_alert(principal.org, "denied", &error.to_string())
                .await;
            return reply(StatusCode::FORBIDDEN, error_body("scope_denied"));
        }
    };
    match handle
        .query(&h5i_db_service::QueryRequest {
            decision_time_ns: body.decision_time_ns,
            sql: body.sql,
            max_rows: Some(100),
        })
        .await
    {
        Ok(result) => reply(
            StatusCode::OK,
            Out::obj([
                (
                    "decision_time_ns",
                    Out::str(result.decision_time_ns.to_string()),
                ),
                (
                    "rows",
                    Out::str(serde_json::to_string(&result.rows).unwrap_or_else(|_| "[]".into())),
                ),
                (
                    "truncated",
                    Out::str(if result.truncated { "true" } else { "false" }),
                ),
            ]),
        ),
        Err(error) => {
            let kind = if error.code() == "scope_denied" {
                "lookahead"
            } else {
                "query"
            };
            app.note_alert(principal.org, kind, &error.to_string())
                .await;
            reply(StatusCode::FORBIDDEN, error_body(error.code()))
        }
    }
}

async fn alerts(State(app): State<App>, actor: Actor<Lab>) -> axum::response::Response {
    let org = actor.principal().org;
    let snap = match app.engine.snapshot(TenantId(org)).await {
        Ok(snap) => snap,
        Err(error) => {
            return reply(
                StatusCode::SERVICE_UNAVAILABLE,
                error_body(&error.to_string()),
            );
        }
    };
    for detail in burst_details(&snap.trials) {
        app.note_alert(org, "duplicate", &detail).await;
    }
    for row in activity(&app.runner) {
        if looks_looping(&row.output) {
            app.note_alert(
                org,
                "looping",
                &format!("agent {} is repeating itself", row.agent),
            )
            .await;
        }
        if looks_stalled(
            row.running && !row.paused,
            row.output.len(),
            row.elapsed,
            std::time::Duration::from_secs(30),
        ) {
            app.note_alert(
                org,
                "stalled",
                &format!("agent {} has no output", row.agent),
            )
            .await;
        }
    }
    let snap = match app.engine.snapshot(TenantId(org)).await {
        Ok(snap) => snap,
        Err(error) => {
            return reply(
                StatusCode::SERVICE_UNAVAILABLE,
                error_body(&error.to_string()),
            );
        }
    };
    let rows: Vec<serde_json::Value> = snap
        .alerts
        .iter()
        .map(|row| {
            serde_json::json!({
                "kind": String::from_utf8_lossy(&row.kind),
                "detail": String::from_utf8_lossy(&row.detail),
            })
        })
        .collect();
    let body = serde_json::to_string(&rows).unwrap_or_else(|_| "[]".into());
    reply(StatusCode::OK, Out::obj([("alerts", Out::str(&body))]))
}

async fn activity_route(State(app): State<App>, _actor: Actor<Lab>) -> axum::response::Response {
    let rows = activity(&app.runner);
    let body = Out::Arr(
        rows.iter()
            .map(|row| {
                Out::obj([
                    ("agent", row.agent.into()),
                    ("trust", Out::str(row.trust)),
                    ("pid", (row.pid as u64).into()),
                    (
                        "running",
                        Out::str(if row.running { "true" } else { "false" }),
                    ),
                    (
                        "paused",
                        Out::str(if row.paused { "true" } else { "false" }),
                    ),
                    ("rss_kb", row.rss_kb.into()),
                    ("output", Out::str(&row.output)),
                ])
            })
            .collect(),
    );
    reply(StatusCode::OK, Out::obj([("agents", body)]))
}

#[derive(Deserialize)]
struct StartBody {
    agent: u64,
    script: String,
    data_dir: String,
}

async fn start(
    State(app): State<App>,
    actor: Actor<Lab>,
    Json(body): Json<StartBody>,
) -> axum::response::Response {
    if actor.principal().role != k::ROLE_HUMAN {
        return reply(StatusCode::FORBIDDEN, error_body("forbidden"));
    }
    let snap = match app.engine.snapshot(TenantId(actor.principal().org)).await {
        Ok(snap) => snap,
        Err(error) => {
            return reply(
                StatusCode::SERVICE_UNAVAILABLE,
                error_body(&error.to_string()),
            );
        }
    };
    let Some(agent) = k::find_agent(&snap.agents, body.agent) else {
        return reply(StatusCode::NOT_FOUND, error_body("not_found"));
    };
    let mode = if agent.sandbox == 0 {
        SandboxMode::Advisory
    } else {
        SandboxMode::Box
    };
    let token = h5i_app_http::HmacAuth::<Lab>::new(app.secret.clone(), principal_from_token).issue(
        actor.principal().org,
        token_user(k::ROLE_AGENT, body.agent),
        86_400,
    );
    match start_agent(
        &app.runner,
        body.agent,
        mode,
        &body.script,
        &body.data_dir,
        Some(&token),
    ) {
        Ok(()) => reply(
            StatusCode::OK,
            Out::obj([("trust", Out::str(mode.label()))]),
        ),
        Err(error) => reply(StatusCode::UNPROCESSABLE_ENTITY, error_body(&error)),
    }
}

#[derive(Deserialize)]
struct StopBody {
    agent: u64,
}

async fn stop(
    State(app): State<App>,
    actor: Actor<Lab>,
    Json(body): Json<StopBody>,
) -> axum::response::Response {
    if actor.principal().role != k::ROLE_HUMAN {
        return reply(StatusCode::FORBIDDEN, error_body("forbidden"));
    }
    match stop_agent(&app.runner, body.agent) {
        Ok(()) => reply(StatusCode::OK, Out::obj([("ok", Out::str("true"))])),
        Err(error) => reply(StatusCode::NOT_FOUND, error_body(&error)),
    }
}

#[derive(Deserialize)]
struct AgentBody {
    agent: u64,
}

async fn pause(
    State(app): State<App>,
    actor: Actor<Lab>,
    Json(body): Json<AgentBody>,
) -> axum::response::Response {
    signal_route(&app, &actor, body.agent, true)
}

async fn resume(
    State(app): State<App>,
    actor: Actor<Lab>,
    Json(body): Json<AgentBody>,
) -> axum::response::Response {
    signal_route(&app, &actor, body.agent, false)
}

fn signal_route(
    app: &App,
    actor: &Actor<Lab>,
    agent: u64,
    pause: bool,
) -> axum::response::Response {
    if actor.principal().role != k::ROLE_HUMAN {
        return reply(StatusCode::FORBIDDEN, error_body("forbidden"));
    }
    let result = if pause {
        pause_agent(&app.runner, agent)
    } else {
        resume_agent(&app.runner, agent)
    };
    match result {
        Ok(()) => reply(StatusCode::OK, Out::obj([("ok", Out::str("true"))])),
        Err(error) => reply(StatusCode::NOT_FOUND, error_body(&error)),
    }
}

#[derive(Deserialize)]
struct CommentBody {
    family: u64,
    text: String,
    decision: Option<String>,
}

async fn comment(
    State(app): State<App>,
    actor: Actor<Lab>,
    Json(body): Json<CommentBody>,
) -> axum::response::Response {
    if actor.principal().role != k::ROLE_HUMAN {
        return reply(StatusCode::FORBIDDEN, error_body("forbidden"));
    }
    let text = body.text.into_bytes();
    let cmd = match body.decision.as_deref() {
        None | Some("note") => k::Command::Comment {
            family: body.family,
            text,
        },
        Some("reject") => k::Command::Reject {
            family: body.family,
            text,
        },
        Some(_) => return reply(StatusCode::UNPROCESSABLE_ENTITY, error_body("invalid")),
    };
    match app.engine.execute(actor.principal(), &cmd).await {
        Ok(Ok(_)) => reply(StatusCode::OK, Out::obj([("ok", Out::str("true"))])),
        Ok(Err(refusal)) => {
            let (status, body) = LabStore::encode_error(&refusal);
            reply(status, body)
        }
        Err(error) => {
            tracing::error!(%error, "review was not recorded");
            reply(StatusCode::SERVICE_UNAVAILABLE, error_body("unavailable"))
        }
    }
}

/// Holdout evaluations as h5i-db recorded them, with the summary the
/// worker computed on the sealed span. Not for agents.
async fn holdouts(State(app): State<App>, actor: Actor<Lab>) -> axum::response::Response {
    if actor.principal().role == k::ROLE_AGENT {
        return reply(StatusCode::FORBIDDEN, error_body("forbidden"));
    }
    let db = Arc::clone(&app.db);
    let state = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        rt.block_on(async move { db.export_state().await.map_err(|e| e.to_string()) })
    })
    .await;
    match state {
        Ok(Ok(state)) => {
            let body = serde_json::to_string(&state.evaluations).unwrap_or_else(|_| "[]".into());
            reply(StatusCode::OK, Out::obj([("holdouts", Out::str(&body))]))
        }
        Ok(Err(error)) => {
            tracing::error!(%error, "holdout records were not read");
            reply(StatusCode::SERVICE_UNAVAILABLE, error_body("unavailable"))
        }
        Err(error) => {
            tracing::error!(%error, "holdout read task failed");
            reply(StatusCode::SERVICE_UNAVAILABLE, error_body("unavailable"))
        }
    }
}

fn mark_name(mark: u64) -> &'static str {
    if mark == k::MARK_REJECT {
        "reject"
    } else {
        "note"
    }
}

async fn comments(State(app): State<App>, actor: Actor<Lab>) -> axum::response::Response {
    let principal = actor.principal();
    let snap = match app.engine.snapshot(TenantId(principal.org)).await {
        Ok(snap) => snap,
        Err(error) => {
            return reply(
                StatusCode::SERVICE_UNAVAILABLE,
                error_body(&error.to_string()),
            );
        }
    };
    let visible: Vec<&k::Comment> = if principal.role == k::ROLE_AGENT {
        let owned: Vec<u64> = snap
            .families
            .iter()
            .filter(|family| family.agent == principal.agent)
            .map(|family| family.id)
            .collect();
        snap.comments
            .iter()
            .filter(|comment| owned.contains(&comment.family))
            .collect()
    } else {
        snap.comments.iter().collect()
    };
    let body = Out::Arr(
        visible
            .iter()
            .map(|comment| {
                let text = String::from_utf8_lossy(&comment.text).into_owned();
                Out::obj([
                    ("id", comment.id.into()),
                    ("family", comment.family.into()),
                    ("decision", Out::str(mark_name(comment.mark))),
                    ("text", Out::str(&text)),
                ])
            })
            .collect(),
    );
    reply(StatusCode::OK, Out::obj([("comments", body)]))
}

/// Deliver outbox effects to h5i-db, then record the result as a kernel command.
pub struct EffectDeliver {
    pub app: App,
}

impl Deliver<()> for EffectDeliver {
    async fn deliver(&self, _endpoint: &(), delivery: &Delivery) -> Result<(), String> {
        let payload = String::from_utf8(delivery.payload.clone()).map_err(|e| e.to_string())?;
        let (kind, id) = payload.split_once(':').ok_or("effect payload")?;
        let kind: u64 = kind
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let id: u64 = id
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        let org = delivery.tenant.0;
        DbWorker {
            engine: Arc::clone(&self.app.engine),
            db: Arc::clone(&self.app.db),
            seal_id: self.app.seal_id.clone(),
            holdout_table: self.app.holdout_table.clone(),
        }
        .deliver(org, &delivery.key, kind, id)
        .await
    }
}
