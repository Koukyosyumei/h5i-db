//! Research protocol kernel.
//!
//! One human runs many agents. The kernel is the ledger they cannot rewrite:
//! a trial count never falls, a pre-registration does not change after it is
//! written, a holdout is evaluated once, and only the human promotes a
//! strategy that passed. A rejection is ledger state: once the human rejects
//! a family, promote conflicts, and that decision is still there after a
//! restart. Comments and alerts are ledger rows too. Agents write only a
//! family they own. The worker, not the agent, records a trial result, so a
//! failed trial cannot be dropped by skipping the call that would have
//! reported it.
//!
//! The family's reported score is `corrected`, an integer the worker stored.
//! The kernel does not compute it. A report returns that integer and the
//! trial count, not a raw trial score.

/// Human operator. The only role that enrolls agents and promotes.
pub const ROLE_HUMAN: u64 = 0;
/// An agent. It pre-registers, requests trials, and freezes. It does not record results.
pub const ROLE_AGENT: u64 = 1;
/// The worker that delivers effects to the database and writes the result back.
pub const ROLE_WORKER: u64 = 2;

pub const HOLDOUT_NONE: u64 = 0;
pub const HOLDOUT_PASS: u64 = 1;
pub const HOLDOUT_FAIL: u64 = 2;
/// Frozen strategy submitted for the one holdout evaluation.
pub const HOLDOUT_QUEUED: u64 = 3;

pub const DEST_DB: u64 = 1;
pub const EFFECT_TRIAL: u64 = 1;
pub const EFFECT_HOLDOUT: u64 = 2;
pub const EFFECT_PROMOTE: u64 = 3;

/// A review comment that does not reject the family.
pub const MARK_NOTE: u64 = 0;
/// The comment written by the rejection itself.
pub const MARK_REJECT: u64 = 1;

/// What a [`Text`] row holds. Family, pending, trial, and agent rows name
/// texts by id.
pub const TEXT_HYPOTHESIS: u64 = 1;
pub const TEXT_METHOD: u64 = 2;
pub const TEXT_CODE: u64 = 3;
pub const TEXT_PARAMS: u64 = 4;
pub const TEXT_AGENT: u64 = 5;

/// Who is calling. `agent` is the agent id when `role` is [`ROLE_AGENT`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Principal {
    pub org: u64,
    pub user: u64,
    pub role: u64,
    pub agent: u64,
}

h5i_app_schema::schema! {
    mapping lab_tables for lab_kernel, writes Write, lean "../proofs/generated/Schema.lean";

    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    pub struct Snapshot {
        meta: Meta,
        agents: Vec<Agent>,
        families: Vec<Family>,
        pendings: Vec<Pending>,
        trials: Vec<Trial>,
        comments: Vec<Comment>,
        alerts: Vec<Alert>,
        texts: Vec<Text>,
        curves: Vec<Curve>,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
    pub struct Meta in "meta" {
        key {}
        next_id: u64,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Agent in "agents" {
        key { id: u64 }
        budget: u64,
        used: u64,
        token_budget: u64,
        tokens_used: u64,
        fork_id: u64,
        sandbox: u64,
        /// Text id of the agent's name ([`TEXT_AGENT`]), or 0 for none.
        label: u64,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Family in "families" {
        key { id: u64 }
        agent: u64,
        task: u64,
        /// Text ids ([`TEXT_HYPOTHESIS`], [`TEXT_METHOD`]).
        hypothesis: u64,
        method: u64,
        trials: u64,
        /// Worker-supplied reported score. The kernel does not derive it.
        corrected: u64,
        frozen_code: u64,
        frozen_params: u64,
        frozen_data: u64,
        holdout: u64,
        promoted: u64,
        rejected: u64,
    }

    /// A review note. `mark` is [`MARK_NOTE`] or [`MARK_REJECT`].
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct Comment in "comments" {
        key { id: u64 }
        family: u64,
        mark: u64,
        text: Vec<u8>,
    }

    /// A protocol alert. The worker writes these; they are not process memory.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct Alert in "alerts" {
        key { id: u64 }
        kind: Vec<u8>,
        detail: Vec<u8>,
    }

    /// Text an id stands for: a hypothesis, a method, strategy code (with a
    /// short name), parameters, or an agent's name. One row per kind and
    /// body. No command rewrites a row: a second submission of the same body
    /// gets the first row's id.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct Text in "texts" {
        key { id: u64 }
        kind: u64,
        name: Vec<u8>,
        body: Vec<u8>,
    }

    /// What the worker measured for one trial (its key is the trial id), in
    /// the worker's encoding. Written with the trial and never changed. The
    /// kernel does not interpret it.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct Curve in "curves" {
        key { id: u64 }
        body: Vec<u8>,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Pending in "pendings" {
        key { id: u64 }
        family: u64,
        agent: u64,
        /// Text ids ([`TEXT_CODE`], [`TEXT_PARAMS`]).
        code: u64,
        params: u64,
        decision_time: u64,
        open: u64,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Trial in "trials" {
        key { id: u64 }
        family: u64,
        agent: u64,
        code: u64,
        params: u64,
        data_version: u64,
        decision_time: u64,
        score: u64,
        /// Return statistics for this trial, in the worker's encoding.
        /// The kernel stores them and does not interpret them.
        sharpe: u64,
        skew: u64,
        kurt: u64,
        samples: u64,
        tokens: u64,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Effect {
    pub dest: u64,
    pub kind: u64,
    pub id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Write {
    SetMeta(Meta),
    PutAgent(Agent),
    PutFamily(Family),
    PutPending(Pending),
    PutTrial(Trial),
    PutComment(Comment),
    PutAlert(Alert),
    PutText(Text),
    PutCurve(Curve),
    Emit(Effect),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Enroll {
        agent: u64,
        budget: u64,
        token_budget: u64,
        sandbox: u64,
        /// The agent's name. Empty for none.
        name: Vec<u8>,
    },
    /// The hypothesis and the method name, as text.
    PreRegister {
        hypothesis: Vec<u8>,
        method: Vec<u8>,
    },
    /// Strategy code with a short name, and its parameters, as text.
    RequestTrial {
        family: u64,
        name: Vec<u8>,
        code: Vec<u8>,
        params: Vec<u8>,
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
        /// The worker's measurements of this trial, stored as a [`Curve`].
        curve: Vec<u8>,
    },
    /// Freeze the code, parameters, and data version of a counted trial of
    /// this family.
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
    /// Human only. Sets [`Family::rejected`] and stores the reason.
    Reject {
        family: u64,
        text: Vec<u8>,
    },
    /// Human only. A note that does not by itself block promotion.
    Comment {
        family: u64,
        text: Vec<u8>,
    },
    /// Worker or human. An agent cannot file an alert.
    NoteAlert {
        kind: Vec<u8>,
        detail: Vec<u8>,
    },
    Report {
        family: u64,
    },
    Fleet,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    Ack,
    Created(u64),
    /// Leaderboard and review read these two numbers and no raw score.
    Metric {
        trials: u64,
        corrected: u64,
    },
    Fleet {
        agents: Vec<Agent>,
        families: Vec<Family>,
        trials: Vec<Trial>,
        texts: Vec<Text>,
        curves: Vec<Curve>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Forbidden,
    NotFound,
    Conflict,
    Budget,
    Overflow,
    Invalid,
}

type Outcome = Result<(Vec<Write>, Reply), Error>;

fn one(w: Write) -> Vec<Write> {
    let mut ws = Vec::new();
    ws.push(w);
    ws
}

fn two(a: Write, b: Write) -> Vec<Write> {
    let mut ws = Vec::new();
    ws.push(a);
    ws.push(b);
    ws
}

fn five(a: Write, b: Write, c: Write, d: Write, e: Write) -> Vec<Write> {
    let mut ws = Vec::new();
    ws.push(a);
    ws.push(b);
    ws.push(c);
    ws.push(d);
    ws.push(e);
    ws
}

pub fn find_agent(v: &Vec<Agent>, id: u64) -> Option<Agent> {
    let mut i = 0;
    while i < v.len() {
        if v[i].id == id {
            return Some(v[i]);
        }
        i += 1;
    }
    None
}

pub fn find_family(v: &Vec<Family>, id: u64) -> Option<Family> {
    let mut i = 0;
    while i < v.len() {
        if v[i].id == id {
            return Some(v[i]);
        }
        i += 1;
    }
    None
}

pub fn find_pending(v: &Vec<Pending>, id: u64) -> Option<Pending> {
    let mut i = 0;
    while i < v.len() {
        if v[i].id == id {
            return Some(v[i]);
        }
        i += 1;
    }
    None
}

pub fn find_trial(v: &Vec<Trial>, id: u64) -> Option<Trial> {
    let mut i = 0;
    while i < v.len() {
        if v[i].id == id {
            return Some(v[i]);
        }
        i += 1;
    }
    None
}

/// Id of the stored text with this kind and body, if there is one.
pub fn find_text(v: &Vec<Text>, kind: u64, body: &Vec<u8>) -> Option<u64> {
    let mut i = 0;
    while i < v.len() {
        if v[i].kind == kind && v[i].body == *body {
            return Some(v[i].id);
        }
        i += 1;
    }
    None
}

/// The id of a text with this kind and body, and the next free id.
///
/// An existing row is reused and nothing is written. Otherwise the text gets
/// id `next` and the row to write comes back with it.
pub fn intern(
    v: &Vec<Text>,
    kind: u64,
    name: &Vec<u8>,
    body: &Vec<u8>,
    next: u64,
) -> Result<(u64, Option<Text>, u64), Error> {
    match find_text(v, kind, body) {
        Some(id) => Ok((id, None, next)),
        None => {
            if next == u64::MAX {
                Err(Error::Overflow)
            } else {
                let row = Text {
                    id: next,
                    kind,
                    name: name.clone(),
                    body: body.clone(),
                };
                Ok((next, Some(row), next + 1))
            }
        }
    }
}

fn push_text(ws: &mut Vec<Write>, row: Option<Text>) {
    match row {
        Some(t) => ws.push(Write::PutText(t)),
        None => {}
    }
}

fn enroll(
    actor: &Principal,
    s: &Snapshot,
    agent: u64,
    budget: u64,
    token_budget: u64,
    sandbox: u64,
    name: &Vec<u8>,
) -> Outcome {
    if actor.role != ROLE_HUMAN {
        return Err(Error::Forbidden);
    }
    if agent == 0 || (sandbox != 0 && sandbox != 1) {
        return Err(Error::Invalid);
    }
    match find_agent(&s.agents, agent) {
        Some(_) => return Err(Error::Conflict),
        None => {}
    }
    if name.len() == 0 {
        let row = Agent {
            id: agent,
            budget,
            used: 0,
            token_budget,
            tokens_used: 0,
            fork_id: agent,
            sandbox,
            label: 0,
        };
        return Ok((one(Write::PutAgent(row)), Reply::Created(agent)));
    }
    // Label 0 means no name, so a name never gets id 0.
    let first = if s.meta.next_id == 0 { 1 } else { s.meta.next_id };
    match intern(&s.texts, TEXT_AGENT, &Vec::new(), name, first) {
        Err(e) => Err(e),
        Ok((label, text, next)) => {
            let row = Agent {
                id: agent,
                budget,
                used: 0,
                token_budget,
                tokens_used: 0,
                fork_id: agent,
                sandbox,
                label,
            };
            let mut ws = Vec::new();
            push_text(&mut ws, text);
            ws.push(Write::PutAgent(row));
            ws.push(Write::SetMeta(Meta { next_id: next }));
            Ok((ws, Reply::Created(agent)))
        }
    }
}

fn pre_register(
    actor: &Principal,
    s: &Snapshot,
    hypothesis: &Vec<u8>,
    method: &Vec<u8>,
) -> Outcome {
    if actor.role != ROLE_AGENT {
        return Err(Error::Forbidden);
    }
    if actor.agent == 0 || hypothesis.len() == 0 || method.len() == 0 {
        return Err(Error::Invalid);
    }
    match find_agent(&s.agents, actor.agent) {
        None => return Err(Error::NotFound),
        Some(_) => {}
    }
    let (hypothesis, h_text, next) =
        match intern(&s.texts, TEXT_HYPOTHESIS, &Vec::new(), hypothesis, s.meta.next_id) {
            Err(e) => return Err(e),
            Ok(found) => found,
        };
    let (method, m_text, next) = match intern(&s.texts, TEXT_METHOD, &Vec::new(), method, next) {
        Err(e) => return Err(e),
        Ok(found) => found,
    };
    if next == u64::MAX {
        return Err(Error::Overflow);
    }
    let id = next;
    let family = Family {
        id,
        agent: actor.agent,
        task: id,
        hypothesis,
        method,
        trials: 0,
        corrected: 0,
        frozen_code: 0,
        frozen_params: 0,
        frozen_data: 0,
        holdout: HOLDOUT_NONE,
        promoted: 0,
        rejected: 0,
    };
    let meta = Meta { next_id: id + 1 };
    let mut ws = Vec::new();
    push_text(&mut ws, h_text);
    push_text(&mut ws, m_text);
    ws.push(Write::PutFamily(family));
    ws.push(Write::SetMeta(meta));
    Ok((ws, Reply::Created(id)))
}

fn request_trial(
    actor: &Principal,
    s: &Snapshot,
    family: u64,
    name: &Vec<u8>,
    code: &Vec<u8>,
    params: &Vec<u8>,
    decision_time: u64,
) -> Outcome {
    if actor.role != ROLE_AGENT {
        return Err(Error::Forbidden);
    }
    if code.len() == 0 {
        return Err(Error::Invalid);
    }
    match find_family(&s.families, family) {
        None => Err(Error::NotFound),
        Some(f) => {
            if f.agent != actor.agent {
                Err(Error::Forbidden)
            } else {
                match find_agent(&s.agents, actor.agent) {
                    None => Err(Error::NotFound),
                    Some(a) => {
                        if a.used >= a.budget {
                            Err(Error::Budget)
                        } else {
                            request_new(actor, s, family, a, name, code, params, decision_time)
                        }
                    }
                }
            }
        }
    }
}

/// The pending row for a request that passed the role, owner, and budget checks.
fn request_new(
    actor: &Principal,
    s: &Snapshot,
    family: u64,
    a: Agent,
    name: &Vec<u8>,
    code: &Vec<u8>,
    params: &Vec<u8>,
    decision_time: u64,
) -> Outcome {
    let (code, c_text, next) = match intern(&s.texts, TEXT_CODE, name, code, s.meta.next_id) {
        Err(e) => return Err(e),
        Ok(found) => found,
    };
    let (params, p_text, next) = match intern(&s.texts, TEXT_PARAMS, &Vec::new(), params, next) {
        Err(e) => return Err(e),
        Ok(found) => found,
    };
    // Id 0 is never a code: a frozen family is one whose frozen code is not 0.
    if code == 0 {
        return Err(Error::Invalid);
    }
    if next == u64::MAX {
        return Err(Error::Overflow);
    }
    let id = next;
    let pending = Pending {
        id,
        family,
        agent: actor.agent,
        code,
        params,
        decision_time,
        open: 1,
    };
    let agent = Agent {
        id: a.id,
        budget: a.budget,
        used: a.used + 1,
        token_budget: a.token_budget,
        tokens_used: a.tokens_used,
        fork_id: a.fork_id,
        sandbox: a.sandbox,
        label: a.label,
    };
    let meta = Meta { next_id: id + 1 };
    let effect = Effect { dest: DEST_DB, kind: EFFECT_TRIAL, id };
    let mut ws = Vec::new();
    push_text(&mut ws, c_text);
    push_text(&mut ws, p_text);
    ws.push(Write::PutPending(pending));
    ws.push(Write::PutAgent(agent));
    ws.push(Write::SetMeta(meta));
    ws.push(Write::Emit(effect));
    Ok((ws, Reply::Created(id)))
}

fn record_trial(
    actor: &Principal,
    s: &Snapshot,
    pending: u64,
    data_version: u64,
    score: u64,
    corrected: u64,
    sharpe: u64,
    skew: u64,
    kurt: u64,
    samples: u64,
    tokens: u64,
    curve: &Vec<u8>,
) -> Outcome {
    if actor.role != ROLE_WORKER {
        return Err(Error::Forbidden);
    }
    match find_pending(&s.pendings, pending) {
        None => Err(Error::NotFound),
        Some(p) => {
            if p.open != 1 {
                Err(Error::Conflict)
            } else {
                match find_family(&s.families, p.family) {
                    None => Err(Error::NotFound),
                    Some(f) => match find_agent(&s.agents, p.agent) {
                        None => Err(Error::NotFound),
                        Some(a) => {
                            if a.tokens_used > u64::MAX - tokens {
                                Err(Error::Overflow)
                            } else if a.tokens_used + tokens > a.token_budget {
                                Err(Error::Budget)
                            } else if f.trials == u64::MAX {
                                Err(Error::Overflow)
                            } else {
                                let closed = Pending {
                                    id: p.id,
                                    family: p.family,
                                    agent: p.agent,
                                    code: p.code,
                                    params: p.params,
                                    decision_time: p.decision_time,
                                    open: 0,
                                };
                                let trial = Trial {
                                    id: p.id,
                                    family: p.family,
                                    agent: p.agent,
                                    code: p.code,
                                    params: p.params,
                                    data_version,
                                    decision_time: p.decision_time,
                                    score,
                                    sharpe,
                                    skew,
                                    kurt,
                                    samples,
                                    tokens,
                                };
                                let family = Family {
                                    id: f.id,
                                    agent: f.agent,
                                    task: f.task,
                                    hypothesis: f.hypothesis,
                                    method: f.method,
                                    trials: f.trials + 1,
                                    corrected,
                                    frozen_code: f.frozen_code,
                                    frozen_params: f.frozen_params,
                                    frozen_data: f.frozen_data,
                                    holdout: f.holdout,
                                    promoted: f.promoted,
                                    rejected: f.rejected,
                                };
                                let agent = Agent {
                                    id: a.id,
                                    budget: a.budget,
                                    used: a.used,
                                    token_budget: a.token_budget,
                                    tokens_used: a.tokens_used + tokens,
                                    fork_id: a.fork_id,
                                    sandbox: a.sandbox,
                                    label: a.label,
                                };
                                let measured = Curve { id: p.id, body: curve.clone() };
                                Ok((
                                    five(
                                        Write::PutPending(closed),
                                        Write::PutTrial(trial),
                                        Write::PutFamily(family),
                                        Write::PutAgent(agent),
                                        Write::PutCurve(measured),
                                    ),
                                    Reply::Ack,
                                ))
                            }
                        }
                    },
                }
            }
        }
    }
}

/// Agent only, on its own family, once. What is frozen is a trial the family
/// already counted: its code, parameters, and data version.
fn freeze(actor: &Principal, s: &Snapshot, family: u64, trial: u64) -> Outcome {
    if actor.role != ROLE_AGENT {
        return Err(Error::Forbidden);
    }
    match find_family(&s.families, family) {
        None => Err(Error::NotFound),
        Some(f) => {
            if f.agent != actor.agent {
                Err(Error::Forbidden)
            } else if f.frozen_code != 0 {
                Err(Error::Conflict)
            } else {
                match find_trial(&s.trials, trial) {
                    None => Err(Error::NotFound),
                    Some(t) => freeze_trial(f, t),
                }
            }
        }
    }
}

fn freeze_trial(f: Family, t: Trial) -> Outcome {
    if t.family != f.id {
        return Err(Error::Invalid);
    }
    if t.code == 0 || t.params == 0 || t.data_version == 0 {
        return Err(Error::Invalid);
    }
    let code = t.code;
    let params = t.params;
    let data_version = t.data_version;
    let row = Family {
        id: f.id,
        agent: f.agent,
        task: f.task,
        hypothesis: f.hypothesis,
        method: f.method,
        trials: f.trials,
        corrected: f.corrected,
        frozen_code: code,
        frozen_params: params,
        frozen_data: data_version,
        holdout: f.holdout,
        promoted: f.promoted,
        rejected: f.rejected,
    };
    Ok((one(Write::PutFamily(row)), Reply::Ack))
}

fn submit_holdout(actor: &Principal, s: &Snapshot, family: u64) -> Outcome {
    if actor.role != ROLE_AGENT {
        return Err(Error::Forbidden);
    }
    match find_family(&s.families, family) {
        None => Err(Error::NotFound),
        Some(f) => {
            if f.agent != actor.agent {
                Err(Error::Forbidden)
            } else if f.frozen_code == 0 || f.holdout != HOLDOUT_NONE {
                Err(Error::Conflict)
            } else {
                let row = Family {
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
                    holdout: HOLDOUT_QUEUED,
                    promoted: f.promoted,
                    rejected: f.rejected,
                };
                let effect = Effect { dest: DEST_DB, kind: EFFECT_HOLDOUT, id: family };
                Ok((two(Write::PutFamily(row), Write::Emit(effect)), Reply::Ack))
            }
        }
    }
}

fn evaluate_holdout(actor: &Principal, s: &Snapshot, family: u64, passed: u64) -> Outcome {
    if actor.role != ROLE_WORKER {
        return Err(Error::Forbidden);
    }
    if passed != HOLDOUT_PASS && passed != HOLDOUT_FAIL {
        return Err(Error::Invalid);
    }
    match find_family(&s.families, family) {
        None => Err(Error::NotFound),
        Some(f) => {
            if f.holdout != HOLDOUT_QUEUED {
                Err(Error::Conflict)
            } else {
                let row = Family {
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
                    holdout: passed,
                    promoted: f.promoted,
                    rejected: f.rejected,
                };
                Ok((one(Write::PutFamily(row)), Reply::Ack))
            }
        }
    }
}

fn three(a: Write, b: Write, c: Write) -> Vec<Write> {
    let mut ws = Vec::new();
    ws.push(a);
    ws.push(b);
    ws.push(c);
    ws
}

fn alert_exists(v: &Vec<Alert>, kind: &Vec<u8>, detail: &Vec<u8>) -> bool {
    let mut i = 0;
    while i < v.len() {
        if v[i].kind == *kind && v[i].detail == *detail {
            return true;
        }
        i += 1;
    }
    false
}

/// Human only. A family that is already rejected stays rejected.
fn reject(actor: &Principal, s: &Snapshot, family: u64, text: &Vec<u8>) -> Outcome {
    if actor.role != ROLE_HUMAN {
        return Err(Error::Forbidden);
    }
    if text.len() == 0 {
        return Err(Error::Invalid);
    }
    match find_family(&s.families, family) {
        None => Err(Error::NotFound),
        Some(f) => {
            if f.rejected == 1 {
                Ok((Vec::new(), Reply::Ack))
            } else if s.meta.next_id == u64::MAX {
                Err(Error::Overflow)
            } else {
                let row = Family {
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
                    rejected: 1,
                };
                let id = s.meta.next_id;
                let note = Comment {
                    id,
                    family,
                    mark: MARK_REJECT,
                    text: text.clone(),
                };
                let meta = Meta { next_id: id + 1 };
                Ok((
                    three(Write::PutFamily(row), Write::PutComment(note), Write::SetMeta(meta)),
                    Reply::Ack,
                ))
            }
        }
    }
}

/// Human only. Does not change [`Family::rejected`].
fn comment(actor: &Principal, s: &Snapshot, family: u64, text: &Vec<u8>) -> Outcome {
    if actor.role != ROLE_HUMAN {
        return Err(Error::Forbidden);
    }
    if text.len() == 0 {
        return Err(Error::Invalid);
    }
    match find_family(&s.families, family) {
        None => Err(Error::NotFound),
        Some(_) => {
            if s.meta.next_id == u64::MAX {
                Err(Error::Overflow)
            } else {
                let id = s.meta.next_id;
                let row = Comment {
                    id,
                    family,
                    mark: MARK_NOTE,
                    text: text.clone(),
                };
                let meta = Meta { next_id: id + 1 };
                Ok((two(Write::PutComment(row), Write::SetMeta(meta)), Reply::Created(id)))
            }
        }
    }
}

/// Worker or human. The same kind and detail twice is one row.
fn note_alert(actor: &Principal, s: &Snapshot, kind: &Vec<u8>, detail: &Vec<u8>) -> Outcome {
    if actor.role != ROLE_WORKER && actor.role != ROLE_HUMAN {
        return Err(Error::Forbidden);
    }
    if kind.len() == 0 {
        return Err(Error::Invalid);
    }
    if alert_exists(&s.alerts, kind, detail) {
        return Ok((Vec::new(), Reply::Ack));
    }
    if s.meta.next_id == u64::MAX {
        return Err(Error::Overflow);
    }
    let id = s.meta.next_id;
    let row = Alert {
        id,
        kind: kind.clone(),
        detail: detail.clone(),
    };
    let meta = Meta { next_id: id + 1 };
    Ok((two(Write::PutAlert(row), Write::SetMeta(meta)), Reply::Created(id)))
}

fn promote(actor: &Principal, s: &Snapshot, family: u64) -> Outcome {
    if actor.role != ROLE_HUMAN {
        return Err(Error::Forbidden);
    }
    match find_family(&s.families, family) {
        None => Err(Error::NotFound),
        Some(f) => {
            if f.rejected != 0 {
                Err(Error::Conflict)
            } else if f.holdout != HOLDOUT_PASS {
                Err(Error::Conflict)
            } else if f.promoted != 0 {
                Err(Error::Conflict)
            } else {
                let row = Family {
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
                    promoted: 1,
                    rejected: f.rejected,
                };
                let effect = Effect { dest: DEST_DB, kind: EFFECT_PROMOTE, id: family };
                Ok((two(Write::PutFamily(row), Write::Emit(effect)), Reply::Ack))
            }
        }
    }
}

fn report(actor: &Principal, s: &Snapshot, family: u64) -> Outcome {
    match find_family(&s.families, family) {
        None => Err(Error::NotFound),
        Some(f) => {
            if actor.role == ROLE_AGENT && f.agent != actor.agent {
                Err(Error::Forbidden)
            } else {
                Ok((
                    Vec::new(),
                    Reply::Metric {
                        trials: f.trials,
                        corrected: f.corrected,
                    },
                ))
            }
        }
    }
}

fn fleet(actor: &Principal, s: &Snapshot) -> Outcome {
    if actor.role == ROLE_AGENT {
        return Err(Error::Forbidden);
    }
    Ok((
        Vec::new(),
        Reply::Fleet {
            agents: s.agents.clone(),
            families: s.families.clone(),
            trials: s.trials.clone(),
            texts: s.texts.clone(),
            curves: s.curves.clone(),
        },
    ))
}

/// One step of the ledger. A refusal commits nothing.
pub fn transition(actor: &Principal, s: &Snapshot, cmd: &Command) -> Outcome {
    match cmd {
        Command::Enroll { agent, budget, token_budget, sandbox, name } => {
            enroll(actor, s, *agent, *budget, *token_budget, *sandbox, name)
        }
        Command::PreRegister { hypothesis, method } => pre_register(actor, s, hypothesis, method),
        Command::RequestTrial { family, name, code, params, decision_time } => {
            request_trial(actor, s, *family, name, code, params, *decision_time)
        }
        Command::RecordTrial {
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
        } => record_trial(
            actor,
            s,
            *pending,
            *data_version,
            *score,
            *corrected,
            *sharpe,
            *skew,
            *kurt,
            *samples,
            *tokens,
            curve,
        ),
        Command::Freeze { family, trial } => freeze(actor, s, *family, *trial),
        Command::SubmitHoldout { family } => submit_holdout(actor, s, *family),
        Command::EvaluateHoldout { family, passed } => evaluate_holdout(actor, s, *family, *passed),
        Command::Promote { family } => promote(actor, s, *family),
        Command::Reject { family, text } => reject(actor, s, *family, text),
        Command::Comment { family, text } => comment(actor, s, *family, text),
        Command::NoteAlert { kind, detail } => note_alert(actor, s, kind, detail),
        Command::Report { family } => report(actor, s, *family),
        Command::Fleet => fleet(actor, s),
    }
}

fn apply_write(s: &mut Snapshot, w: Write) {
    match w {
        Write::SetMeta(m) => s.meta = m,
        Write::PutAgent(a) => Agent::put(&mut s.agents, a),
        Write::PutFamily(f) => Family::put(&mut s.families, f),
        Write::PutPending(p) => Pending::put(&mut s.pendings, p),
        Write::PutTrial(t) => Trial::put(&mut s.trials, t),
        Write::PutComment(c) => Comment::put(&mut s.comments, c),
        Write::PutAlert(a) => Alert::put(&mut s.alerts, a),
        Write::PutText(t) => Text::put(&mut s.texts, t),
        Write::PutCurve(c) => Curve::put(&mut s.curves, c),
        Write::Emit(_) => {}
    }
}

fn sql_write(w: &Write, out: &mut Vec<h5i_app_sql::Write>) {
    match w {
        Write::SetMeta(m) => out.push(m.sql_put()),
        Write::PutAgent(a) => out.push(a.sql_put()),
        Write::PutFamily(f) => out.push(f.sql_put()),
        Write::PutPending(p) => out.push(p.sql_put()),
        Write::PutTrial(t) => out.push(t.sql_put()),
        Write::PutComment(c) => out.push(c.sql_put()),
        Write::PutAlert(a) => out.push(a.sql_put()),
        Write::PutText(t) => out.push(t.sql_put()),
        Write::PutCurve(c) => out.push(c.sql_put()),
        Write::Emit(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn human() -> Principal {
        Principal { org: 1, user: 1, role: ROLE_HUMAN, agent: 0 }
    }
    fn agent(id: u64) -> Principal {
        Principal { org: 1, user: id, role: ROLE_AGENT, agent: id }
    }
    fn worker() -> Principal {
        Principal { org: 1, user: 9, role: ROLE_WORKER, agent: 0 }
    }

    fn run(s: &Snapshot, actor: Principal, c: Command) -> (Snapshot, Result<Reply, Error>) {
        match transition(&actor, s, &c) {
            Ok((ws, r)) => (apply(s, &ws), Ok(r)),
            Err(e) => (s.clone(), Err(e)),
        }
    }

    fn text(value: &str) -> Vec<u8> {
        value.as_bytes().to_vec()
    }

    /// Request a trial of `code` in `family` and have the worker record it.
    fn tried(s: &Snapshot, id: u64, family: u64, code: &str) -> (Snapshot, u64) {
        let (s, r) = run(s, agent(id), Command::RequestTrial {
            family, name: text("s"), code: text(code), params: text("{}"), decision_time: 1,
        });
        let Reply::Created(pending) = r.unwrap() else { panic!("pending") };
        let (s, r) = run(&s, worker(), Command::RecordTrial {
            pending, data_version: 1, score: 0, corrected: 0, sharpe: 0, skew: 0, kurt: 0, samples: 0, tokens: 0, curve: text("{}"),
        });
        assert_eq!(r, Ok(Reply::Ack));
        (s, pending)
    }

    #[test]
    fn an_agent_cannot_drop_or_rewrite_a_trial() {
        let s = Snapshot::default();
        let (s, _) = run(&s, human(), Command::Enroll { agent: 7, budget: 2, token_budget: 10, sandbox: 0, name: Vec::new() });
        let (s, r) = run(&s, agent(7), Command::PreRegister { hypothesis: text("h11"), method: text("m12") });
        let Reply::Created(family) = r.unwrap() else { panic!("family") };
        let (s, r) = run(
            &s,
            agent(7),
            Command::RequestTrial { family, name: text("s"), code: text("c3"), params: text("p4"), decision_time: 5 },
        );
        let Reply::Created(pending) = r.unwrap() else { panic!("pending") };
        assert_eq!(
            run(&s, agent(7), Command::RecordTrial { pending, data_version: 1, score: 9, corrected: 1, sharpe: 0, skew: 0, kurt: 0, samples: 0, tokens: 1, curve: Vec::new() }).1,
            Err(Error::Forbidden)
        );
        let (s, r) = run(&s, worker(), Command::RecordTrial { pending, data_version: 1, score: 4, corrected: 800_000, sharpe: 0, skew: 0, kurt: 0, samples: 0, tokens: 1, curve: Vec::new() });
        assert_eq!(r, Ok(Reply::Ack));
        let (s2, r) = run(&s, worker(), Command::RecordTrial { pending, data_version: 1, score: 100, corrected: 1, sharpe: 0, skew: 0, kurt: 0, samples: 0, tokens: 1, curve: Vec::new() });
        assert_eq!(r, Err(Error::Conflict));
        assert_eq!(s2.families[0].trials, s.families[0].trials);
        let (s, r) = run(
            &s,
            agent(7),
            Command::RequestTrial { family, name: text("s"), code: text("c8"), params: text("p4"), decision_time: 6 },
        );
        let Reply::Created(pending2) = r.unwrap() else { panic!("pending2") };
        let (s, _) = run(&s, worker(), Command::RecordTrial { pending: pending2, data_version: 1, score: 9, corrected: 100_000, sharpe: 0, skew: 0, kurt: 0, samples: 0, tokens: 1, curve: Vec::new() });
        assert_eq!(s.families[0].trials, 2);
        assert_eq!(s.families[0].corrected, 100_000);
        assert_eq!(s.trials[1].score, 9);
        assert_eq!(s.trials.len(), 2);
        match run(&s, agent(7), Command::Report { family }).1 {
            Ok(Reply::Metric { trials, corrected }) => {
                assert_eq!(trials, 2);
                assert_eq!(corrected, 100_000);
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            run(&s, agent(7), Command::RequestTrial { family, name: text("s"), code: text("c1"), params: text("p1"), decision_time: 1 }).1,
            Err(Error::Budget)
        );
    }

    #[test]
    fn holdout_is_once_and_only_a_human_promotes_a_pass() {
        let s = Snapshot::default();
        let (s, _) = run(&s, human(), Command::Enroll { agent: 3, budget: 5, token_budget: 5, sandbox: 1, name: Vec::new() });
        let (s, r) = run(&s, agent(3), Command::PreRegister { hypothesis: text("h1"), method: text("m2") });
        let Reply::Created(family) = r.unwrap() else { panic!("family") };
        // A second pre-registration creates another family; the first row does not change.
        let (s, _) = run(&s, agent(3), Command::PreRegister { hypothesis: text("h9"), method: text("m8") });
        let hypothesis = s.families[0].hypothesis;
        assert_eq!(s.texts.iter().find(|t| t.id == hypothesis).unwrap().body, text("h1"));
        assert_eq!(s.texts.len(), 4);
        assert_eq!(run(&s, agent(3), Command::SubmitHoldout { family }).1, Err(Error::Conflict));
        let (s, first) = tried(&s, 3, family, "a");
        let (s, second) = tried(&s, 3, family, "b");
        let (s, _) = run(&s, agent(3), Command::Freeze { family, trial: first });
        assert_eq!(s.families[0].frozen_code, s.trials[0].code);
        assert_eq!(
            run(&s, agent(3), Command::Freeze { family, trial: second }).1,
            Err(Error::Conflict)
        );
        let (s, _) = run(&s, agent(3), Command::SubmitHoldout { family });
        assert_eq!(run(&s, agent(3), Command::SubmitHoldout { family }).1, Err(Error::Conflict));
        assert_eq!(
            run(&s, agent(3), Command::EvaluateHoldout { family, passed: HOLDOUT_PASS }).1,
            Err(Error::Forbidden)
        );
        assert_eq!(run(&s, human(), Command::Promote { family }).1, Err(Error::Conflict));
        let (s, _) = run(&s, worker(), Command::EvaluateHoldout { family, passed: HOLDOUT_FAIL });
        assert_eq!(run(&s, worker(), Command::EvaluateHoldout { family, passed: HOLDOUT_PASS }).1, Err(Error::Conflict));
        assert_eq!(run(&s, human(), Command::Promote { family }).1, Err(Error::Conflict));
        assert_eq!(s.families[0].holdout, HOLDOUT_FAIL);
        assert_eq!(s.families[0].promoted, 0);
    }

    #[test]
    fn a_passing_holdout_promotes_once_and_agents_stay_on_their_fork() {
        let s = Snapshot::default();
        let (s, _) = run(&s, human(), Command::Enroll { agent: 1, budget: 1, token_budget: 1, sandbox: 0, name: Vec::new() });
        let (s, _) = run(&s, human(), Command::Enroll { agent: 2, budget: 1, token_budget: 1, sandbox: 1, name: Vec::new() });
        let (s, r) = run(&s, agent(1), Command::PreRegister { hypothesis: text("h4"), method: text("m5") });
        let Reply::Created(family) = r.unwrap() else { panic!("family") };
        assert_eq!(run(&s, agent(2), Command::Report { family }).1, Err(Error::Forbidden));
        assert_eq!(run(&s, agent(2), Command::RequestTrial { family, name: text("s"), code: text("c1"), params: text("p1"), decision_time: 1 }).1, Err(Error::Forbidden));
        let (s, trial) = tried(&s, 1, family, "a");
        let (s, _) = run(&s, agent(1), Command::Freeze { family, trial });
        let (s, _) = run(&s, agent(1), Command::SubmitHoldout { family });
        let (s, _) = run(&s, worker(), Command::EvaluateHoldout { family, passed: HOLDOUT_PASS });
        assert_eq!(run(&s, agent(1), Command::Promote { family }).1, Err(Error::Forbidden));
        let (s, r) = run(&s, human(), Command::Promote { family });
        assert_eq!(r, Ok(Reply::Ack));
        assert_eq!(s.families[0].promoted, 1);
        assert_eq!(run(&s, human(), Command::Promote { family }).1, Err(Error::Conflict));
        assert_eq!(s.agents[0].fork_id, 1);
        assert_eq!(s.agents[1].fork_id, 2);
    }

    #[test]
    fn a_rejected_strategy_cannot_be_promoted_and_only_the_human_reviews() {
        let s = Snapshot::default();
        let (s, _) = run(&s, human(), Command::Enroll { agent: 1, budget: 1, token_budget: 1, sandbox: 0, name: Vec::new() });
        let (s, r) = run(&s, agent(1), Command::PreRegister { hypothesis: text("h4"), method: text("m5") });
        let Reply::Created(family) = r.unwrap() else { panic!("family") };
        let (s, trial) = tried(&s, 1, family, "a");
        let (s, _) = run(&s, agent(1), Command::Freeze { family, trial });
        let (s, _) = run(&s, agent(1), Command::SubmitHoldout { family });
        let (s, _) = run(&s, worker(), Command::EvaluateHoldout { family, passed: HOLDOUT_PASS });
        let text = b"no".to_vec();
        assert_eq!(run(&s, agent(1), Command::Reject { family, text: text.clone() }).1, Err(Error::Forbidden));
        assert_eq!(run(&s, worker(), Command::Reject { family, text: text.clone() }).1, Err(Error::Forbidden));
        assert_eq!(run(&s, worker(), Command::Comment { family, text: text.clone() }).1, Err(Error::Forbidden));
        assert_eq!(run(&s, agent(1), Command::Promote { family }).1, Err(Error::Forbidden));
        assert_eq!(
            run(&s, agent(1), Command::NoteAlert { kind: b"x".to_vec(), detail: b"y".to_vec() }).1,
            Err(Error::Forbidden)
        );
        assert_eq!(run(&s, human(), Command::Comment { family, text: Vec::new() }).1, Err(Error::Invalid));
        let (s, r) = run(&s, human(), Command::Reject { family, text: text.clone() });
        assert_eq!(r, Ok(Reply::Ack));
        assert_eq!(s.families[0].rejected, 1);
        assert_eq!(s.families[0].promoted, 0);
        assert_eq!(s.comments.len(), 1);
        assert_eq!(s.comments[0].mark, MARK_REJECT);
        assert_eq!(s.comments[0].text, b"no");
        assert_eq!(run(&s, human(), Command::Promote { family }).1, Err(Error::Conflict));
        let (s, r) = run(&s, human(), Command::Reject { family, text: b"again".to_vec() });
        assert_eq!(r, Ok(Reply::Ack));
        assert_eq!(s.comments.len(), 1);
        assert_eq!(s.families[0].rejected, 1);
        let next = s.meta.next_id;
        let (s, r) = run(&s, human(), Command::Comment { family, text: b"see".to_vec() });
        assert_eq!(r, Ok(Reply::Created(next)));
        assert_eq!(s.comments[1].mark, MARK_NOTE);
        let (s, r) = run(&s, worker(), Command::NoteAlert { kind: b"budget".to_vec(), detail: b"over".to_vec() });
        assert_eq!(r, Ok(Reply::Created(next + 1)));
        assert_eq!(s.alerts.len(), 1);
        let (again, r) = run(&s, human(), Command::NoteAlert { kind: b"budget".to_vec(), detail: b"over".to_vec() });
        assert_eq!(r, Ok(Reply::Ack));
        assert_eq!(again.alerts.len(), 1);
        assert_eq!(run(&s, human(), Command::Promote { family }).1, Err(Error::Conflict));
    }

    #[test]
    fn the_report_returns_the_stored_correction_not_the_raw_score() {
        let s = Snapshot::default();
        let (s, _) = run(&s, human(), Command::Enroll { agent: 4, budget: 2, token_budget: 2, sandbox: 0, name: Vec::new() });
        let (s, r) = run(&s, agent(4), Command::PreRegister { hypothesis: text("h1"), method: text("m1") });
        let Reply::Created(family) = r.unwrap() else { panic!("family") };
        let (s, r) = run(&s, agent(4), Command::RequestTrial { family, name: text("s"), code: text("c2"), params: text("p2"), decision_time: 3 });
        let Reply::Created(pending) = r.unwrap() else { panic!("pending") };
        let (s, _) = run(&s, worker(), Command::RecordTrial {
            pending, data_version: 1, score: 50, corrected: 250_000, sharpe: 1, skew: 2, kurt: 3, samples: 4, tokens: 0, curve: Vec::new(),
        });
        assert_eq!(s.trials[0].score, 50);
        assert_eq!(s.trials[0].samples, 4);
        match run(&s, human(), Command::Report { family }).1 {
            Ok(Reply::Metric { trials, corrected }) => {
                assert_eq!(trials, 1);
                assert_eq!(corrected, 250_000);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn texts_are_stored_once_and_named_by_id() {
        let s = Snapshot::default();
        let (s, _) = run(&s, human(), Command::Enroll { agent: 2, budget: 3, token_budget: 0, sandbox: 0, name: text("trend") });
        let label = s.agents[0].label;
        assert_ne!(label, 0);
        assert_eq!(s.texts.iter().find(|t| t.id == label).unwrap().kind, TEXT_AGENT);
        let (s, _) = run(&s, human(), Command::Enroll { agent: 3, budget: 0, token_budget: 0, sandbox: 0, name: text("trend") });
        assert_eq!(s.agents[1].label, label);
        let (s, r) = run(&s, agent(2), Command::PreRegister { hypothesis: text(""), method: text("m") });
        assert_eq!(r, Err(Error::Invalid));
        let (s, r) = run(&s, agent(2), Command::PreRegister { hypothesis: text("momentum persists"), method: text("ma-cross") });
        let Reply::Created(family) = r.unwrap() else { panic!("family") };
        let request = |s: &Snapshot, code: &str, params: &str| {
            run(s, agent(2), Command::RequestTrial {
                family, name: text("cross"), code: text(code), params: text(params), decision_time: 9,
            })
        };
        let (s, _) = request(&s, "long when sma(5) > sma(20)", "{\"n\":1}");
        let (s, _) = request(&s, "long when sma(5) > sma(20)", "{\"n\":2}");
        assert_eq!(s.pendings[0].code, s.pendings[1].code);
        assert_ne!(s.pendings[0].params, s.pendings[1].params);
        let code = s.pendings[0].code;
        let row = s.texts.iter().find(|t| t.id == code).unwrap();
        assert_eq!(row.kind, TEXT_CODE);
        assert_eq!(row.name, text("cross"));
        assert_eq!(row.body, text("long when sma(5) > sma(20)"));
        assert_eq!(s.texts.iter().filter(|t| t.kind == TEXT_CODE).count(), 1);
        assert_eq!(request(&s, "", "{}").1, Err(Error::Invalid));
    }

    #[test]
    fn only_a_counted_trial_of_the_family_can_be_frozen() {
        let s = Snapshot::default();
        let (s, _) = run(&s, human(), Command::Enroll { agent: 5, budget: 4, token_budget: 0, sandbox: 0, name: Vec::new() });
        let (s, r) = run(&s, agent(5), Command::PreRegister { hypothesis: text("a"), method: text("b") });
        let Reply::Created(one) = r.unwrap() else { panic!("family") };
        let (s, r) = run(&s, agent(5), Command::PreRegister { hypothesis: text("c"), method: text("d") });
        let Reply::Created(other) = r.unwrap() else { panic!("family") };
        let (s, r) = run(&s, agent(5), Command::RequestTrial {
            family: one, name: text("s"), code: text("x"), params: text("{}"), decision_time: 1,
        });
        let Reply::Created(pending) = r.unwrap() else { panic!("pending") };
        assert_eq!(run(&s, agent(5), Command::Freeze { family: one, trial: pending }).1, Err(Error::NotFound));
        let (s, trial) = tried(&s, 5, one, "y");
        assert_eq!(run(&s, agent(5), Command::Freeze { family: other, trial }).1, Err(Error::Invalid));
        let (s, r) = run(&s, agent(5), Command::Freeze { family: one, trial });
        assert_eq!(r, Ok(Reply::Ack));
        let row = find_family(&s.families, one).unwrap();
        let counted = find_trial(&s.trials, trial).unwrap();
        assert_eq!((row.frozen_code, row.frozen_params, row.frozen_data), (counted.code, counted.params, counted.data_version));
        assert_eq!(s.curves.len(), 1);
        assert_eq!(s.curves[0].id, trial);
    }
}
