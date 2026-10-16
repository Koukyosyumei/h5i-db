//! Research protocol kernel.
//!
//! One human runs many agents. The kernel is the ledger they cannot rewrite:
//! a trial count never falls, a pre-registration does not change after it is
//! written, a holdout is evaluated once, and only the human promotes a
//! strategy that passed. Agents write only a family they own. The worker, not
//! the agent, records a trial result, so a failed trial cannot be dropped by
//! skipping the call that would have reported it.
//!
//! Scores are non-negative integers. [`reported_score`] subtracts one for
//! every trial after the first, so a family with more than one positive trial
//! is never ranked on its raw best.

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
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Family in "families" {
        key { id: u64 }
        agent: u64,
        task: u64,
        hypothesis: u64,
        method: u64,
        trials: u64,
        best: u64,
        frozen_code: u64,
        frozen_params: u64,
        frozen_data: u64,
        holdout: u64,
        promoted: u64,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Pending in "pendings" {
        key { id: u64 }
        family: u64,
        agent: u64,
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
        tokens: u64,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Effect {
    pub dest: u64,
    pub kind: u64,
    pub id: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Write {
    SetMeta(Meta),
    PutAgent(Agent),
    PutFamily(Family),
    PutPending(Pending),
    PutTrial(Trial),
    Emit(Effect),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Enroll {
        agent: u64,
        budget: u64,
        token_budget: u64,
        sandbox: u64,
    },
    PreRegister {
        hypothesis: u64,
        method: u64,
    },
    RequestTrial {
        family: u64,
        code: u64,
        params: u64,
        decision_time: u64,
    },
    RecordTrial {
        pending: u64,
        data_version: u64,
        score: u64,
        tokens: u64,
    },
    Freeze {
        family: u64,
        code: u64,
        params: u64,
        data_version: u64,
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
    Report {
        family: u64,
    },
    Fleet,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    Ack,
    Created(u64),
    Metric {
        trials: u64,
        best: u64,
        reported: u64,
    },
    Fleet {
        agents: Vec<Agent>,
        families: Vec<Family>,
        trials: Vec<Trial>,
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

fn four(a: Write, b: Write, c: Write, d: Write) -> Vec<Write> {
    let mut ws = Vec::new();
    ws.push(a);
    ws.push(b);
    ws.push(c);
    ws.push(d);
    ws
}

/// Rank a family on every trial, not on the raw best.
///
/// With more than one trial and a positive best, the result is strictly
/// smaller than `best`: one point per extra trial, and zero once that
/// penalty meets the best.
pub fn reported_score(best: u64, n: u64) -> u64 {
    if n <= 1 || best == 0 {
        best
    } else {
        let pen = n - 1;
        if pen >= best { 0 } else { best - pen }
    }
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

fn enroll(
    actor: &Principal,
    s: &Snapshot,
    agent: u64,
    budget: u64,
    token_budget: u64,
    sandbox: u64,
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
    let row = Agent {
        id: agent,
        budget,
        used: 0,
        token_budget,
        tokens_used: 0,
        fork_id: agent,
        sandbox,
    };
    Ok((one(Write::PutAgent(row)), Reply::Created(agent)))
}

fn pre_register(actor: &Principal, s: &Snapshot, hypothesis: u64, method: u64) -> Outcome {
    if actor.role != ROLE_AGENT {
        return Err(Error::Forbidden);
    }
    if actor.agent == 0 || hypothesis == 0 || method == 0 {
        return Err(Error::Invalid);
    }
    match find_agent(&s.agents, actor.agent) {
        None => return Err(Error::NotFound),
        Some(_) => {}
    }
    let m = s.meta;
    if m.next_id == u64::MAX {
        return Err(Error::Overflow);
    }
    let id = m.next_id;
    let family = Family {
        id,
        agent: actor.agent,
        task: id,
        hypothesis,
        method,
        trials: 0,
        best: 0,
        frozen_code: 0,
        frozen_params: 0,
        frozen_data: 0,
        holdout: HOLDOUT_NONE,
        promoted: 0,
    };
    let meta = Meta { next_id: m.next_id + 1 };
    Ok((two(Write::PutFamily(family), Write::SetMeta(meta)), Reply::Created(id)))
}

fn request_trial(
    actor: &Principal,
    s: &Snapshot,
    family: u64,
    code: u64,
    params: u64,
    decision_time: u64,
) -> Outcome {
    if actor.role != ROLE_AGENT {
        return Err(Error::Forbidden);
    }
    if code == 0 {
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
                        } else if s.meta.next_id == u64::MAX {
                            Err(Error::Overflow)
                        } else {
                            let id = s.meta.next_id;
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
                            };
                            let meta = Meta { next_id: id + 1 };
                            let effect = Effect { dest: DEST_DB, kind: EFFECT_TRIAL, id };
                            Ok((
                                four(
                                    Write::PutPending(pending),
                                    Write::PutAgent(agent),
                                    Write::SetMeta(meta),
                                    Write::Emit(effect),
                                ),
                                Reply::Created(id),
                            ))
                        }
                    }
                }
            }
        }
    }
}

fn record_trial(
    actor: &Principal,
    s: &Snapshot,
    pending: u64,
    data_version: u64,
    score: u64,
    tokens: u64,
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
                                    tokens,
                                };
                                let best = if score > f.best { score } else { f.best };
                                let family = Family {
                                    id: f.id,
                                    agent: f.agent,
                                    task: f.task,
                                    hypothesis: f.hypothesis,
                                    method: f.method,
                                    trials: f.trials + 1,
                                    best,
                                    frozen_code: f.frozen_code,
                                    frozen_params: f.frozen_params,
                                    frozen_data: f.frozen_data,
                                    holdout: f.holdout,
                                    promoted: f.promoted,
                                };
                                let agent = Agent {
                                    id: a.id,
                                    budget: a.budget,
                                    used: a.used,
                                    token_budget: a.token_budget,
                                    tokens_used: a.tokens_used + tokens,
                                    fork_id: a.fork_id,
                                    sandbox: a.sandbox,
                                };
                                Ok((
                                    four(
                                        Write::PutPending(closed),
                                        Write::PutTrial(trial),
                                        Write::PutFamily(family),
                                        Write::PutAgent(agent),
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

fn freeze(
    actor: &Principal,
    s: &Snapshot,
    family: u64,
    code: u64,
    params: u64,
    data_version: u64,
) -> Outcome {
    if actor.role != ROLE_AGENT {
        return Err(Error::Forbidden);
    }
    if code == 0 || params == 0 || data_version == 0 {
        return Err(Error::Invalid);
    }
    match find_family(&s.families, family) {
        None => Err(Error::NotFound),
        Some(f) => {
            if f.agent != actor.agent {
                Err(Error::Forbidden)
            } else if f.frozen_code != 0 {
                Err(Error::Conflict)
            } else {
                let row = Family {
                    id: f.id,
                    agent: f.agent,
                    task: f.task,
                    hypothesis: f.hypothesis,
                    method: f.method,
                    trials: f.trials,
                    best: f.best,
                    frozen_code: code,
                    frozen_params: params,
                    frozen_data: data_version,
                    holdout: f.holdout,
                    promoted: f.promoted,
                };
                Ok((one(Write::PutFamily(row)), Reply::Ack))
            }
        }
    }
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
                    best: f.best,
                    frozen_code: f.frozen_code,
                    frozen_params: f.frozen_params,
                    frozen_data: f.frozen_data,
                    holdout: HOLDOUT_QUEUED,
                    promoted: f.promoted,
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
                    best: f.best,
                    frozen_code: f.frozen_code,
                    frozen_params: f.frozen_params,
                    frozen_data: f.frozen_data,
                    holdout: passed,
                    promoted: f.promoted,
                };
                Ok((one(Write::PutFamily(row)), Reply::Ack))
            }
        }
    }
}

fn promote(actor: &Principal, s: &Snapshot, family: u64) -> Outcome {
    if actor.role != ROLE_HUMAN {
        return Err(Error::Forbidden);
    }
    match find_family(&s.families, family) {
        None => Err(Error::NotFound),
        Some(f) => {
            if f.holdout != HOLDOUT_PASS {
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
                    best: f.best,
                    frozen_code: f.frozen_code,
                    frozen_params: f.frozen_params,
                    frozen_data: f.frozen_data,
                    holdout: f.holdout,
                    promoted: 1,
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
                        best: f.best,
                        reported: reported_score(f.best, f.trials),
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
        },
    ))
}

/// One step of the ledger. A refusal commits nothing.
pub fn transition(actor: &Principal, s: &Snapshot, cmd: &Command) -> Outcome {
    match cmd {
        Command::Enroll { agent, budget, token_budget, sandbox } => {
            enroll(actor, s, *agent, *budget, *token_budget, *sandbox)
        }
        Command::PreRegister { hypothesis, method } => pre_register(actor, s, *hypothesis, *method),
        Command::RequestTrial { family, code, params, decision_time } => {
            request_trial(actor, s, *family, *code, *params, *decision_time)
        }
        Command::RecordTrial { pending, data_version, score, tokens } => {
            record_trial(actor, s, *pending, *data_version, *score, *tokens)
        }
        Command::Freeze { family, code, params, data_version } => {
            freeze(actor, s, *family, *code, *params, *data_version)
        }
        Command::SubmitHoldout { family } => submit_holdout(actor, s, *family),
        Command::EvaluateHoldout { family, passed } => evaluate_holdout(actor, s, *family, *passed),
        Command::Promote { family } => promote(actor, s, *family),
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

    #[test]
    fn an_agent_cannot_drop_or_rewrite_a_trial() {
        let s = Snapshot::default();
        let (s, _) = run(&s, human(), Command::Enroll { agent: 7, budget: 2, token_budget: 10, sandbox: 0 });
        let (s, r) = run(&s, agent(7), Command::PreRegister { hypothesis: 11, method: 12 });
        let Reply::Created(family) = r.unwrap() else { panic!("family") };
        let (s, r) = run(
            &s,
            agent(7),
            Command::RequestTrial { family, code: 3, params: 4, decision_time: 5 },
        );
        let Reply::Created(pending) = r.unwrap() else { panic!("pending") };
        assert_eq!(
            run(&s, agent(7), Command::RecordTrial { pending, data_version: 1, score: 9, tokens: 1 }).1,
            Err(Error::Forbidden)
        );
        let (s, r) = run(&s, worker(), Command::RecordTrial { pending, data_version: 1, score: 4, tokens: 1 });
        assert_eq!(r, Ok(Reply::Ack));
        let (s2, r) = run(&s, worker(), Command::RecordTrial { pending, data_version: 1, score: 100, tokens: 1 });
        assert_eq!(r, Err(Error::Conflict));
        assert_eq!(s2.families[0].trials, s.families[0].trials);
        let (s, r) = run(
            &s,
            agent(7),
            Command::RequestTrial { family, code: 8, params: 4, decision_time: 6 },
        );
        let Reply::Created(pending2) = r.unwrap() else { panic!("pending2") };
        let (s, _) = run(&s, worker(), Command::RecordTrial { pending: pending2, data_version: 1, score: 9, tokens: 1 });
        assert_eq!(s.families[0].trials, 2);
        assert_eq!(s.families[0].best, 9);
        assert_eq!(s.trials.len(), 2);
        match run(&s, agent(7), Command::Report { family }).1 {
            Ok(Reply::Metric { trials, best, reported }) => {
                assert_eq!(trials, 2);
                assert_eq!(best, 9);
                assert!(reported < best);
                assert_eq!(reported, reported_score(best, trials));
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            run(&s, agent(7), Command::RequestTrial { family, code: 1, params: 1, decision_time: 1 }).1,
            Err(Error::Budget)
        );
    }

    #[test]
    fn holdout_is_once_and_only_a_human_promotes_a_pass() {
        let s = Snapshot::default();
        let (s, _) = run(&s, human(), Command::Enroll { agent: 3, budget: 5, token_budget: 5, sandbox: 1 });
        let (s, r) = run(&s, agent(3), Command::PreRegister { hypothesis: 1, method: 2 });
        let Reply::Created(family) = r.unwrap() else { panic!("family") };
        // A second pre-registration creates another family; the first row does not change.
        let (s, _) = run(&s, agent(3), Command::PreRegister { hypothesis: 9, method: 8 });
        assert_eq!(s.families[0].hypothesis, 1);
        assert_eq!(s.families[0].method, 2);
        assert_eq!(run(&s, agent(3), Command::SubmitHoldout { family }).1, Err(Error::Conflict));
        let (s, _) = run(&s, agent(3), Command::Freeze { family, code: 4, params: 5, data_version: 6 });
        assert_eq!(
            run(&s, agent(3), Command::Freeze { family, code: 7, params: 5, data_version: 6 }).1,
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
        let (s, _) = run(&s, human(), Command::Enroll { agent: 1, budget: 1, token_budget: 1, sandbox: 0 });
        let (s, _) = run(&s, human(), Command::Enroll { agent: 2, budget: 1, token_budget: 1, sandbox: 1 });
        let (s, r) = run(&s, agent(1), Command::PreRegister { hypothesis: 4, method: 5 });
        let Reply::Created(family) = r.unwrap() else { panic!("family") };
        assert_eq!(run(&s, agent(2), Command::Report { family }).1, Err(Error::Forbidden));
        assert_eq!(run(&s, agent(2), Command::RequestTrial { family, code: 1, params: 1, decision_time: 1 }).1, Err(Error::Forbidden));
        let (s, _) = run(&s, agent(1), Command::Freeze { family, code: 8, params: 9, data_version: 10 });
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
    fn reported_score_is_below_the_raw_best_after_the_first_trial() {
        assert_eq!(reported_score(10, 1), 10);
        assert_eq!(reported_score(10, 2), 9);
        assert_eq!(reported_score(3, 10), 0);
        assert!(reported_score(10, 4) < 10);
    }
}
