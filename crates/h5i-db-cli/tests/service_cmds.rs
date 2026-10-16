//! CLI coverage for the worker commands: keyed ingest fingerprints, seals,
//! one holdout evaluation, and ledger reconciliation.

use std::path::Path;
use std::process::{Command, Output};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_h5i-db")
}

fn run(args: &[&str], cwd: &Path) -> Output {
    Command::new(bin())
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("spawn h5i-db")
}

fn stdout_json(out: &Output) -> serde_json::Value {
    assert!(
        out.status.success(),
        "expected success, got {:?}\nstdout: {}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("stdout is JSON")
}

const CSV: &str = "ts,symbol,price,size\n\
2026-07-01T09:30:00Z,AAPL,210.5,100\n\
2026-07-01T09:30:01Z,MSFT,455.2,50\n\
2026-07-01T09:30:02Z,AAPL,210.7,200\n";

const OTHER: &str = "ts,symbol,price,size\n\
2026-07-01T09:31:00Z,AAPL,1.0,1\n";

fn bootstrap(cwd: &Path) {
    std::fs::write(cwd.join("trades.csv"), CSV).unwrap();
    std::fs::write(cwd.join("other.csv"), OTHER).unwrap();
    stdout_json(&run(&["init", "m.db", "--format", "json"], cwd));
    stdout_json(&run(
        &[
            "create-table",
            "m.db",
            "trades",
            "--like",
            "trades.csv",
            "--time-column",
            "ts",
            "--format",
            "json",
        ],
        cwd,
    ));
}

#[test]
fn the_same_ingest_key_rejects_a_different_file() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd);
    let first = stdout_json(&run(
        &[
            "ingest",
            "m.db",
            "trades",
            "trades.csv",
            "--mode",
            "write",
            "--idempotency-key",
            "load-1",
            "--format",
            "json",
        ],
        cwd,
    ));
    let replay = stdout_json(&run(
        &[
            "ingest",
            "m.db",
            "trades",
            "trades.csv",
            "--mode",
            "write",
            "--idempotency-key",
            "load-1",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert_eq!(replay["sequence"], first["sequence"]);
    assert_eq!(replay["segments_added"], 0);

    let conflict = run(
        &[
            "ingest",
            "m.db",
            "trades",
            "other.csv",
            "--mode",
            "write",
            "--idempotency-key",
            "load-1",
            "--format",
            "json",
        ],
        cwd,
    );
    assert_eq!(conflict.status.code(), Some(2));
    let envelope: serde_json::Value = serde_json::from_slice(&conflict.stderr).unwrap();
    assert_eq!(envelope["code"], "idempotency_conflict");
}

#[test]
fn seal_holdout_and_reconcile_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd);
    stdout_json(&run(
        &[
            "ingest",
            "m.db",
            "trades",
            "trades.csv",
            "--mode",
            "write",
            "--format",
            "json",
        ],
        cwd,
    ));
    stdout_json(&run(
        &[
            "seal",
            "m.db",
            "--id",
            "hold",
            "--table",
            "trades",
            "--start",
            "0",
            "--end",
            "4102444800000000000",
            "--idempotency-key",
            "seal-1",
            "--format",
            "json",
        ],
        cwd,
    ));
    let hidden = stdout_json(&run(
        &[
            "query",
            "m.db",
            "SELECT count(*) AS n FROM trades",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert_eq!(hidden[0]["n"], 0);

    let evaluation = stdout_json(&run(
        &[
            "holdout",
            "m.db",
            "--seal",
            "hold",
            "--code",
            "frozen",
            "--parameters",
            "{}",
            "--data-version",
            "v1",
            "--idempotency-key",
            "eval-1",
            "--sql",
            "SELECT count(*) AS n FROM trades",
            "--decision-time",
            "4102444800000000000",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert_eq!(evaluation["state"], "completed");
    assert_eq!(evaluation["summary"]["rows"][0]["n"], 3);

    let again = stdout_json(&run(
        &[
            "holdout",
            "m.db",
            "--seal",
            "hold",
            "--code",
            "frozen",
            "--parameters",
            "{}",
            "--data-version",
            "v1",
            "--idempotency-key",
            "eval-1",
            "--summary",
            "{\"ignored\":true}",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert_eq!(again["summary"]["rows"][0]["n"], 3);

    let denied = run(
        &[
            "holdout",
            "m.db",
            "--seal",
            "hold",
            "--code",
            "other",
            "--parameters",
            "{}",
            "--data-version",
            "v1",
            "--idempotency-key",
            "eval-2",
            "--summary",
            "{}",
            "--format",
            "json",
        ],
        cwd,
    );
    assert_eq!(denied.status.code(), Some(2));
    let envelope: serde_json::Value = serde_json::from_slice(&denied.stderr).unwrap();
    assert_eq!(envelope["code"], "holdout_used");

    let state = stdout_json(&run(&["reconcile", "m.db", "--format", "json"], cwd));
    std::fs::write(cwd.join("ledger.json"), serde_json::to_vec(&state).unwrap()).unwrap();
    let agree = stdout_json(&run(
        &[
            "reconcile",
            "m.db",
            "--ledger",
            "ledger.json",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert!(agree["missing"].as_array().unwrap().is_empty());
    assert!(agree["extra"].as_array().unwrap().is_empty());

    std::fs::write(cwd.join("ledger.json"), b"{\"format\":1,\"forks\":[],\"versions\":[],\"trials\":[],\"evaluations\":[],\"promotions\":[]}").unwrap();
    let disagree = run(
        &[
            "reconcile",
            "m.db",
            "--ledger",
            "ledger.json",
            "--format",
            "json",
        ],
        cwd,
    );
    assert_eq!(disagree.status.code(), Some(2));
    let diff: serde_json::Value = serde_json::from_slice(&disagree.stdout).unwrap();
    assert!(!diff["extra"].as_array().unwrap().is_empty());
}
