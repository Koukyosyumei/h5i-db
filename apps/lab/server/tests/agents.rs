//! Two fake agents. The honest one does not touch the data directory. The
//! cheater tries to read it. Without a sandbox the read succeeds and the
//! ledger still refuses a self-recorded trial. Inside an h5i box the read
//! fails as well.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Mutex;

use lab_kernel as k;
use lab_server::{BoxSpec, Runner, SandboxMode, start_shell, wait_agent};

fn human() -> k::Principal {
    k::Principal {
        org: 1,
        user: 1,
        role: k::ROLE_HUMAN,
        agent: 0,
    }
}
fn agent(id: u64) -> k::Principal {
    k::Principal {
        org: 1,
        user: id,
        role: k::ROLE_AGENT,
        agent: id,
    }
}

fn step(s: &k::Snapshot, actor: k::Principal, cmd: k::Command) -> k::Snapshot {
    match k::transition(&actor, s, &cmd) {
        Ok((ws, _)) => k::apply(s, &ws),
        Err(error) => panic!("{error:?}"),
    }
}

fn run(mode: SandboxMode, command: &str, data_dir: &str, boxed: Option<BoxSpec>) -> (bool, String) {
    let runner = Mutex::new(Runner::new());
    start_shell(&runner, 1, mode, command, data_dir, boxed, None).unwrap();
    let (status, output) = wait_agent(&runner, 1).unwrap();
    (status.success(), output)
}

const BOX_ENV: &str = r#"[profile.lab]
isolation = "process"

[profile.lab.fs]
read = [
  "/usr", "/lib", "/lib64", "/bin", "/sbin", "/etc", "/nix", "/opt",
  "/dev/null", "/dev/zero", "/dev/urandom", "/proc",
]
write = ["$WORK"]

[profile.lab.net]
mode = "host"
unix = false
"#;

fn box_repo() -> PathBuf {
    let repo = PathBuf::from("/var/tmp/h5idb-lab-box/repo");
    if !repo.join(".git").exists() {
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::create_dir_all(repo.join(".h5i")).unwrap();
        std::fs::write(repo.join(".h5i/env.toml"), BOX_ENV).unwrap();
        std::fs::write(repo.join("README"), "lab box profile\n").unwrap();
        let git = |args: &[&str]| {
            let status = Command::new("git")
                .args(args)
                .current_dir(&repo)
                .env("GIT_AUTHOR_NAME", "koukyosyumei")
                .env("GIT_AUTHOR_EMAIL", "koukyosyumei@hotmail.com")
                .env("GIT_COMMITTER_NAME", "koukyosyumei")
                .env("GIT_COMMITTER_EMAIL", "koukyosyumei@hotmail.com")
                .status()
                .unwrap();
            assert!(status.success(), "git {args:?}");
        };
        git(&["init"]);
        git(&["add", "README"]);
        git(&["add", "-f", ".h5i/env.toml"]);
        git(&["commit", "-m", "lab box profile"]);
    }
    repo
}

/// `h5i box probe` prints `process tier runnable = yes` only when a confined
/// command can actually execute. Landlock support alone is not enough.
fn process_tier_runs() -> bool {
    let output = Command::new("h5i")
        .args(["box", "probe"])
        .output()
        .expect("h5i box probe");
    let text = String::from_utf8_lossy(&output.stdout);
    text.lines().any(|line| {
        line.contains("process tier runnable") && line.contains("yes") && !line.contains("no")
    })
}

#[test]
fn advisory_lets_a_shell_read_files_and_the_ledger_still_refuses() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("secret"), b"sealed-bytes\n").unwrap();
    let data = dir.path().to_str().unwrap();

    let (honest_ok, honest_out) = run(SandboxMode::Advisory, "echo honest", data, None);
    assert!(honest_ok, "{honest_out}");
    assert!(honest_out.contains("honest"));

    let (cheat_ok, cheat_out) = run(
        SandboxMode::Advisory,
        "cat \"$DATA_DIR/secret\"",
        data,
        None,
    );
    assert!(
        cheat_ok,
        "without a sandbox the file read succeeds: {cheat_out}"
    );
    assert!(cheat_out.contains("sealed-bytes"));

    let mut snap = k::Snapshot::default();
    snap = step(
        &snap,
        human(),
        k::Command::Enroll {
            agent: 1,
            budget: 1,
            token_budget: 1,
            sandbox: 0,
        },
    );
    snap = step(
        &snap,
        human(),
        k::Command::Enroll {
            agent: 2,
            budget: 1,
            token_budget: 1,
            sandbox: 1,
        },
    );
    let (snap, created) = match k::transition(
        &agent(1),
        &snap,
        &k::Command::PreRegister {
            hypothesis: 1,
            method: 2,
        },
    ) {
        Ok((ws, reply)) => (k::apply(&snap, &ws), reply),
        Err(error) => panic!("{error:?}"),
    };
    let k::Reply::Created(family) = created else {
        panic!("family")
    };
    let (snap, pending) = match k::transition(
        &agent(1),
        &snap,
        &k::Command::RequestTrial {
            family,
            code: 3,
            params: 4,
            decision_time: 5,
        },
    ) {
        Ok((ws, reply)) => (k::apply(&snap, &ws), reply),
        Err(error) => panic!("{error:?}"),
    };
    let k::Reply::Created(pending_id) = pending else {
        panic!("pending")
    };
    let dropped = k::transition(
        &agent(1),
        &snap,
        &k::Command::RecordTrial {
            pending: pending_id,
            data_version: 1,
            score: 0,
            corrected: 0,
            sharpe: 0,
            skew: 0,
            kurt: 0,
            samples: 0,
            tokens: 0,
        },
    );
    assert!(
        matches!(dropped, Err(k::Error::Forbidden)),
        "the cheater cannot drop a failed trial by recording it"
    );
    let other = k::transition(&agent(2), &snap, &k::Command::Report { family });
    assert!(matches!(other, Err(k::Error::Forbidden)));
}

#[test]
fn a_box_denies_the_data_directory() {
    let repo = box_repo();
    let profile = std::fs::read_to_string(repo.join(".h5i/env.toml")).unwrap();
    assert!(
        !profile.contains("/var/tmp/h5idb"),
        "the profile must not grant the data directory"
    );
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("secret"), b"sealed-bytes\n").unwrap();
    let data = dir.path().to_str().unwrap().to_string();
    assert!(
        !profile.contains(&data),
        "the profile must not grant this test's data directory"
    );

    let spec = BoxSpec {
        name: "labagent".into(),
        dir: repo.clone(),
    };
    if process_tier_runs() {
        let ready = PathBuf::from("/var/tmp/h5idb-lab-box/ready");
        if !ready.exists() {
            let output = Command::new("h5i")
                .args([
                    "box",
                    "create",
                    "labagent",
                    "--profile",
                    "lab",
                    "--isolation",
                    "process",
                ])
                .current_dir(&repo)
                .output()
                .expect("h5i box");
            assert!(
                output.status.success(),
                "h5i box create failed\n{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            std::fs::write(ready, "ok").unwrap();
        }
        let (honest_ok, honest_out) = run(
            SandboxMode::Box,
            "echo honest",
            &data,
            Some(BoxSpec {
                name: spec.name.clone(),
                dir: spec.dir.clone(),
            }),
        );
        assert!(
            honest_ok,
            "an enforced agent can still run a command that does not touch the data: {honest_out}"
        );
        let (cheat_ok, cheat_out) = run(
            SandboxMode::Box,
            "cat \"$DATA_DIR/secret\"",
            &data,
            Some(spec),
        );
        assert!(
            !cheat_ok,
            "the box must refuse the data directory, got: {cheat_out}"
        );
        assert!(!cheat_out.contains("sealed-bytes"), "{cheat_out}");
        return;
    }

    // The process tier is not runnable here. Ubuntu 24.04 with
    // kernel.apparmor_restrict_unprivileged_userns=1 denies the
    // unprivileged CLONE_NEWUSER that `h5i box` requires, so `true` inside
    // the sandbox returns EPERM. Create must fail closed. The runner must
    // not fall back to an unconfined shell. See README.md.
    let created = Command::new("h5i")
        .args([
            "box",
            "create",
            "labagent",
            "--profile",
            "lab",
            "--isolation",
            "process",
        ])
        .current_dir(&repo)
        .output()
        .expect("h5i box");
    assert!(
        !created.status.success(),
        "process tier is not runnable, so create must fail closed\n{}{}",
        String::from_utf8_lossy(&created.stdout),
        String::from_utf8_lossy(&created.stderr)
    );
    let (honest_ok, honest_out) = run(
        SandboxMode::Box,
        "echo honest",
        &data,
        Some(BoxSpec {
            name: spec.name.clone(),
            dir: spec.dir.clone(),
        }),
    );
    assert!(
        !honest_ok,
        "the runner must not execute the agent outside a box: {honest_out}"
    );
    let (cheat_ok, cheat_out) = run(
        SandboxMode::Box,
        "cat \"$DATA_DIR/secret\"",
        &data,
        Some(spec),
    );
    assert!(!cheat_ok, "{cheat_out}");
    assert!(!cheat_out.contains("sealed-bytes"), "{cheat_out}");
}
