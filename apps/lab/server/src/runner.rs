//! Start and stop agent processes.
//!
//! Default is no sandbox: the protocol is then advisory against an agent that
//! has a shell, because it can open the data directory itself. Opt-in, the
//! process runs inside an h5i box whose policy does not grant that directory.
//! The box has to exist already (`LAB_BOX` is its name, `LAB_BOX_DIR` the
//! repository the box was created in). A caller can pass those two instead of
//! the environment.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read};
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SandboxMode {
    /// The API refuses a sealed read. A shell can still open the files.
    Advisory,
    /// The API refuses, and the box policy refuses the files too.
    Box,
}

impl SandboxMode {
    pub fn label(self) -> &'static str {
        match self {
            SandboxMode::Advisory => "advisory",
            SandboxMode::Box => "enforced",
        }
    }
}

pub struct AgentProc {
    pub agent: u64,
    pub mode: SandboxMode,
    pub child: Child,
    pub output: Arc<Mutex<String>>,
    readers: Vec<JoinHandle<()>>,
    pub started: Instant,
    pub paused: bool,
}

pub struct Runner {
    pub procs: Vec<AgentProc>,
}

impl Runner {
    pub fn new() -> Self {
        Runner { procs: Vec::new() }
    }
}

impl Default for Runner {
    fn default() -> Self {
        Self::new()
    }
}

/// Where an enforced agent runs. `None` reads `LAB_BOX` and `LAB_BOX_DIR`.
pub struct BoxSpec {
    pub name: String,
    pub dir: PathBuf,
}

pub fn start_agent(
    runner: &Mutex<Runner>,
    agent: u64,
    mode: SandboxMode,
    script: &str,
    data_dir: &str,
    token: Option<&str>,
) -> Result<(), String> {
    start_shell(
        runner,
        agent,
        mode,
        &format!("sh {}", shell_quote(script)),
        data_dir,
        None,
        token,
    )
}

pub fn start_shell(
    runner: &Mutex<Runner>,
    agent: u64,
    mode: SandboxMode,
    command: &str,
    data_dir: &str,
    boxed: Option<BoxSpec>,
    token: Option<&str>,
) -> Result<(), String> {
    let mut runner = runner.lock().expect("runner");
    if let Some(pos) = runner.procs.iter().position(|proc| proc.agent == agent) {
        let mut old = runner.procs.remove(pos);
        let _ = signal_tree(old.child.id(), libc::SIGKILL);
        let _ = old.child.wait();
        for reader in old.readers {
            let _ = reader.join();
        }
    }
    let (child, output, readers) = spawn(mode, command, data_dir, boxed, agent, token)?;
    runner.procs.push(AgentProc {
        agent,
        mode,
        child,
        output,
        readers,
        started: Instant::now(),
        paused: false,
    });
    Ok(())
}

fn signal_tree(pid: u32, sig: i32) -> Result<(), String> {
    // The child is its own process group, so this reaches processes it spawned.
    let rc = unsafe { libc::kill(-(pid as i32), sig) };
    if rc == 0 {
        Ok(())
    } else {
        Err(format!(
            "signal {sig} to {pid}: {}",
            std::io::Error::last_os_error()
        ))
    }
}

pub fn pause_agent(runner: &Mutex<Runner>, agent: u64) -> Result<(), String> {
    let mut runner = runner.lock().expect("runner");
    let Some(proc) = runner.procs.iter_mut().find(|proc| proc.agent == agent) else {
        return Err("not running".into());
    };
    signal_tree(proc.child.id(), libc::SIGSTOP)?;
    proc.paused = true;
    Ok(())
}

pub fn resume_agent(runner: &Mutex<Runner>, agent: u64) -> Result<(), String> {
    let mut runner = runner.lock().expect("runner");
    let Some(proc) = runner.procs.iter_mut().find(|proc| proc.agent == agent) else {
        return Err("not running".into());
    };
    signal_tree(proc.child.id(), libc::SIGCONT)?;
    proc.paused = false;
    Ok(())
}

/// Resident set from `/proc`, in kilobytes. Zero when the process is gone.
pub fn rss_kb(pid: u32) -> u64 {
    let Ok(text) = std::fs::read_to_string(format!("/proc/{pid}/status")) else {
        return 0;
    };
    for line in text.lines() {
        let Some(rest) = line.strip_prefix("VmRSS:") else {
            continue;
        };
        return rest
            .split_whitespace()
            .next()
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
    }
    0
}

pub fn stop_agent(runner: &Mutex<Runner>, agent: u64) -> Result<(), String> {
    let mut runner = runner.lock().expect("runner");
    let Some(pos) = runner.procs.iter().position(|proc| proc.agent == agent) else {
        return Err("not running".into());
    };
    let mut proc = runner.procs.remove(pos);
    signal_tree(proc.child.id(), libc::SIGKILL)?;
    let _ = proc.child.wait();
    for reader in proc.readers {
        let _ = reader.join();
    }
    Ok(())
}

/// Wait for the agent and return its exit status and captured output.
pub fn wait_agent(runner: &Mutex<Runner>, agent: u64) -> Result<(ExitStatus, String), String> {
    let mut runner = runner.lock().expect("runner");
    let Some(pos) = runner.procs.iter().position(|proc| proc.agent == agent) else {
        return Err("not running".into());
    };
    let mut proc = runner.procs.remove(pos);
    let status = proc.child.wait().map_err(|e| e.to_string())?;
    for reader in proc.readers {
        let _ = reader.join();
    }
    let text = proc.output.lock().expect("output").clone();
    Ok((status, text))
}

pub struct Activity {
    pub agent: u64,
    pub trust: &'static str,
    pub pid: u32,
    pub running: bool,
    pub paused: bool,
    pub rss_kb: u64,
    pub output: String,
    pub elapsed: Duration,
}

pub fn activity(runner: &Mutex<Runner>) -> Vec<Activity> {
    let mut runner = runner.lock().expect("runner");
    runner
        .procs
        .iter_mut()
        .map(|proc| {
            let pid = proc.child.id();
            let running = proc.child.try_wait().ok().flatten().is_none();
            Activity {
                agent: proc.agent,
                trust: proc.mode.label(),
                pid,
                running,
                paused: proc.paused && running,
                rss_kb: rss_kb(pid),
                output: proc.output.lock().expect("output").clone(),
                elapsed: proc.started.elapsed(),
            }
        })
        .collect()
}

/// Same non-empty line eight times is a looping agent.
pub fn looks_looping(output: &str) -> bool {
    let mut counts = BTreeMap::new();
    for line in output.lines() {
        if line.is_empty() {
            continue;
        }
        let count = counts.entry(line.to_string()).or_insert(0);
        *count += 1;
        if *count >= 8 {
            return true;
        }
    }
    false
}

/// A live process with no output for `quiet` is stalled.
pub fn looks_stalled(running: bool, output_len: usize, elapsed: Duration, quiet: Duration) -> bool {
    running && output_len == 0 && elapsed >= quiet
}

fn shell_quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "'\\''"))
}

fn spawn(
    mode: SandboxMode,
    command: &str,
    data_dir: &str,
    boxed: Option<BoxSpec>,
    agent: u64,
    token: Option<&str>,
) -> Result<(Child, Arc<Mutex<String>>, Vec<JoinHandle<()>>), String> {
    let mut cmd = match mode {
        SandboxMode::Advisory => {
            let mut cmd = Command::new("sh");
            cmd.arg("-c").arg(command);
            cmd
        }
        SandboxMode::Box => {
            let spec = match boxed {
                Some(spec) => spec,
                None => BoxSpec {
                    name: std::env::var("LAB_BOX").map_err(|_| "LAB_BOX is not set".to_string())?,
                    dir: PathBuf::from(
                        std::env::var("LAB_BOX_DIR")
                            .map_err(|_| "LAB_BOX_DIR is not set".to_string())?,
                    ),
                },
            };
            // The path is in the environment so the agent can try. The box
            // policy is what makes the open fail; the variable is not a grant.
            let mut cmd = Command::new("h5i");
            cmd.current_dir(spec.dir)
                .arg("box")
                .arg("run")
                .arg(spec.name)
                .arg("--")
                .arg("sh")
                .arg("-c")
                .arg(command);
            cmd
        }
    };
    unsafe {
        cmd.pre_exec(|| {
            if libc::setpgid(0, 0) == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        });
    }
    cmd.env("DATA_DIR", data_dir)
        .env("LAB_AGENT", agent.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(token) = token {
        cmd.env("LAB_TOKEN", token);
    }
    let mut child = cmd.spawn().map_err(|e| e.to_string())?;
    let output = Arc::new(Mutex::new(String::new()));
    let mut readers = Vec::new();
    let streams: [Option<Box<dyn Read + Send>>; 2] = [
        child
            .stdout
            .take()
            .map(|stream| Box::new(stream) as Box<dyn Read + Send>),
        child
            .stderr
            .take()
            .map(|stream| Box::new(stream) as Box<dyn Read + Send>),
    ];
    for stream in streams.into_iter().flatten() {
        let buf = Arc::clone(&output);
        readers.push(std::thread::spawn(move || {
            let reader = BufReader::new(stream);
            for line in reader.lines() {
                let Ok(line) = line else { break };
                let mut text = buf.lock().expect("output");
                text.push_str(&line);
                text.push('\n');
                if text.len() > 8_000 {
                    let next = text.len() - 4_000;
                    *text = text.split_off(next);
                }
            }
        }));
    }
    Ok((child, output, readers))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_repeated_line_is_a_loop_and_silence_is_a_stall() {
        let mut output = String::new();
        for _ in 0..8 {
            output.push_str("retry the same trial\n");
        }
        assert!(looks_looping(&output));
        assert!(!looks_looping("one line\n"));
        assert!(looks_stalled(
            true,
            0,
            Duration::from_secs(30),
            Duration::from_secs(30)
        ));
        assert!(!looks_stalled(
            true,
            4,
            Duration::from_secs(30),
            Duration::from_secs(30)
        ));
        assert!(!looks_stalled(
            false,
            0,
            Duration::from_secs(60),
            Duration::from_secs(30)
        ));
    }

    fn proc_state(pid: u32) -> String {
        let text = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
        text.split(") ")
            .nth(1)
            .and_then(|rest| rest.split_whitespace().next())
            .unwrap_or("")
            .to_string()
    }

    #[test]
    fn pause_stops_the_process_until_resume() {
        let runner = Mutex::new(Runner::new());
        start_shell(
            &runner,
            7,
            SandboxMode::Advisory,
            "sleep 30",
            "/tmp",
            None,
            None,
        )
        .unwrap();
        let pid = activity(&runner)[0].pid;
        pause_agent(&runner, 7).unwrap();
        // SIGSTOP is delivered asynchronously; a loaded host can still report
        // the process as running for a moment after kill() returns.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while proc_state(pid) != "T" && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(proc_state(pid), "T");
        assert!(activity(&runner)[0].paused);
        assert!(rss_kb(pid) > 0);
        resume_agent(&runner, 7).unwrap();
        let state = proc_state(pid);
        assert!(state == "S" || state == "R", "{state}");
        assert!(!activity(&runner)[0].paused);
        stop_agent(&runner, 7).unwrap();
    }

    #[test]
    fn the_child_receives_its_scoped_token() {
        let runner = Mutex::new(Runner::new());
        start_shell(
            &runner,
            4,
            SandboxMode::Advisory,
            "printf %s \"$LAB_TOKEN\"",
            "/tmp",
            None,
            Some("scoped-token"),
        )
        .unwrap();
        let (status, output) = wait_agent(&runner, 4).unwrap();
        assert!(status.success(), "{output}");
        assert_eq!(output.trim(), "scoped-token");
    }
}
