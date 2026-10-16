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

pub struct Comment {
    pub family: u64,
    pub text: String,
}

pub struct AgentProc {
    pub agent: u64,
    pub mode: SandboxMode,
    pub child: Child,
    pub output: Arc<Mutex<String>>,
    readers: Vec<JoinHandle<()>>,
    pub started: Instant,
}

pub struct Runner {
    pub procs: Vec<AgentProc>,
    pub comments: Vec<Comment>,
}

impl Runner {
    pub fn new() -> Self {
        Runner {
            procs: Vec::new(),
            comments: Vec::new(),
        }
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
) -> Result<(), String> {
    start_shell(
        runner,
        agent,
        mode,
        &format!("sh {}", shell_quote(script)),
        data_dir,
        None,
    )
}

pub fn start_shell(
    runner: &Mutex<Runner>,
    agent: u64,
    mode: SandboxMode,
    command: &str,
    data_dir: &str,
    boxed: Option<BoxSpec>,
) -> Result<(), String> {
    let mut runner = runner.lock().expect("runner");
    if let Some(pos) = runner.procs.iter().position(|proc| proc.agent == agent) {
        let mut old = runner.procs.remove(pos);
        let _ = old.child.kill();
        for reader in old.readers {
            let _ = reader.join();
        }
    }
    let (child, output, readers) = spawn(mode, command, data_dir, boxed)?;
    runner.procs.push(AgentProc {
        agent,
        mode,
        child,
        output,
        readers,
        started: Instant::now(),
    });
    Ok(())
}

pub fn stop_agent(runner: &Mutex<Runner>, agent: u64) -> Result<(), String> {
    let mut runner = runner.lock().expect("runner");
    let Some(pos) = runner.procs.iter().position(|proc| proc.agent == agent) else {
        return Err("not running".into());
    };
    let mut proc = runner.procs.remove(pos);
    proc.child.kill().map_err(|e| e.to_string())?;
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
    pub output: String,
    pub elapsed: Duration,
}

pub fn activity(runner: &Mutex<Runner>) -> Vec<Activity> {
    let mut runner = runner.lock().expect("runner");
    runner
        .procs
        .iter_mut()
        .map(|proc| {
            let running = proc.child.try_wait().ok().flatten().is_none();
            Activity {
                agent: proc.agent,
                trust: proc.mode.label(),
                pid: proc.child.id(),
                running,
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
    cmd.env("DATA_DIR", data_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
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
}
