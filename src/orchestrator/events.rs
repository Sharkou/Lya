use std::{
    error::Error,
    fmt,
    fs::{self, OpenOptions},
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use super::{
    executor::ExecutorResult,
    publisher::{PublishResult, PublishStage},
    repository::RepositoryState,
    supervisor::{Project, SupervisorDecision},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobEvent {
    pub timestamp_unix_millis: u64,
    pub job_id: String,
    pub project_name: String,
    pub project_path: PathBuf,
    pub iteration: Option<u32>,
    #[serde(flatten)]
    pub kind: JobEventKind,
}

impl JobEvent {
    pub fn new(
        job_id: impl Into<String>,
        project: &Project,
        iteration: Option<u32>,
        kind: JobEventKind,
    ) -> Self {
        Self {
            timestamp_unix_millis: current_unix_millis(),
            job_id: job_id.into(),
            project_name: project.name.clone(),
            project_path: project.path.clone(),
            iteration,
            kind,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobEventKind {
    JobStarted {
        task: String,
    },
    JobFinished {
        status: String,
    },
    SupervisorStarted {
        prompt: String,
    },
    SupervisorFinished {
        action: String,
        reason: Option<String>,
        prompt: Option<String>,
        commit_title: Option<String>,
        next_prompt: Option<String>,
    },
    ExecutorStarted {
        prompt: String,
        session_id: Option<String>,
    },
    ExecutorFinished {
        session_id: Option<String>,
        final_response: String,
        exit_code: Option<i32>,
        duration_ms: Option<u64>,
        turns: Option<u32>,
        total_cost_usd: Option<f64>,
        usage: Option<serde_json::Value>,
    },
    RepositoryCaptured {
        summary: RepositorySummary,
    },
    PublishStarted {
        commit_title: String,
    },
    PublishStageChanged {
        stage: PublishStage,
    },
    Published {
        result: PublishResult,
    },
    WaitingForQuota {
        provider: String,
        reason: String,
    },
    WaitingForHuman {
        reason: String,
    },
    PauseRequested,
    Paused {
        reason: String,
    },
    Resumed,
    StopRequested,
    UserInstructionQueued {
        instruction: String,
    },
    UserInstructionApplied {
        instruction: String,
    },
    UserInstructionRejected {
        instruction: String,
        reason: String,
    },
    ResumeStarted {
        status: String,
        pending_operation: Option<String>,
    },
    ResumeValidated {
        continuation: String,
        head: String,
    },
    ResumeRejected {
        reason: String,
    },
    QuotaRetryStarted {
        provider: String,
        operation: String,
    },
    StatusReported {
        status: String,
        phase: String,
        claude_session_id: Option<String>,
        publish_stage: Option<PublishStage>,
        pause_requested: bool,
        stop_requested: bool,
    },
    DiffReported {
        summary: RepositorySummary,
    },
    ControlMessage {
        message: String,
    },
    Stopped {
        reason: String,
    },
    Failed {
        error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositorySummary {
    pub head: String,
    pub clean: bool,
    pub changed_paths: Vec<String>,
    pub untracked_paths: Vec<String>,
    pub binary_untracked_paths: Vec<String>,
    pub diff_stat: String,
    pub diff_truncated: bool,
    pub diff_total_bytes: usize,
    pub untracked_truncated: bool,
    pub untracked_total_bytes: usize,
}

impl From<&RepositoryState> for RepositorySummary {
    fn from(state: &RepositoryState) -> Self {
        Self {
            head: state.head.clone(),
            clean: state.is_clean(),
            changed_paths: state.changed_files.clone(),
            untracked_paths: state
                .untracked_files
                .iter()
                .map(|file| file.path.clone())
                .collect(),
            binary_untracked_paths: state
                .untracked_files
                .iter()
                .filter(|file| file.is_binary)
                .map(|file| file.path.clone())
                .collect(),
            diff_stat: state.diff_stat.clone(),
            diff_truncated: state.diff_truncated,
            diff_total_bytes: state.diff_total_bytes,
            untracked_truncated: state.untracked_truncated,
            untracked_total_bytes: state.untracked_total_bytes,
        }
    }
}

pub trait EventSink: Send + Sync {
    fn emit(&self, event: &JobEvent) -> Result<(), EventSinkError>;
}

/// A sink can be shared by several concurrently driven jobs.
///
/// The scheduler gives every job its own `events.jsonl` but a single shared terminal or JSON sink,
/// so one writer owns stdout and concurrent jobs cannot interleave inside a rendered block.
impl<T: EventSink + ?Sized> EventSink for std::sync::Arc<T> {
    fn emit(&self, event: &JobEvent) -> Result<(), EventSinkError> {
        (**self).emit(event)
    }
}

#[derive(Debug, Default)]
pub struct NoopEventSink;

impl EventSink for NoopEventSink {
    fn emit(&self, _event: &JobEvent) -> Result<(), EventSinkError> {
        Ok(())
    }
}

pub struct CompositeEventSink {
    sinks: Vec<Box<dyn EventSink>>,
}

impl CompositeEventSink {
    pub fn new(sinks: Vec<Box<dyn EventSink>>) -> Self {
        Self { sinks }
    }
}

impl EventSink for CompositeEventSink {
    fn emit(&self, event: &JobEvent) -> Result<(), EventSinkError> {
        for sink in &self.sinks {
            sink.emit(event)?;
        }
        Ok(())
    }
}

pub struct JsonlEventSink {
    path: PathBuf,
    write_lock: Mutex<()>,
}

impl JsonlEventSink {
    pub fn for_job(home: &Path, job_id: &str) -> Self {
        Self::new(home.join("jobs").join(job_id).join("events.jsonl"))
    }

    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            write_lock: Mutex::new(()),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl EventSink for JsonlEventSink {
    fn emit(&self, event: &JobEvent) -> Result<(), EventSinkError> {
        let _lock = self
            .write_lock
            .lock()
            .map_err(|_| EventSinkError::Write("event sink lock was poisoned".to_owned()))?;
        let parent = self.path.parent().ok_or_else(|| {
            EventSinkError::Write("event log path has no parent directory".to_owned())
        })?;
        fs::create_dir_all(parent).map_err(|error| EventSinkError::Write(error.to_string()))?;
        let mut line = serde_json::to_vec(event)
            .map_err(|error| EventSinkError::Serialize(error.to_string()))?;
        line.push(b'\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| EventSinkError::Write(error.to_string()))?;
        file.write_all(&line)
            .map_err(|error| EventSinkError::Write(error.to_string()))?;
        file.sync_data()
            .map_err(|error| EventSinkError::Write(error.to_string()))
    }
}

pub struct JsonEventSink<W: Write + Send> {
    writer: Mutex<W>,
}

impl<W: Write + Send> JsonEventSink<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer: Mutex::new(writer),
        }
    }
}

impl<W: Write + Send> EventSink for JsonEventSink<W> {
    fn emit(&self, event: &JobEvent) -> Result<(), EventSinkError> {
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| EventSinkError::Write("JSON output lock was poisoned".to_owned()))?;
        serde_json::to_writer(&mut *writer, event)
            .map_err(|error| EventSinkError::Serialize(error.to_string()))?;
        writer
            .write_all(b"\n")
            .and_then(|_| writer.flush())
            .map_err(|error| EventSinkError::Write(error.to_string()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HumanRenderMode {
    Normal,
    Verbose,
}

pub struct HumanEventSink<W: Write + Send> {
    writer: Mutex<W>,
    mode: HumanRenderMode,
    color: bool,
    job_context: bool,
}

impl<W: Write + Send> HumanEventSink<W> {
    pub fn new(writer: W, mode: HumanRenderMode, color: bool) -> Self {
        Self {
            writer: Mutex::new(writer),
            mode,
            color,
            job_context: false,
        }
    }

    /// Prefix every rendered line with the originating project and job.
    ///
    /// Single-job runs leave this off: there is nothing to disambiguate. Concurrently scheduled
    /// jobs turn it on, so an interleaved terminal stream still says which project and which job
    /// each line belongs to.
    pub fn with_job_context(mut self, job_context: bool) -> Self {
        self.job_context = job_context;
        self
    }
}

impl HumanEventSink<io::Stdout> {
    pub fn stdout(mode: HumanRenderMode) -> Self {
        Self::new(io::stdout(), mode, io::stdout().is_terminal())
    }
}

impl<W: Write + Send> EventSink for HumanEventSink<W> {
    fn emit(&self, event: &JobEvent) -> Result<(), EventSinkError> {
        let rendered = render_human(event, self.mode, self.color);
        let rendered = if self.job_context {
            prefix_lines(
                &rendered,
                &format!("[{} {}] ", event.project_name, event.job_id),
            )
        } else {
            rendered
        };
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| EventSinkError::Write("terminal output lock was poisoned".to_owned()))?;
        // One `write_all` per event: a rendered block reaches the terminal as a unit, so events
        // from concurrent jobs interleave between blocks and never inside one.
        writer
            .write_all(rendered.as_bytes())
            .and_then(|_| writer.flush())
            .map_err(|error| EventSinkError::Write(error.to_string()))
    }
}

pub fn supervisor_event_fields(
    decision: &SupervisorDecision,
) -> (
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    match decision {
        SupervisorDecision::Claude { prompt, reason } => (
            "CLAUDE".to_owned(),
            reason.clone(),
            Some(prompt.clone()),
            None,
            None,
        ),
        SupervisorDecision::Accept {
            commit_title,
            next_prompt,
            reason,
        } => (
            "ACCEPT".to_owned(),
            reason.clone(),
            None,
            Some(commit_title.clone()),
            next_prompt.clone(),
        ),
        SupervisorDecision::Human { reason } => {
            ("HUMAN".to_owned(), Some(reason.clone()), None, None, None)
        }
        SupervisorDecision::Stop { reason } => {
            ("STOP".to_owned(), Some(reason.clone()), None, None, None)
        }
    }
}

pub fn executor_event_kind(result: ExecutorResult) -> JobEventKind {
    JobEventKind::ExecutorFinished {
        session_id: result.session_id,
        final_response: result.final_response,
        exit_code: result.exit_code,
        duration_ms: result.duration_ms,
        turns: result.turns,
        total_cost_usd: result.total_cost_usd,
        usage: result.usage,
    }
}

pub fn render_human(event: &JobEvent, mode: HumanRenderMode, color: bool) -> String {
    let timestamp = format_timestamp(event.timestamp_unix_millis);
    let heading = |name: &str| style(name, "36", color);
    let detail = |value: &str| format!("      {value}\n");
    let full = |value: &str| format!("      {}\n", indent(value, "      "));
    let mut output = String::new();
    match &event.kind {
        JobEventKind::JobStarted { task } => {
            output.push_str(&format!(
                "{timestamp}  {}  {}\n",
                heading("JOB"),
                event.project_name
            ));
            output.push_str(&detail(&truncate(task, 180)));
        }
        JobEventKind::JobFinished { status } => {
            output.push_str(&format!(
                "{timestamp}  {}  {}\n",
                heading("JOB"),
                style(status, "32", color)
            ));
        }
        JobEventKind::SupervisorStarted { prompt } => {
            output.push_str(&format!("{timestamp}  {}\n", heading("SUPERVISOR")));
            output.push_str(&detail("review started"));
            if mode == HumanRenderMode::Verbose {
                output.push_str(&full(prompt));
            }
        }
        JobEventKind::SupervisorFinished {
            action,
            reason,
            prompt,
            commit_title,
            next_prompt,
        } => {
            output.push_str(&format!(
                "{timestamp}  {}  {}\n",
                heading("SUPERVISOR"),
                style(action, "35", color)
            ));
            if let Some(reason) = reason {
                output.push_str(&detail(&truncate(reason, 360)));
            }
            if let Some(commit_title) = commit_title {
                output.push_str(&detail(&format!("commit: {commit_title}")));
            }
            if let Some(prompt) = prompt {
                if mode == HumanRenderMode::Verbose {
                    output.push_str(&full(&format!("Claude prompt:\n{prompt}")));
                } else {
                    output.push_str(&detail(&format!("Claude: {}", truncate(prompt, 360))));
                }
            }
            if let Some(next_prompt) = next_prompt {
                output.push_str(&detail(&format!("next: {}", truncate(next_prompt, 360))));
            }
        }
        JobEventKind::ExecutorStarted {
            prompt: _,
            session_id,
        } => {
            output.push_str(&format!("{timestamp}  {}\n", heading("CLAUDE")));
            if let Some(session_id) = session_id {
                output.push_str(&detail(&format!("resuming session {session_id}")));
            } else {
                output.push_str(&detail("starting requested execution"));
            }
        }
        JobEventKind::ExecutorFinished {
            session_id,
            final_response,
            duration_ms,
            turns,
            total_cost_usd,
            ..
        } => {
            output.push_str(&format!("{timestamp}  {}\n", heading("CLAUDE")));
            if let Some(session_id) = session_id {
                output.push_str(&detail(&format!("session {session_id}")));
            }
            if mode == HumanRenderMode::Verbose {
                if let Some(duration_ms) = duration_ms {
                    output.push_str(&detail(&format!("reported duration: {duration_ms} ms")));
                }
                if let Some(turns) = turns {
                    output.push_str(&detail(&format!("reported turns: {turns}")));
                }
                if let Some(total_cost_usd) = total_cost_usd {
                    // Read straight out of Claude Code's own JSON envelope. Lya does not compute
                    // it, does not know how the provider CLI is authenticated, and so cannot say
                    // whether anything was charged — the wording has to stop short of claiming it
                    // either way. See `EXECUTOR_FINISHED` in the events documentation.
                    output.push_str(&detail(&format!(
                        "Claude-reported cost metadata: ${total_cost_usd:.4} (not proof of billing)"
                    )));
                }
                output.push_str(&full(final_response));
            } else {
                output.push_str(&detail(&truncate_response(final_response, 360)));
            }
        }
        JobEventKind::RepositoryCaptured { summary } => {
            output.push_str(&format!("{timestamp}  {}\n", heading("REPOSITORY")));
            output.push_str(&detail(&format!(
                "HEAD {} ({})",
                short_sha(&summary.head),
                if summary.clean { "clean" } else { "dirty" }
            )));
            if !summary.changed_paths.is_empty() {
                output.push_str(&detail(&format!(
                    "tracked: {}",
                    summary.changed_paths.join(", ")
                )));
            }
            if !summary.untracked_paths.is_empty() {
                output.push_str(&detail(&format!(
                    "untracked: {}",
                    summary.untracked_paths.join(", ")
                )));
            }
            if !summary.diff_stat.trim().is_empty() {
                output.push_str(&detail(&format!(
                    "tracked diff: {}",
                    truncate(&summary.diff_stat.replace('\n', "; "), 360)
                )));
            }
            if summary.diff_truncated || summary.untracked_truncated {
                output.push_str(&detail("repository capture is truncated"));
            }
            if mode == HumanRenderMode::Verbose && !summary.binary_untracked_paths.is_empty() {
                output.push_str(&detail(&format!(
                    "binary untracked: {}",
                    summary.binary_untracked_paths.join(", ")
                )));
            }
        }
        JobEventKind::PublishStarted { commit_title } => {
            output.push_str(&format!("{timestamp}  {}\n", heading("PUBLISH")));
            output.push_str(&detail(&format!("commit: {commit_title}")));
        }
        JobEventKind::PublishStageChanged { stage } => {
            output.push_str(&format!(
                "{timestamp}  {}  {}\n",
                heading("PUBLISH"),
                publish_stage_label(stage)
            ));
        }
        JobEventKind::Published { result } => {
            output.push_str(&format!(
                "{timestamp}  {}  {} {}\n",
                heading("PUBLISH"),
                style("committed", "32", color),
                short_sha(&result.commit_sha)
            ));
            output.push_str(&detail(&format!(
                "pushed {}/{}",
                result.remote, result.branch
            )));
        }
        JobEventKind::WaitingForQuota { provider, reason } => {
            output.push_str(&format!(
                "{timestamp}  {}  {}\n",
                heading("WAITING"),
                provider
            ));
            output.push_str(&detail(reason));
        }
        JobEventKind::WaitingForHuman { reason } => {
            output.push_str(&format!("{timestamp}  {}\n", heading("WAITING FOR HUMAN")));
            output.push_str(&detail(reason));
        }
        JobEventKind::PauseRequested => {
            output.push_str(&format!("{timestamp}  {}\n", heading("CONTROL")));
            output.push_str(&detail("pause requested; waiting for a safe boundary"));
        }
        JobEventKind::Paused { reason } => {
            output.push_str(&format!(
                "{timestamp}  {}  {}\n",
                heading("JOB"),
                style("PAUSED", "33", color)
            ));
            output.push_str(&detail(reason));
        }
        JobEventKind::Resumed => {
            output.push_str(&format!(
                "{timestamp}  {}  {}\n",
                heading("JOB"),
                style("RESUMED", "32", color)
            ));
        }
        JobEventKind::StopRequested => {
            output.push_str(&format!("{timestamp}  {}\n", heading("CONTROL")));
            output.push_str(&detail("stop requested; finishing safe shutdown"));
        }
        JobEventKind::UserInstructionQueued { instruction } => {
            output.push_str(&format!("{timestamp}  {}\n", heading("CONTROL")));
            output.push_str(&detail("instruction queued for the next agent turn"));
            output.push_str(&detail(&truncate(instruction, 360)));
        }
        JobEventKind::UserInstructionApplied { instruction } => {
            output.push_str(&format!("{timestamp}  {}\n", heading("CONTROL")));
            output.push_str(&detail("instruction applied to agent prompts"));
            if mode == HumanRenderMode::Verbose {
                output.push_str(&full(instruction));
            }
        }
        JobEventKind::UserInstructionRejected {
            instruction,
            reason,
        } => {
            output.push_str(&format!(
                "{timestamp}  {}
",
                heading("CONTROL")
            ));
            output.push_str(&detail(&format!("instruction refused: {reason}")));
            output.push_str(&detail(&truncate(instruction, 360)));
        }
        JobEventKind::ResumeStarted {
            status,
            pending_operation,
        } => {
            output.push_str(&format!(
                "{timestamp}  {}
",
                heading("RESUME")
            ));
            output.push_str(&detail(&format!(
                "persisted status: {status}; pending operation: {}",
                pending_operation.as_deref().unwrap_or("none")
            )));
        }
        JobEventKind::ResumeValidated { continuation, head } => {
            output.push_str(&format!(
                "{timestamp}  {}  {}
",
                heading("RESUME"),
                style("validated", "32", color)
            ));
            output.push_str(&detail(&format!(
                "continuing at {continuation}; HEAD {}",
                short_sha(head)
            )));
        }
        JobEventKind::ResumeRejected { reason } => {
            output.push_str(&format!(
                "{timestamp}  {}  {}
",
                heading("RESUME"),
                style("rejected", "31", color)
            ));
            output.push_str(&detail(reason));
        }
        JobEventKind::QuotaRetryStarted {
            provider,
            operation,
        } => {
            output.push_str(&format!(
                "{timestamp}  {}
",
                heading("RESUME")
            ));
            output.push_str(&detail(&format!(
                "retrying {operation} after the {provider} quota wait"
            )));
        }
        JobEventKind::StatusReported {
            status,
            phase,
            claude_session_id,
            publish_stage,
            pause_requested,
            stop_requested,
        } => {
            output.push_str(&format!("{timestamp}  {}  {status}\n", heading("STATUS")));
            output.push_str(&detail(&format!(
                "job: {}; project: {}",
                event.job_id, event.project_name
            )));
            output.push_str(&detail(&format!(
                "phase: {phase}; iteration: {}",
                event.iteration.unwrap_or(0)
            )));
            if let Some(session_id) = claude_session_id {
                output.push_str(&detail(&format!("Claude session: {session_id}")));
            }
            if let Some(stage) = publish_stage {
                output.push_str(&detail(&format!(
                    "publication: {}",
                    publish_stage_label(stage)
                )));
            }
            if *pause_requested || *stop_requested {
                output.push_str(&detail(&format!(
                    "pause requested: {pause_requested}; stop requested: {stop_requested}"
                )));
            }
        }
        JobEventKind::DiffReported { summary } => {
            output.push_str(&format!("{timestamp}  {}\n", heading("DIFF")));
            if summary.clean {
                output.push_str(&detail("working tree clean"));
            }
            if !summary.changed_paths.is_empty() {
                output.push_str(&detail(&format!(
                    "tracked: {}",
                    summary.changed_paths.join(", ")
                )));
            }
            if !summary.untracked_paths.is_empty() {
                output.push_str(&detail(&format!(
                    "untracked: {}",
                    summary.untracked_paths.join(", ")
                )));
            }
            if !summary.diff_stat.trim().is_empty() {
                output.push_str(&detail(&format!(
                    "stat: {}",
                    truncate(&summary.diff_stat.replace('\n', "; "), 360)
                )));
            }
        }
        JobEventKind::ControlMessage { message } => {
            output.push_str(&format!("{timestamp}  {}\n", heading("CONTROL")));
            output.push_str(&detail(message));
        }
        JobEventKind::Stopped { reason } => {
            output.push_str(&format!("{timestamp}  {}\n", heading("STOPPED")));
            output.push_str(&detail(reason));
        }
        JobEventKind::Failed { error } => {
            output.push_str(&format!("{timestamp}  {}\n", style("FAILED", "31", color)));
            output.push_str(&detail(error));
        }
    }
    output
}

/// Prefix every non-empty line of an already rendered block, keeping its line structure intact.
fn prefix_lines(rendered: &str, prefix: &str) -> String {
    let mut output = String::with_capacity(rendered.len());
    for line in rendered.split_inclusive('\n') {
        if line.trim_end_matches(['\r', '\n']).is_empty() {
            output.push_str(line);
            continue;
        }
        output.push_str(prefix);
        output.push_str(line);
    }
    output
}

/// Milliseconds since the Unix epoch.
///
/// 64 bits on purpose: an event is deserialized through serde's buffering — a flattened field or a
/// tagged enum — and that buffer has no 128-bit integer, so a wider timestamp could be written and
/// never read back.
pub(crate) fn current_unix_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

pub(crate) fn format_timestamp(timestamp_unix_millis: u64) -> String {
    let seconds = (timestamp_unix_millis / 1_000) % 86_400;
    format!(
        "{:02}:{:02}:{:02}",
        seconds / 3_600,
        (seconds % 3_600) / 60,
        seconds % 60
    )
}

pub(crate) fn style(value: &str, code: &str, color: bool) -> String {
    if color {
        format!("\x1b[{code}m{value}\x1b[0m")
    } else {
        value.to_owned()
    }
}

fn short_sha(value: &str) -> &str {
    value.get(..value.len().min(7)).unwrap_or(value)
}

fn truncate(value: &str, maximum: usize) -> String {
    if value.chars().count() <= maximum {
        return value.to_owned();
    }
    value.chars().take(maximum).collect::<String>() + "..."
}

fn truncate_response(value: &str, maximum: usize) -> String {
    if value.chars().count() <= maximum {
        return value.to_owned();
    }
    format!(
        "{}... (use --verbose for full response)",
        value.chars().take(maximum).collect::<String>()
    )
}

fn indent(value: &str, prefix: &str) -> String {
    value
        .lines()
        .map(|line| format!("{prefix}{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn publish_stage_label(stage: &PublishStage) -> &'static str {
    match stage {
        PublishStage::Verifying => "verifying snapshot",
        PublishStage::Staging => "staging accepted changes",
        PublishStage::Staged => "staged state verified",
        PublishStage::Committing => "committing",
        PublishStage::Committed => "committed",
        PublishStage::Pushing => "pushing",
        PublishStage::Pushed => "pushed",
    }
}

#[derive(Debug)]
pub enum EventSinkError {
    Serialize(String),
    Write(String),
}

impl fmt::Display for EventSinkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialize(error) => write!(formatter, "could not serialize job event: {error}"),
            Self::Write(error) => write!(formatter, "could not write job event: {error}"),
        }
    }
}

impl Error for EventSinkError {}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::{Arc, Mutex},
    };

    use super::{
        CompositeEventSink, EventSink, HumanEventSink, HumanRenderMode, JobEvent, JobEventKind,
        JsonEventSink, JsonlEventSink, RepositorySummary, render_human,
    };
    use crate::orchestrator::supervisor::Project;

    fn event(kind: JobEventKind) -> JobEvent {
        JobEvent::new(
            "job-1",
            &Project {
                name: "demo".to_owned(),
                path: PathBuf::from("C:/demo"),
            },
            Some(1),
            kind,
        )
    }

    /// Text a provider really produced, with the two characters that showed up as mojibake when a
    /// recorded log was read back by hand on Windows.
    const NON_ASCII_REPORT: &str = "em dash: \u{2014}\narrow: \u{2192}\nfixed malformed string literal \u{2014} node --check app.js \u{2192} passed";

    fn executor_finished(report: &str, total_cost_usd: Option<f64>) -> JobEvent {
        event(JobEventKind::ExecutorFinished {
            session_id: Some("session-1".to_owned()),
            final_response: report.to_owned(),
            exit_code: Some(0),
            duration_ms: Some(41_000),
            turns: Some(3),
            total_cost_usd,
            usage: None,
        })
    }

    /// Non-ASCII provider text survives the recorded log byte for byte.
    ///
    /// The file is deliberately plain UTF-8 with no byte-order mark, which is what every JSON
    /// consumer expects and what `serde_json` produces. This asserts the bytes rather than the
    /// decoded string, because "it round-trips" and "it is encoded the way it claims" are two
    /// different claims and only the second one explains a reader showing `\u{00e2}\u{20ac}\u{201d}`.
    ///
    /// That mojibake is a *reading* defect, not a writing one: Windows PowerShell 5.1's
    /// `Get-Content` decodes a BOM-less file with the legacy ANSI codepage, so the three bytes of
    /// an em dash arrive as three Windows-1252 characters. `Get-Content -Encoding UTF8` shows the
    /// real text. Nothing here compensates for that by adding a BOM or escaping non-ASCII, because
    /// either would corrupt the log for every correct consumer in order to flatter one incorrect
    /// one.
    #[test]
    fn recorded_events_keep_non_ascii_provider_text_as_plain_utf8() {
        let home = temporary_home("utf8-round-trip");
        let sink = JsonlEventSink::for_job(&home, "job-1");
        let original = executor_finished(NON_ASCII_REPORT, Some(0.079_407));
        sink.emit(&original).expect("the event should be recorded");

        let raw = fs::read(sink.path()).expect("the log should be readable");
        assert_ne!(
            &raw[..3.min(raw.len())],
            b"\xef\xbb\xbf",
            "the log must stay plain UTF-8 with no byte-order mark"
        );
        let text = std::str::from_utf8(&raw).expect("the log must be valid UTF-8");
        // The exact UTF-8 encodings, asserted as bytes: U+2014 is E2 80 94 and U+2192 is E2 86 92.
        assert!(
            raw.windows(3).any(|window| window == [0xe2, 0x80, 0x94]),
            "an em dash should be stored as its UTF-8 bytes"
        );
        assert!(
            raw.windows(3).any(|window| window == [0xe2, 0x86, 0x92]),
            "an arrow should be stored as its UTF-8 bytes"
        );
        assert!(
            !text.contains("\\u2014") && !text.contains("\\u2192"),
            "non-ASCII must not be escaped away: {text}"
        );

        let decoded: JobEvent =
            serde_json::from_str(text.trim_end()).expect("the line should decode");
        assert_eq!(decoded, original, "the event should round-trip exactly");
        match &decoded.kind {
            JobEventKind::ExecutorFinished { final_response, .. } => {
                assert_eq!(final_response, NON_ASCII_REPORT);
                assert!(final_response.contains('\u{2014}') && final_response.contains('\u{2192}'));
            }
            other => panic!("unexpected event kind: {other:?}"),
        }

        let _ = fs::remove_dir_all(&home);
    }

    /// The same characters survive Lya's own rendering, in every human mode.
    #[test]
    fn rendering_keeps_non_ascii_provider_text() {
        let event = executor_finished(NON_ASCII_REPORT, Some(0.079_407));

        for mode in [HumanRenderMode::Normal, HumanRenderMode::Verbose] {
            let rendered = render_human(&event, mode, false);
            assert!(
                rendered.contains('\u{2014}'),
                "an em dash should survive {mode:?} rendering: {rendered}"
            );
            assert!(
                rendered.contains('\u{2192}'),
                "an arrow should survive {mode:?} rendering: {rendered}"
            );
            assert!(
                !rendered.contains('\u{fffd}'),
                "nothing should be replaced with U+FFFD: {rendered}"
            );
        }
    }

    /// The cost line says whose number it is and what it does not prove.
    ///
    /// `total_cost_usd` is parsed straight out of Claude Code's JSON envelope. Lya neither computes
    /// it nor knows how the provider CLI is authenticated — it removes `ANTHROPIC_API_KEY` from the
    /// child environment, so a subscription session is the normal case — and it must therefore not
    /// present the number as a charge. The field itself is preserved untouched for machine readers.
    #[test]
    fn the_verbose_cost_line_describes_provider_metadata_rather_than_a_bill() {
        let event = executor_finished("done", Some(0.079_407));

        let verbose = render_human(&event, HumanRenderMode::Verbose, false);
        assert!(
            verbose.contains("Claude-reported cost metadata: $0.0794"),
            "the cost must be attributed to the provider: {verbose}"
        );
        assert!(
            verbose.contains("not proof of billing"),
            "the cost must not read as a confirmed charge: {verbose}"
        );
        // Normal mode stays quiet about it; only `--verbose` shows provider metadata.
        let normal = render_human(&event, HumanRenderMode::Normal, false);
        assert!(!normal.contains("cost"), "{normal}");

        // Absent metadata produces no line at all rather than a zero, which would be a claim.
        let without = render_human(
            &executor_finished("done", None),
            HumanRenderMode::Verbose,
            false,
        );
        assert!(!without.contains("cost"), "{without}");
    }

    /// A log whose last line was cut off mid-write stays usable.
    ///
    /// This is what a crash leaves behind. The complete lines before it must still decode, so a
    /// reader — including the daemon's replay — can skip the tail rather than reject the file.
    #[test]
    fn a_truncated_final_line_leaves_the_earlier_events_readable() {
        let home = temporary_home("utf8-truncated");
        let sink = JsonlEventSink::for_job(&home, "job-1");
        sink.emit(&executor_finished(NON_ASCII_REPORT, Some(0.5)))
            .expect("the event should be recorded");

        // Append a line that stops in the middle, exactly as an interrupted write would.
        let mut raw = fs::read(sink.path()).expect("the log should be readable");
        raw.extend_from_slice(br#"{"timestamp_unix_millis":1,"job_id":"job-1","#);
        fs::write(sink.path(), &raw).expect("the log should be writable");

        let mut decoded = 0;
        let mut skipped = 0;
        let text = String::from_utf8(raw).expect("the log must stay valid UTF-8");
        for line in text.lines() {
            match serde_json::from_str::<JobEvent>(line) {
                Ok(event) => {
                    decoded += 1;
                    match &event.kind {
                        JobEventKind::ExecutorFinished { final_response, .. } => {
                            assert!(final_response.contains('\u{2014}'));
                        }
                        other => panic!("unexpected event kind: {other:?}"),
                    }
                }
                Err(_) => skipped += 1,
            }
        }
        assert_eq!(decoded, 1, "the complete event should still decode");
        assert_eq!(skipped, 1, "the truncated tail should be the only casualty");

        let _ = fs::remove_dir_all(&home);
    }

    fn temporary_home(name: &str) -> PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let home = std::env::temp_dir().join(format!(
            "lya-events-test-{}-{name}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&home).expect("the home should be created");
        home
    }

    #[test]
    fn events_round_trip_through_json() {
        let event = event(JobEventKind::SupervisorFinished {
            action: "CLAUDE".to_owned(),
            reason: Some("Needs a fix".to_owned()),
            prompt: Some("Fix it".to_owned()),
            commit_title: None,
            next_prompt: None,
        });
        let json = serde_json::to_string(&event).expect("event should serialize");
        let restored: JobEvent = serde_json::from_str(&json).expect("event should deserialize");
        assert_eq!(restored, event);
    }

    #[test]
    fn jsonl_sink_appends_events_in_order() {
        let directory =
            std::env::temp_dir().join(format!("lya-events-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&directory);
        let sink = JsonlEventSink::for_job(&directory, "job-1");
        sink.emit(&event(JobEventKind::JobStarted {
            task: "first".to_owned(),
        }))
        .expect("first event should persist");
        sink.emit(&event(JobEventKind::JobFinished {
            status: "ACCEPTED".to_owned(),
        }))
        .expect("second event should append");
        let events = fs::read_to_string(sink.path())
            .expect("event log should exist")
            .lines()
            .map(|line| serde_json::from_str::<JobEvent>(line).expect("line should be JSON"))
            .collect::<Vec<_>>();
        assert!(matches!(events[0].kind, JobEventKind::JobStarted { .. }));
        assert!(matches!(events[1].kind, JobEventKind::JobFinished { .. }));
        fs::remove_dir_all(directory).expect("test directory should be removed");
    }

    #[test]
    fn jsonl_sink_persists_control_events_as_valid_json_lines() {
        let directory =
            std::env::temp_dir().join(format!("lya-control-events-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&directory);
        let sink = JsonlEventSink::for_job(&directory, "job-1");
        sink.emit(&event(JobEventKind::UserInstructionQueued {
            instruction: "Keep compatibility.".to_owned(),
        }))
        .expect("control event should persist");
        sink.emit(&event(JobEventKind::Paused {
            reason: "safe boundary reached".to_owned(),
        }))
        .expect("paused event should persist");

        let events = fs::read_to_string(sink.path())
            .expect("event log should exist")
            .lines()
            .map(|line| serde_json::from_str::<JobEvent>(line).expect("line should be JSON"))
            .collect::<Vec<_>>();
        assert!(matches!(
            events[0].kind,
            JobEventKind::UserInstructionQueued { .. }
        ));
        assert!(matches!(events[1].kind, JobEventKind::Paused { .. }));
        fs::remove_dir_all(directory).expect("test directory should be removed");
    }

    #[derive(Clone)]
    struct RecordingSink(Arc<Mutex<Vec<String>>>);
    impl EventSink for RecordingSink {
        fn emit(&self, event: &JobEvent) -> Result<(), super::EventSinkError> {
            self.0
                .lock()
                .expect("record lock")
                .push(format!("{:?}", event.kind));
            Ok(())
        }
    }

    #[test]
    fn composite_sink_delivers_to_each_sink() {
        let first = Arc::new(Mutex::new(Vec::new()));
        let second = Arc::new(Mutex::new(Vec::new()));
        let sink = CompositeEventSink::new(vec![
            Box::new(RecordingSink(first.clone())),
            Box::new(RecordingSink(second.clone())),
        ]);
        sink.emit(&event(JobEventKind::JobFinished {
            status: "STOPPED".to_owned(),
        }))
        .expect("composite sink should emit");
        assert_eq!(first.lock().expect("first lock").len(), 1);
        assert_eq!(second.lock().expect("second lock").len(), 1);
    }

    #[test]
    fn human_renderer_summarizes_repository_without_diff_content() {
        let event = event(JobEventKind::RepositoryCaptured {
            summary: RepositorySummary {
                head: "abcdef123".to_owned(),
                clean: false,
                changed_paths: vec!["README.md".to_owned()],
                untracked_paths: vec!["app.js".to_owned(), "app.test.js".to_owned()],
                binary_untracked_paths: Vec::new(),
                diff_stat: " README.md | 1 +\n".to_owned(),
                diff_truncated: true,
                diff_total_bytes: 200_000,
                untracked_truncated: false,
                untracked_total_bytes: 0,
            },
        });
        let mut output = Vec::new();
        HumanEventSink::new(&mut output, HumanRenderMode::Normal, false)
            .emit(&event)
            .expect("renderer should write");
        let output = String::from_utf8(output).expect("output should be UTF-8");
        assert!(output.contains("tracked: README.md"));
        assert!(output.contains("untracked: app.js, app.test.js"));
        assert!(output.contains("tracked diff:"));
        assert!(!output.contains("diff --git"));
    }

    /// Concurrent jobs share one terminal. Every line has to say which project and job it came
    /// from, or an interleaved stream becomes unreadable.
    #[test]
    fn job_context_prefixes_every_line_of_a_concurrent_stream() {
        let event = event(JobEventKind::JobStarted {
            task: "Fix the flaky test".to_owned(),
        });
        let mut plain = Vec::new();
        HumanEventSink::new(&mut plain, HumanRenderMode::Normal, false)
            .emit(&event)
            .expect("renderer should write");
        let mut prefixed = Vec::new();
        HumanEventSink::new(&mut prefixed, HumanRenderMode::Normal, false)
            .with_job_context(true)
            .emit(&event)
            .expect("renderer should write");

        let plain = String::from_utf8(plain).expect("output should be UTF-8");
        let prefixed = String::from_utf8(prefixed).expect("output should be UTF-8");
        assert!(
            !plain.contains("[demo job-1]"),
            "a single job run keeps its unprefixed output"
        );
        assert!(prefixed.lines().count() > 1);
        for line in prefixed.lines() {
            assert!(
                line.starts_with("[demo job-1] "),
                "every line must name its origin: {line}"
            );
        }
        assert!(prefixed.contains("Fix the flaky test"));
    }

    /// One sink object, shared by every concurrently driven job.
    #[test]
    fn a_shared_sink_can_be_used_through_an_arc() {
        #[derive(Default)]
        struct Counting {
            seen: Mutex<Vec<String>>,
        }

        impl EventSink for Counting {
            fn emit(&self, event: &JobEvent) -> Result<(), super::EventSinkError> {
                self.seen
                    .lock()
                    .expect("sink lock")
                    .push(event.job_id.clone());
                Ok(())
            }
        }

        let shared = Arc::new(Counting::default());
        let composite = CompositeEventSink::new(vec![
            Box::new(Arc::clone(&shared)),
            Box::new(Arc::clone(&shared)),
        ]);

        composite
            .emit(&event(JobEventKind::Resumed))
            .expect("a shared sink should accept events");

        assert_eq!(shared.seen.lock().expect("sink lock").len(), 2);
    }

    #[test]
    fn verbose_renderer_exposes_explicit_prompt() {
        let prompt = format!("{} tail-visible-only-in-verbose", "x".repeat(360));
        let event = event(JobEventKind::SupervisorFinished {
            action: "CLAUDE".to_owned(),
            reason: None,
            prompt: Some(prompt),
            commit_title: None,
            next_prompt: None,
        });
        let mut normal = Vec::new();
        HumanEventSink::new(&mut normal, HumanRenderMode::Normal, false)
            .emit(&event)
            .expect("normal should write");
        let mut verbose = Vec::new();
        HumanEventSink::new(&mut verbose, HumanRenderMode::Verbose, false)
            .emit(&event)
            .expect("verbose should write");
        let normal = String::from_utf8(normal).expect("normal UTF-8");
        let verbose = String::from_utf8(verbose).expect("verbose UTF-8");
        assert!(!normal.contains("tail-visible-only-in-verbose"));
        assert!(verbose.contains("tail-visible-only-in-verbose"));
        assert_eq!(verbose.matches("tail-visible-only-in-verbose").count(), 1);
    }

    #[test]
    fn normal_renderer_does_not_repeat_claude_prompt_at_execution_start() {
        let prompt = "Apply the requested README correction.";
        let started = event(JobEventKind::JobStarted {
            task: "Update README.".to_owned(),
        });
        let decision = event(JobEventKind::SupervisorFinished {
            action: "CLAUDE".to_owned(),
            reason: None,
            prompt: Some(prompt.to_owned()),
            commit_title: None,
            next_prompt: None,
        });
        let execution = event(JobEventKind::ExecutorStarted {
            prompt: prompt.to_owned(),
            session_id: None,
        });
        let output = format!(
            "{}{}",
            render_human(&decision, HumanRenderMode::Normal, false),
            render_human(&execution, HumanRenderMode::Normal, false)
        );
        assert_eq!(output.matches(prompt).count(), 1);
        assert!(output.contains("starting requested execution"));
        assert!(
            !render_human(&started, HumanRenderMode::Normal, false)
                .contains("iteration limit pending")
        );
    }

    #[test]
    fn renderer_marks_truncated_normal_response_and_renders_verbose_response_once() {
        let response = format!("{} response-tail", "x".repeat(360));
        let event = event(JobEventKind::ExecutorFinished {
            session_id: Some("session-1".to_owned()),
            final_response: response.clone(),
            exit_code: Some(0),
            duration_ms: None,
            turns: None,
            total_cost_usd: None,
            usage: None,
        });
        let normal = render_human(&event, HumanRenderMode::Normal, false);
        let verbose = render_human(&event, HumanRenderMode::Verbose, false);
        assert!(normal.contains("... (use --verbose for full response)"));
        assert!(!normal.contains("response-tail"));
        assert!(verbose.contains("response-tail"));
        assert_eq!(verbose.matches("response-tail").count(), 1);
    }

    #[test]
    fn json_renderer_writes_one_valid_object_per_line() {
        let mut output = Vec::new();
        let sink = JsonEventSink::new(&mut output);
        sink.emit(&event(JobEventKind::JobStarted {
            task: "one".to_owned(),
        }))
        .expect("first JSON event should write");
        sink.emit(&event(JobEventKind::JobFinished {
            status: "ACCEPTED".to_owned(),
        }))
        .expect("second JSON event should write");
        let output = String::from_utf8(output).expect("output should be UTF-8");
        let lines = output.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        assert!(
            lines
                .iter()
                .all(|line| serde_json::from_str::<JobEvent>(line).is_ok())
        );
        assert!(!output.contains("\x1b["));
    }
}
