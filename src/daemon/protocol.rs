//! The local control protocol spoken between a Lya client and the Lya daemon.
//!
//! Three rules define it:
//!
//! * **It is versioned from the first line.** Every message in both directions carries
//!   [`PROTOCOL_VERSION`]. A client built against another version is refused with a message that
//!   names both versions instead of being half-understood.
//! * **It is framed.** One JSON object per line, newline-delimited, bounded by
//!   [`MAX_MESSAGE_BYTES`]. Each frame is an envelope — the protocol version — around one tagged
//!   payload:
//!
//!   ```text
//!   {"protocol_version":1,"message":{"request":"ATTACH","job_id":"job-17-4-0","replay":true}}
//!   {"protocol_version":1,"message":{"response":"ATTACHED","job_id":"job-17-4-0"}}
//!   ```
//!
//!   The payload is nested rather than hoisted into the envelope on purpose: a hoisted payload
//!   cannot carry a field the envelope already names, and hoisting makes every future payload field
//!   a potential collision. A frame that is too long, not JSON, or not a message this version knows
//!   is answered with an error and never crashes the daemon.
//! * **It is a boundary, not a window into storage.** Requests and responses are their own data
//!   transfer objects. [`JobSummary`] is derived from persisted job state rather than being that
//!   state serialized, so the on-disk layout can change without changing the wire, and fields that
//!   have no business leaving the machine simply do not exist here.
//!
//! [`JobEvent`](crate::orchestrator::events::JobEvent) is the deliberate exception: it is already
//! Lya's published observation format — the same objects `events.jsonl` and `lya run --json` carry —
//! so an attached client receives exactly the events every other Lya surface shows.
//!
//! Nothing in this module carries a credential. Provider API keys are never part of job state, and
//! Git publication is described by identity, remote and branch only; authentication stays with the
//! machine's own Git configuration.

use serde::{Deserialize, Serialize};

use crate::orchestrator::{
    control::ControlCommand,
    events::JobEvent,
    publisher::GitPublishConfig,
    state::{
        DEFAULT_MAX_ITERATIONS, DEFAULT_MAX_JOBS, JobState, MAX_ACTIVE_USER_INSTRUCTION_BYTES,
        RunConfiguration,
    },
};

/// The protocol version this build speaks. Bump it whenever a message changes meaning.
pub const PROTOCOL_VERSION: u32 = 1;

/// Longest accepted frame, in bytes, in either direction.
///
/// A client cannot make the daemon allocate without bound, and a job event that would exceed it is
/// a bug in the emitter rather than something to stream.
pub const MAX_MESSAGE_BYTES: usize = 1024 * 1024;

/// One request, as it travels over the wire.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientMessage {
    pub protocol_version: u32,
    pub message: DaemonRequest,
}

impl ClientMessage {
    pub fn new(message: DaemonRequest) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            message,
        }
    }
}

/// One response or streamed event, as it travels over the wire.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServerMessage {
    pub protocol_version: u32,
    pub message: DaemonResponse,
}

impl ServerMessage {
    pub fn new(message: DaemonResponse) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            message,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "request", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DaemonRequest {
    /// Liveness and identity. Answered even while the daemon is shutting down.
    Ping,
    /// Everything a client needs to describe the daemon and its work.
    Status,
    /// Hand work to the daemon's scheduler.
    Submit { jobs: Vec<SubmitJob> },
    /// Observe one job's live events. The connection then belongs to that stream.
    Attach {
        job_id: String,
        /// Replay the job's recorded history before the live events.
        #[serde(default)]
        replay: bool,
    },
    /// Send one control command to one job.
    Control {
        job_id: String,
        #[serde(flatten)]
        command: ControlRequest,
    },
    /// Ask for a graceful shutdown.
    Shutdown,
}

/// Every payload carries its data in a named field, and the envelope nests the payload.
///
/// Both choices exist for the same reason: nothing is hoisted into a map that already has names of
/// its own, so no payload field can ever collide with `protocol_version` or with a response tag.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "response", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DaemonResponse {
    Pong {
        daemon: DaemonIdentity,
    },
    Status {
        status: Box<DaemonStatus>,
    },
    /// The result of one [`DaemonRequest::Submit`], in submitted order, one entry per submitted
    /// job.
    ///
    /// Every job is answered, including one the daemon could not accept: a failure partway through
    /// a batch is that job's [`SubmitOutcome::Rejected`] and never an [`DaemonResponse::Error`] for
    /// the batch, because an error for the batch would hide the ids of the jobs already durably
    /// queued before it.
    Submitted {
        jobs: Vec<SubmitOutcome>,
    },
    /// The attach stream is open. Events follow on the same connection.
    Attached {
        job_id: String,
    },
    /// One job event on an open attach stream.
    Event {
        event: Box<JobEvent>,
    },
    /// The attach stream ended for a reason of the daemon's own.
    Detached {
        job_id: String,
        reason: String,
    },
    /// A control command was delivered to the job's existing control channel.
    Controlled {
        job_id: String,
        command: String,
    },
    /// A graceful shutdown started. The connection closes immediately afterwards.
    ShuttingDown {
        active_jobs: usize,
    },
    Error {
        error: DaemonErrorResponse,
    },
}

/// Identity a client can use to recognise the daemon it reached.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonIdentity {
    pub protocol_version: u32,
    pub process_id: u32,
    pub started_unix_seconds: u64,
    /// The endpoint the daemon is listening on, as a client would address it.
    pub endpoint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonStatus {
    #[serde(flatten)]
    pub identity: DaemonIdentity,
    /// False once a graceful shutdown has begun: no further work is accepted.
    pub accepting_work: bool,
    pub max_concurrent: usize,
    /// Whether this daemon resumes interrupted jobs automatically.
    pub resume_interrupted: bool,
    pub connected_clients: usize,
    pub attached_clients: usize,
    /// Jobs the daemon is driving right now.
    pub active: Vec<JobSummary>,
    /// Jobs the daemon accepted and has not started.
    pub queued: Vec<JobSummary>,
    /// Interrupted jobs that could be continued. The daemon does not start these on its own unless
    /// it was configured to; they are listed so nothing is silently parked.
    pub resumable: Vec<JobSummary>,
}

/// A job as a client sees it.
///
/// Deliberately a projection of [`JobState`] rather than [`JobState`] itself: persisted state holds
/// provider transcripts, supervisor decisions and publication bookkeeping that a status listing has
/// no reason to carry across a socket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobSummary {
    pub job_id: String,
    pub project_name: String,
    pub project_path: String,
    pub task: String,
    pub status: String,
    pub phase: String,
    pub iteration: u32,
    pub max_iterations: u32,
    pub created_unix_seconds: u64,
    pub last_updated_unix_seconds: u64,
}

impl From<&JobState> for JobSummary {
    fn from(job: &JobState) -> Self {
        Self {
            job_id: job.job_id.clone(),
            project_name: job.project_name.clone(),
            project_path: job.project_path.display().to_string(),
            task: job.task.clone(),
            status: job.status.label().to_owned(),
            phase: job.phase.label().to_owned(),
            iteration: job.iteration,
            max_iterations: job.run.max_iterations,
            created_unix_seconds: job.created_unix_seconds,
            last_updated_unix_seconds: job.last_updated_unix_seconds,
        }
    }
}

/// One piece of work a client asks the daemon to schedule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitJob {
    /// Path to the Git working tree. The client resolves it before submitting, so a relative path
    /// is never interpreted against the daemon's working directory.
    pub project_path: String,
    #[serde(default)]
    pub project_name: Option<String>,
    pub task: String,
    #[serde(default)]
    pub run: RunOptions,
}

/// What the daemon did with one submitted job.
///
/// `Queued` is a durability statement: the job exists on disk as `QUEUED` before this is sent. The
/// converse does not hold, and the protocol does not pretend otherwise — a response can be lost
/// after the daemon has committed, so **submission is at-least-once**. There is no submission
/// identity on the wire and no de-duplication, so retrying an identical submission after a lost
/// response can create a second job for the same work. `STATUS` is how a client finds out what was
/// really accepted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SubmitOutcome {
    Queued { job_id: String, repository: String },
    Rejected { job_id: String, reason: String },
}

/// The run limits a submitted job is accepted with.
///
/// A transfer object, not [`RunConfiguration`]: the wire states the limits explicitly and the
/// conversion into persisted configuration happens in one place.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunOptions {
    pub max_iterations: u32,
    pub max_jobs: u32,
    pub browser: bool,
    pub publish: bool,
    #[serde(default)]
    pub git: Option<GitOptions>,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            max_iterations: DEFAULT_MAX_ITERATIONS,
            max_jobs: DEFAULT_MAX_JOBS,
            browser: false,
            publish: false,
            git: None,
        }
    }
}

impl RunOptions {
    /// Validate the options and turn them into the configuration a job is persisted with.
    ///
    /// Publication is refused without a complete Git identity here, at the boundary, so a job is
    /// never queued in a shape that can only fail later.
    pub fn to_run_configuration(&self) -> Result<RunConfiguration, String> {
        if self.max_iterations == 0 {
            return Err("max iterations must be greater than zero".to_owned());
        }
        if self.max_jobs == 0 {
            return Err("max jobs must be greater than zero".to_owned());
        }
        let git = match &self.git {
            Some(git) => Some(
                GitPublishConfig::new(
                    git.name.clone(),
                    git.email.clone(),
                    git.remote.clone(),
                    git.branch.clone(),
                )
                .map_err(|error| error.to_string())?,
            ),
            None => None,
        };
        if self.publish && git.is_none() {
            return Err(
                "publication was requested without a Git identity, remote and branch".to_owned(),
            );
        }
        Ok(RunConfiguration {
            max_iterations: self.max_iterations,
            max_jobs: self.max_jobs,
            browser: self.browser,
            publish: self.publish,
            git,
        })
    }
}

/// Git publication description. Identity, remote and branch only; never a credential.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitOptions {
    pub name: String,
    pub email: String,
    pub remote: String,
    pub branch: String,
}

impl From<&GitPublishConfig> for GitOptions {
    fn from(configuration: &GitPublishConfig) -> Self {
        Self {
            name: configuration.identity.name.clone(),
            email: configuration.identity.email.clone(),
            remote: configuration.remote.clone(),
            branch: configuration.branch.clone(),
        }
    }
}

/// The control commands a client may address to a named job.
///
/// One-to-one with [`ControlCommand`] on purpose. The daemon converts and forwards; it never
/// interprets, reorders or re-implements control semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ControlRequest {
    Pause,
    Resume,
    Stop,
    Status,
    Diff,
    Send { instruction: String },
}

impl ControlRequest {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Pause => "PAUSE",
            Self::Resume => "RESUME",
            Self::Stop => "STOP",
            Self::Status => "STATUS",
            Self::Diff => "DIFF",
            Self::Send { .. } => "SEND",
        }
    }

    /// The existing control command this request means.
    ///
    /// An instruction is bounded here by the same limit the orchestrator enforces per job, so a
    /// remote client cannot push a message the job would have to reject anyway.
    pub fn to_command(&self) -> Result<ControlCommand, String> {
        Ok(match self {
            Self::Pause => ControlCommand::Pause,
            Self::Resume => ControlCommand::Resume,
            Self::Stop => ControlCommand::Stop,
            Self::Status => ControlCommand::Status,
            Self::Diff => ControlCommand::Diff,
            Self::Send { instruction } => {
                if instruction.trim().is_empty() {
                    return Err("an instruction cannot be empty".to_owned());
                }
                if instruction.len() > MAX_ACTIVE_USER_INSTRUCTION_BYTES {
                    return Err(format!(
                        "an instruction cannot exceed {MAX_ACTIVE_USER_INSTRUCTION_BYTES} bytes"
                    ));
                }
                ControlCommand::Send(instruction.clone())
            }
        })
    }
}

/// Why a request could not be served.
///
/// The code is for programs, the message for people. Every refusal names one of these, so a client
/// never has to parse prose to tell "no such job" from "not mine to drive".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonErrorResponse {
    pub code: DaemonErrorCode,
    pub message: String,
}

impl DaemonErrorResponse {
    pub fn new(code: DaemonErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DaemonErrorCode {
    /// The frame was not a message this protocol version understands.
    InvalidRequest,
    /// The client speaks another protocol version.
    UnsupportedProtocol,
    /// No such persisted job.
    UnknownJob,
    /// The job exists but has reached a terminal status.
    JobTerminal,
    /// The job exists and is not terminal, but this daemon is not driving it.
    JobNotOwned,
    /// The command cannot apply to the job in its current state.
    InvalidForState,
    /// A graceful shutdown has begun, so no new work is accepted.
    ShuttingDown,
    /// The daemon failed while serving the request. The message says what failed.
    Internal,
}

impl DaemonErrorCode {
    pub fn label(&self) -> &'static str {
        match self {
            Self::InvalidRequest => "INVALID_REQUEST",
            Self::UnsupportedProtocol => "UNSUPPORTED_PROTOCOL",
            Self::UnknownJob => "UNKNOWN_JOB",
            Self::JobTerminal => "JOB_TERMINAL",
            Self::JobNotOwned => "JOB_NOT_OWNED",
            Self::InvalidForState => "INVALID_FOR_STATE",
            Self::ShuttingDown => "SHUTTING_DOWN",
            Self::Internal => "INTERNAL",
        }
    }
}

/// Decode one received frame.
///
/// Both failure shapes matter and are distinguished: a frame this version cannot parse is a client
/// mistake, and a frame from another protocol version is a version mismatch. Neither is fatal.
pub fn decode_request(line: &str) -> Result<DaemonRequest, DaemonErrorResponse> {
    if line.len() > MAX_MESSAGE_BYTES {
        return Err(DaemonErrorResponse::new(
            DaemonErrorCode::InvalidRequest,
            format!("a request may not exceed {MAX_MESSAGE_BYTES} bytes"),
        ));
    }
    let message: ClientMessage = serde_json::from_str(line).map_err(|error| {
        DaemonErrorResponse::new(
            DaemonErrorCode::InvalidRequest,
            format!("could not decode the request: {error}"),
        )
    })?;
    if message.protocol_version != PROTOCOL_VERSION {
        return Err(DaemonErrorResponse::new(
            DaemonErrorCode::UnsupportedProtocol,
            format!(
                "this daemon speaks protocol version {PROTOCOL_VERSION}; the client speaks {}",
                message.protocol_version
            ),
        ));
    }
    Ok(message.message)
}

/// Encode one message as a frame, newline included.
pub fn encode_response(response: DaemonResponse) -> Result<Vec<u8>, String> {
    let mut line = serde_json::to_vec(&ServerMessage::new(response))
        .map_err(|error| format!("could not encode the response: {error}"))?;
    line.push(b'\n');
    Ok(line)
}

/// Encode one request as a frame, newline included.
pub fn encode_request(request: DaemonRequest) -> Result<Vec<u8>, String> {
    let mut line = serde_json::to_vec(&ClientMessage::new(request))
        .map_err(|error| format!("could not encode the request: {error}"))?;
    line.push(b'\n');
    Ok(line)
}

/// Decode one received response frame.
pub fn decode_response(line: &str) -> Result<DaemonResponse, String> {
    let message: ServerMessage = serde_json::from_str(line)
        .map_err(|error| format!("could not decode the daemon response: {error}"))?;
    if message.protocol_version != PROTOCOL_VERSION {
        return Err(format!(
            "the daemon speaks protocol version {}; this client speaks {PROTOCOL_VERSION}",
            message.protocol_version
        ));
    }
    Ok(message.message)
}

#[cfg(test)]
mod tests {
    use super::{
        ClientMessage, ControlRequest, DaemonErrorCode, DaemonErrorResponse, DaemonIdentity,
        DaemonRequest, DaemonResponse, GitOptions, JobSummary, MAX_MESSAGE_BYTES, PROTOCOL_VERSION,
        RunOptions, SubmitJob, decode_request, decode_response, encode_request, encode_response,
    };
    use crate::orchestrator::{
        control::ControlCommand,
        events::JobEvent,
        state::{JobState, MAX_ACTIVE_USER_INSTRUCTION_BYTES},
    };

    #[test]
    fn a_request_round_trips_through_one_framed_line() {
        let request = DaemonRequest::Submit {
            jobs: vec![SubmitJob {
                project_path: "/work/project".to_owned(),
                project_name: Some("project".to_owned()),
                task: "Fix the flaky test".to_owned(),
                run: RunOptions {
                    max_iterations: 4,
                    max_jobs: 2,
                    browser: true,
                    publish: false,
                    git: None,
                },
            }],
        };

        let frame = encode_request(request.clone()).expect("a request should encode");

        assert!(frame.ends_with(b"\n"), "frames are newline-delimited");
        assert_eq!(
            frame.iter().filter(|byte| **byte == b'\n').count(),
            1,
            "one frame is exactly one line"
        );
        let line = String::from_utf8(frame).expect("a frame is UTF-8");
        assert_eq!(
            decode_request(line.trim_end()).expect("the frame should decode"),
            request
        );
    }

    #[test]
    fn every_message_carries_the_protocol_version() {
        let frame = encode_request(DaemonRequest::Ping).expect("a request should encode");
        let value: serde_json::Value =
            serde_json::from_slice(&frame).expect("a frame is one JSON object");

        assert_eq!(value["protocol_version"], PROTOCOL_VERSION);
        assert_eq!(value["message"]["request"], "PING");

        let frame = encode_response(DaemonResponse::Attached {
            job_id: "job-1".to_owned(),
        })
        .expect("a response should encode");
        let value: serde_json::Value =
            serde_json::from_slice(&frame).expect("a frame is one JSON object");
        assert_eq!(value["protocol_version"], PROTOCOL_VERSION);
        assert_eq!(value["message"]["response"], "ATTACHED");
    }

    #[test]
    fn a_client_from_another_protocol_version_is_refused_by_code() {
        let line = serde_json::to_string(&ClientMessage {
            protocol_version: PROTOCOL_VERSION + 1,
            message: DaemonRequest::Ping,
        })
        .expect("the message should encode");

        let error = decode_request(&line).expect_err("another version should be refused");

        assert_eq!(error.code, DaemonErrorCode::UnsupportedProtocol);
        assert!(error.message.contains(&PROTOCOL_VERSION.to_string()));
    }

    #[test]
    fn malformed_and_oversized_frames_are_refused_as_invalid_requests() {
        for line in [
            "",
            "{",
            "not json at all",
            "{\"protocol_version\":1}",
            "{\"protocol_version\":1,\"message\":{\"request\":\"NOPE\"}}",
        ] {
            let error = decode_request(line).expect_err("a malformed frame should be refused");
            assert_eq!(error.code, DaemonErrorCode::InvalidRequest, "{line}");
        }

        let oversized = "x".repeat(MAX_MESSAGE_BYTES + 1);
        let error = decode_request(&oversized).expect_err("an oversized frame should be refused");

        assert_eq!(error.code, DaemonErrorCode::InvalidRequest);
        assert!(error.message.contains("exceed"));
    }

    #[test]
    fn control_requests_map_one_to_one_onto_existing_control_commands() {
        assert_eq!(
            ControlRequest::Pause.to_command(),
            Ok(ControlCommand::Pause)
        );
        assert_eq!(
            ControlRequest::Resume.to_command(),
            Ok(ControlCommand::Resume)
        );
        assert_eq!(ControlRequest::Stop.to_command(), Ok(ControlCommand::Stop));
        assert_eq!(
            ControlRequest::Status.to_command(),
            Ok(ControlCommand::Status)
        );
        assert_eq!(ControlRequest::Diff.to_command(), Ok(ControlCommand::Diff));
        assert_eq!(
            ControlRequest::Send {
                instruction: "Also update the changelog".to_owned()
            }
            .to_command(),
            Ok(ControlCommand::Send("Also update the changelog".to_owned()))
        );
    }

    /// The per-job instruction bound is the orchestrator's. The protocol refuses at the boundary
    /// rather than inventing a second limit.
    #[test]
    fn an_instruction_is_bounded_by_the_existing_per_job_limit() {
        let error = ControlRequest::Send {
            instruction: "x".repeat(MAX_ACTIVE_USER_INSTRUCTION_BYTES + 1),
        }
        .to_command()
        .expect_err("an oversized instruction should be refused");

        assert!(error.contains(&MAX_ACTIVE_USER_INSTRUCTION_BYTES.to_string()));
        assert!(
            ControlRequest::Send {
                instruction: "   ".to_owned()
            }
            .to_command()
            .is_err(),
            "an empty instruction is refused too"
        );
    }

    #[test]
    fn run_options_validate_limits_and_publication_configuration() {
        let complete = RunOptions {
            max_iterations: 3,
            max_jobs: 1,
            browser: false,
            publish: true,
            git: Some(GitOptions {
                name: "Lya".to_owned(),
                email: "lya@example.com".to_owned(),
                remote: "origin".to_owned(),
                branch: "main".to_owned(),
            }),
        };
        let configuration = complete
            .to_run_configuration()
            .expect("complete options should convert");
        assert!(configuration.publish);
        assert_eq!(
            configuration.git.as_ref().map(|git| git.branch.as_str()),
            Some("main")
        );

        let publish_without_identity = RunOptions {
            publish: true,
            ..RunOptions::default()
        };
        assert!(
            publish_without_identity.to_run_configuration().is_err(),
            "publication without a Git identity must be refused at the boundary"
        );
        assert!(
            RunOptions {
                max_iterations: 0,
                ..RunOptions::default()
            }
            .to_run_configuration()
            .is_err()
        );
        assert!(
            RunOptions {
                max_jobs: 0,
                ..RunOptions::default()
            }
            .to_run_configuration()
            .is_err()
        );
    }

    /// The status listing is a projection: persisted transcripts and publication bookkeeping must
    /// not travel over the socket just because they exist on disk.
    #[test]
    fn a_job_summary_exposes_identity_and_progress_only() {
        let mut job = JobState::new(
            "job-42",
            "project",
            std::path::PathBuf::from("/work/project"),
            "Fix the flaky test",
        );
        job.claude_session_id = Some("session-secret".to_owned());
        job.last_executor_report = Some("a long provider transcript".to_owned());

        let rendered = serde_json::to_string(&JobSummary::from(&job)).expect("summary encodes");

        assert!(rendered.contains("job-42"));
        assert!(rendered.contains("RUNNING"));
        assert!(
            !rendered.contains("session-secret"),
            "a session identifier is not part of the wire: {rendered}"
        );
        assert!(
            !rendered.contains("provider transcript"),
            "a provider transcript is not part of the wire: {rendered}"
        );
    }

    /// A streamed job event carries a 128-bit millisecond timestamp. It has to survive the envelope
    /// unchanged, in both directions.
    #[test]
    fn a_streamed_job_event_round_trips_through_the_envelope() {
        let event = JobEvent::new(
            "job-1",
            &crate::orchestrator::supervisor::Project {
                name: "project".to_owned(),
                path: std::path::PathBuf::from("/work/project"),
            },
            Some(2),
            crate::orchestrator::events::JobEventKind::ControlMessage {
                message: "paused at a safe boundary".to_owned(),
            },
        );

        let frame = encode_response(DaemonResponse::Event {
            event: Box::new(event.clone()),
        })
        .expect("an event should encode");
        let line = String::from_utf8(frame).expect("a frame is UTF-8");

        match decode_response(line.trim_end()).expect("the frame should decode") {
            DaemonResponse::Event { event: decoded } => assert_eq!(*decoded, event),
            other => panic!("expected an event, got {other:?}"),
        }
    }

    /// The envelope and a payload may both want to name the protocol version. Nesting the payload is
    /// what keeps such a frame decodable, so it is checked rather than assumed.
    #[test]
    fn a_payload_that_names_the_protocol_version_still_round_trips() {
        let identity = DaemonIdentity {
            protocol_version: PROTOCOL_VERSION,
            process_id: 42,
            started_unix_seconds: 7,
            endpoint: "local".to_owned(),
        };

        let frame = encode_response(DaemonResponse::Pong {
            daemon: identity.clone(),
        })
        .expect("a pong should encode");
        let line = String::from_utf8(frame).expect("a frame is UTF-8");

        assert_eq!(
            decode_response(line.trim_end()).expect("the frame should decode"),
            DaemonResponse::Pong { daemon: identity }
        );
    }

    #[test]
    fn an_error_response_round_trips_with_its_code() {
        let frame = encode_response(DaemonResponse::Error {
            error: DaemonErrorResponse::new(DaemonErrorCode::UnknownJob, "no such job"),
        })
        .expect("an error should encode");
        let line = String::from_utf8(frame).expect("a frame is UTF-8");

        let response = decode_response(line.trim_end()).expect("the frame should decode");

        assert_eq!(
            response,
            DaemonResponse::Error {
                error: DaemonErrorResponse::new(DaemonErrorCode::UnknownJob, "no such job")
            }
        );
    }
}
