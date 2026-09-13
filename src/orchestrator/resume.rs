use std::{error::Error, fmt};

use super::{
    state::{JobState, JobStatus, PendingOperation, StateError, StateStore},
    supervisor::SupervisorDecision,
};

/// Where a resumed job continues. Derived from authoritative persisted state only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeContinuation {
    /// Nothing was owed: start the next supervisor iteration normally.
    Fresh,
    /// A supervisor review was started and never completed; its iteration is already counted.
    Supervisor,
    /// A Claude decision was recorded but the executor run never completed.
    Executor,
    /// An ACCEPT was recorded and publication never completed.
    Publication,
}

impl ResumeContinuation {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Fresh => "NEW_SUPERVISOR_ITERATION",
            Self::Supervisor => "SUPERVISOR_REVIEW",
            Self::Executor => "EXECUTOR_RUN",
            Self::Publication => "PUBLICATION",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumePlan {
    pub continuation: ResumeContinuation,
}

impl ResumePlan {
    /// Validate a persisted job structurally and semantically and decide where it continues.
    /// This never touches the repository; the caller proves repository reality separately.
    pub fn for_job(job: &JobState) -> Result<Self, ResumeRejection> {
        if job.job_id.trim().is_empty() || job.task.trim().is_empty() {
            return Err(ResumeRejection::Invalid(
                "persisted job is missing its identifier or task".to_owned(),
            ));
        }
        if !job.status.is_resumable() {
            return Err(ResumeRejection::NotResumable {
                job_id: job.job_id.clone(),
                status: job.status.label().to_owned(),
            });
        }
        if job.run.max_iterations == 0 || job.run.max_jobs == 0 {
            return Err(ResumeRejection::Invalid(
                "persisted run configuration has a zero iteration or job limit".to_owned(),
            ));
        }

        let continuation = match job.pending_operation {
            None => ResumeContinuation::Fresh,
            Some(PendingOperation::SupervisorReview) => {
                if job.iteration == 0 {
                    return Err(ResumeRejection::Invalid(
                        "a supervisor review is pending but no iteration was counted".to_owned(),
                    ));
                }
                ResumeContinuation::Supervisor
            }
            Some(PendingOperation::ExecutorRun) => {
                match &job.last_supervisor_decision {
                    Some(SupervisorDecision::Claude { prompt, .. })
                        if !prompt.trim().is_empty() => {}
                    _ => {
                        return Err(ResumeRejection::Invalid(
                            "an executor run is pending but no Claude prompt was recorded"
                                .to_owned(),
                        ));
                    }
                }
                if job.last_executor_report.is_some()
                    && job
                        .claude_session_id
                        .as_ref()
                        .is_none_or(|session| session.trim().is_empty())
                {
                    return Err(ResumeRejection::Invalid(
                        "a correction is pending but no resumable Claude session was recorded"
                            .to_owned(),
                    ));
                }
                ResumeContinuation::Executor
            }
            Some(PendingOperation::Publication) => {
                if !job.run.publish {
                    return Err(ResumeRejection::Invalid(
                        "publication is pending but publication was not enabled for this job"
                            .to_owned(),
                    ));
                }
                if job.run.git.is_none() {
                    return Err(ResumeRejection::MissingPublicationConfiguration(
                        job.job_id.clone(),
                    ));
                }
                if job.accepted_repository_state.is_none() {
                    return Err(ResumeRejection::Invalid(
                        "publication is pending but no accepted repository snapshot was persisted"
                            .to_owned(),
                    ));
                }
                if !matches!(
                    job.last_supervisor_decision,
                    Some(SupervisorDecision::Accept { .. })
                ) {
                    return Err(ResumeRejection::Invalid(
                        "publication is pending but no ACCEPT decision was recorded".to_owned(),
                    ));
                }
                ResumeContinuation::Publication
            }
        };

        // A quota wait must name the operation it is waiting to retry, otherwise the retry could
        // repeat a model action that already completed.
        if let Some(quota) = &job.quota_wait
            && Some(quota.operation) != job.pending_operation
        {
            return Err(ResumeRejection::Invalid(
                "the recorded quota wait does not match the pending operation".to_owned(),
            ));
        }
        match job.status {
            JobStatus::WaitingClaudeQuota if continuation != ResumeContinuation::Executor => {
                Err(ResumeRejection::Invalid(
                    "a Claude quota wait must continue with an executor run".to_owned(),
                ))
            }
            JobStatus::WaitingOpenAiQuota if continuation != ResumeContinuation::Supervisor => {
                Err(ResumeRejection::Invalid(
                    "an OpenAI quota wait must continue with a supervisor review".to_owned(),
                ))
            }
            JobStatus::Publishing if continuation != ResumeContinuation::Publication => {
                Err(ResumeRejection::Invalid(
                    "an interrupted publication has no pending publication operation".to_owned(),
                ))
            }
            _ => Ok(Self { continuation }),
        }
    }
}

/// Every persisted job a later process may continue, newest first.
pub fn resumable_jobs(store: &StateStore) -> Result<Vec<JobState>, StateError> {
    let mut jobs = store
        .load_all()?
        .into_iter()
        .filter(|job| job.status.is_resumable())
        .collect::<Vec<_>>();
    jobs.sort_by(|left, right| {
        right
            .last_updated_unix_seconds
            .cmp(&left.last_updated_unix_seconds)
            .then_with(|| left.job_id.cmp(&right.job_id))
    });
    Ok(jobs)
}

/// Pick the job to resume. Without an explicit ID this only succeeds when exactly one resumable
/// job exists, so a run never continues the wrong job by accident.
pub fn select_job(
    jobs: Vec<JobState>,
    requested: Option<&str>,
) -> Result<JobState, ResumeRejection> {
    match requested {
        Some(job_id) => jobs
            .into_iter()
            .find(|job| job.job_id == job_id)
            .ok_or_else(|| ResumeRejection::UnknownJob(job_id.to_owned())),
        None => {
            let mut jobs = jobs.into_iter();
            let Some(first) = jobs.next() else {
                return Err(ResumeRejection::NoResumableJobs);
            };
            match jobs.next() {
                None => Ok(first),
                Some(second) => {
                    let mut candidates = vec![describe(&first), describe(&second)];
                    candidates.extend(jobs.map(|job| describe(&job)));
                    Err(ResumeRejection::Ambiguous(candidates))
                }
            }
        }
    }
}

pub fn describe(job: &JobState) -> String {
    format!(
        "{}  {}  iteration {}  {}  {}",
        job.job_id,
        job.status.label(),
        job.iteration,
        job.project_name,
        job.task.lines().next().unwrap_or("").trim()
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResumeRejection {
    NoResumableJobs,
    UnknownJob(String),
    NotResumable { job_id: String, status: String },
    MissingPublicationConfiguration(String),
    Ambiguous(Vec<String>),
    Invalid(String),
}

impl fmt::Display for ResumeRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoResumableJobs => formatter.write_str(
                "no resumable job was found; only RUNNING, PAUSED, PUBLISHING and quota-waiting jobs can be resumed",
            ),
            Self::UnknownJob(job_id) => write!(formatter, "unknown job: {job_id}"),
            Self::NotResumable { job_id, status } => write!(
                formatter,
                "job {job_id} is {status} and is terminal for automatic recovery"
            ),
            Self::MissingPublicationConfiguration(job_id) => write!(
                formatter,
                "job {job_id} needs to finish publishing, but no Git identity, remote and branch were persisted for it"
            ),
            Self::Ambiguous(candidates) => write!(
                formatter,
                "several jobs can be resumed; choose one with --job:\n  {}",
                candidates.join("\n  ")
            ),
            Self::Invalid(reason) => write!(formatter, "persisted job state is inconsistent: {reason}"),
        }
    }
}

impl Error for ResumeRejection {}

#[cfg(test)]
mod tests {
    use super::{ResumeContinuation, ResumePlan, ResumeRejection, resumable_jobs, select_job};
    use crate::orchestrator::{
        publisher::{GitPublishConfig, PublishResult, PushStatus},
        repository::RepositoryState,
        state::{
            JobState, JobStatus, PendingOperation, QuotaSource, QuotaWait, StateStore,
            current_unix_seconds,
        },
        supervisor::SupervisorDecision,
    };
    use std::{
        fs,
        sync::atomic::{AtomicUsize, Ordering},
    };

    static NEXT_TEMP_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    fn job(job_id: &str, status: JobStatus) -> JobState {
        let mut job = JobState::new(
            job_id,
            "Sandbox",
            std::env::temp_dir(),
            "improve the sandbox",
        );
        job.status = status;
        job
    }

    fn snapshot() -> RepositoryState {
        RepositoryState {
            head: "abc123".to_owned(),
            status_short: String::new(),
            diff_stat: String::new(),
            changed_files: Vec::new(),
            diff: String::new(),
            diff_truncated: false,
            diff_total_bytes: 0,
            untracked_files: Vec::new(),
            untracked_total_bytes: 0,
            untracked_truncated: false,
        }
    }

    #[test]
    fn a_job_without_a_pending_operation_starts_a_new_iteration() {
        let plan = ResumePlan::for_job(&job("fresh", JobStatus::Paused)).expect("plan");

        assert_eq!(plan.continuation, ResumeContinuation::Fresh);
    }

    #[test]
    fn a_pending_supervisor_review_keeps_its_counted_iteration() {
        let mut state = job("supervisor", JobStatus::Paused);
        state.iteration = 3;
        state.pending_operation = Some(PendingOperation::SupervisorReview);

        let plan = ResumePlan::for_job(&state).expect("plan");

        assert_eq!(plan.continuation, ResumeContinuation::Supervisor);
    }

    #[test]
    fn a_pending_executor_run_requires_a_recorded_claude_prompt() {
        let mut state = job("executor", JobStatus::Paused);
        state.iteration = 1;
        state.pending_operation = Some(PendingOperation::ExecutorRun);

        let error = ResumePlan::for_job(&state).expect_err("missing prompt should be refused");

        assert!(matches!(error, ResumeRejection::Invalid(_)));

        state.last_supervisor_decision = Some(SupervisorDecision::Claude {
            prompt: "Implement.".to_owned(),
            reason: None,
        });
        assert_eq!(
            ResumePlan::for_job(&state).expect("plan").continuation,
            ResumeContinuation::Executor
        );
    }

    #[test]
    fn a_correction_without_a_session_is_refused() {
        let mut state = job("correction", JobStatus::Paused);
        state.iteration = 2;
        state.pending_operation = Some(PendingOperation::ExecutorRun);
        state.last_supervisor_decision = Some(SupervisorDecision::Claude {
            prompt: "Correct it.".to_owned(),
            reason: None,
        });
        state.last_executor_report = Some("first pass".to_owned());

        let error = ResumePlan::for_job(&state).expect_err("missing session should be refused");

        assert!(matches!(error, ResumeRejection::Invalid(_)));
    }

    #[test]
    fn pending_publication_requires_persisted_git_configuration() {
        let mut state = job("publish", JobStatus::Publishing);
        state.iteration = 1;
        state.pending_operation = Some(PendingOperation::Publication);
        state.run.publish = true;
        state.accepted_repository_state = Some(snapshot());
        state.last_supervisor_decision = Some(SupervisorDecision::Accept {
            commit_title: "Ship it".to_owned(),
            next_prompt: None,
            reason: None,
        });

        let error = ResumePlan::for_job(&state).expect_err("missing configuration should refuse");
        assert!(matches!(
            error,
            ResumeRejection::MissingPublicationConfiguration(_)
        ));

        state.run.git = Some(
            GitPublishConfig::new("Bot", "bot@example.com", "origin", "main")
                .expect("configuration"),
        );
        assert_eq!(
            ResumePlan::for_job(&state).expect("plan").continuation,
            ResumeContinuation::Publication
        );
    }

    #[test]
    fn a_commit_recorded_before_push_still_resumes_publication() {
        let mut state = job("committed", JobStatus::Publishing);
        state.iteration = 1;
        state.pending_operation = Some(PendingOperation::Publication);
        state.run.publish = true;
        state.run.git = Some(
            GitPublishConfig::new("Bot", "bot@example.com", "origin", "main")
                .expect("configuration"),
        );
        state.accepted_repository_state = Some(snapshot());
        state.last_supervisor_decision = Some(SupervisorDecision::Accept {
            commit_title: "Ship it".to_owned(),
            next_prompt: None,
            reason: None,
        });
        state.publish_result = Some(PublishResult {
            commit_sha: "def456".to_owned(),
            commit_title: "Ship it".to_owned(),
            remote: "origin".to_owned(),
            branch: "main".to_owned(),
            push_status: PushStatus::Pending,
        });

        assert_eq!(
            ResumePlan::for_job(&state).expect("plan").continuation,
            ResumeContinuation::Publication
        );
    }

    #[test]
    fn terminal_states_are_never_resumable() {
        for status in [
            JobStatus::Published,
            JobStatus::Stopped,
            JobStatus::Failed,
            JobStatus::Accepted,
            JobStatus::WaitingHuman,
        ] {
            let error = ResumePlan::for_job(&job("terminal", status.clone()))
                .expect_err("terminal state should be refused");
            let ResumeRejection::NotResumable {
                status: reported, ..
            } = error
            else {
                panic!("a terminal status should be refused as not resumable");
            };
            assert_eq!(reported, status.label());
        }
    }

    #[test]
    fn quota_wait_must_match_the_pending_operation() {
        let mut state = job("mismatch", JobStatus::WaitingClaudeQuota);
        state.iteration = 1;
        state.pending_operation = Some(PendingOperation::SupervisorReview);
        state.quota_wait = Some(QuotaWait {
            provider: "Claude".to_owned(),
            operation: PendingOperation::ExecutorRun,
            source: QuotaSource::ProviderMessageHeuristic,
            reason: "rate limit".to_owned(),
            detected_unix_seconds: current_unix_seconds(),
        });

        let error = ResumePlan::for_job(&state).expect_err("mismatch should be refused");

        assert!(matches!(error, ResumeRejection::Invalid(_)));
    }

    #[test]
    fn a_claude_quota_wait_must_continue_with_the_executor() {
        let mut state = job("claude-quota", JobStatus::WaitingClaudeQuota);
        state.iteration = 1;
        state.pending_operation = Some(PendingOperation::SupervisorReview);

        let error = ResumePlan::for_job(&state).expect_err("mismatch should be refused");

        assert!(matches!(error, ResumeRejection::Invalid(_)));
    }

    #[test]
    fn selection_requires_an_explicit_job_when_several_can_resume() {
        let jobs = vec![job("one", JobStatus::Paused), job("two", JobStatus::Paused)];

        let error = select_job(jobs.clone(), None).expect_err("ambiguous selection should fail");
        assert!(matches!(error, ResumeRejection::Ambiguous(ref names) if names.len() == 2));

        assert_eq!(
            select_job(jobs.clone(), Some("two"))
                .expect("explicit job")
                .job_id,
            "two"
        );
        assert!(matches!(
            select_job(jobs, Some("three")).expect_err("unknown job"),
            ResumeRejection::UnknownJob(_)
        ));
        assert!(matches!(
            select_job(Vec::new(), None).expect_err("no jobs"),
            ResumeRejection::NoResumableJobs
        ));
    }

    #[test]
    fn only_resumable_jobs_are_listed() {
        let directory = std::env::temp_dir().join(format!(
            "lya-resume-test-{}-{}",
            std::process::id(),
            NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(&directory).expect("home should be created");
        let store = StateStore::at(&directory);
        store
            .save_job(&job("kept", JobStatus::Paused))
            .expect("save");
        store
            .save_job(&job("gone", JobStatus::Published))
            .expect("save");

        let resumable = resumable_jobs(&store).expect("listing should succeed");

        assert_eq!(resumable.len(), 1);
        assert_eq!(resumable[0].job_id, "kept");
        fs::remove_dir_all(directory).expect("home should be removed");
    }
}
