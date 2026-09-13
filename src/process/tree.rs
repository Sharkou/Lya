//! OS-backed termination of a spawned process and everything it started.
//!
//! Killing only the direct child leaves whatever that child spawned running. Codex and Claude both
//! start their own helpers, so a cancelled or timed-out provider must be terminated as a tree
//! rather than as a single process. The claim is made by the operating system, never by walking a
//! process table Lya cannot trust:
//!
//! * Windows assigns the child to a Job Object created with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`;
//! * Unix puts the child in its own process group and signals the whole group.
//!
//! Both claims are taken for every spawned process, so a cancellation never has to decide whether
//! the tree is worth claiming. Platform code stays in this module; [`super::ProcessSpec::run`] only
//! configures, attaches and terminates.

#[cfg(windows)]
pub(super) use self::windows_tree::ProcessTree;

#[cfg(unix)]
pub(super) use self::unix_tree::ProcessTree;

#[cfg(windows)]
mod windows_tree {
    use std::{ffi::c_void, ptr};

    use tokio::process::{Child, Command};
    use windows_sys::Win32::{
        Foundation::{CloseHandle, HANDLE},
        System::JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
            SetInformationJobObject, TerminateJobObject,
        },
    };

    /// An owned Job Object handle holding one spawned process and its descendants.
    struct Job(HANDLE);

    // A Job Object handle belongs to the process, not to the thread that created it, and every
    // call Lya makes on it is documented as thread-safe.
    unsafe impl Send for Job {}
    unsafe impl Sync for Job {}

    impl Drop for Job {
        fn drop(&mut self) {
            // The job is created with `KILL_ON_JOB_CLOSE`, so closing the last handle terminates
            // anything still inside it. An abandoned run, or Lya itself being force-terminated by
            // a second Ctrl+C, therefore cannot leak a provider tree.
            unsafe { CloseHandle(self.0) };
        }
    }

    pub struct ProcessTree {
        job: Option<Job>,
    }

    impl ProcessTree {
        /// Nothing has to be configured before spawning on Windows: the job claim is taken on the
        /// spawned process itself.
        pub fn configure(_command: &mut Command) {}

        /// Claim the spawned process and its future descendants.
        ///
        /// Call this immediately after spawning. Anything the child manages to start before the
        /// assignment completes is outside the job; the window is a few microseconds and Windows
        /// offers no way to assign a job to a process `tokio` has already resumed.
        pub fn attach(child: &Child) -> Self {
            let Some(handle) = child.raw_handle() else {
                return Self { job: None };
            };
            Self {
                job: create_job().and_then(|job| assign(job, handle as HANDLE)),
            }
        }

        /// Terminate the whole tree. Terminating an already exited job is a no-op.
        pub fn terminate(&self) {
            if let Some(job) = &self.job {
                unsafe { TerminateJobObject(job.0, 1) };
            }
        }
    }

    fn create_job() -> Option<Job> {
        let handle = unsafe { CreateJobObjectW(ptr::null(), ptr::null()) };
        if handle.is_null() {
            return None;
        }
        let job = Job(handle);
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        let configured = unsafe {
            SetInformationJobObject(
                job.0,
                JobObjectExtendedLimitInformation,
                std::ptr::from_ref(&limits) as *const c_void,
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        (configured != 0).then_some(job)
    }

    /// A job that cannot hold the child is dropped rather than kept: an empty job would make
    /// `terminate` silently do nothing while pretending to own the tree.
    fn assign(job: Job, process: HANDLE) -> Option<Job> {
        let assigned = unsafe { AssignProcessToJobObject(job.0, process) };
        (assigned != 0).then_some(job)
    }
}

#[cfg(unix)]
mod unix_tree {
    use tokio::process::{Child, Command};

    pub struct ProcessTree {
        group: Option<i32>,
    }

    impl ProcessTree {
        /// Put the child in a new process group of its own, so everything it starts can be
        /// signalled as a unit.
        ///
        /// A side effect is that the provider no longer shares Lya's foreground process group, so
        /// a terminal `Ctrl+C` reaches Lya alone. That is deliberate: the first interrupt stays
        /// graceful and Lya decides when the provider is cancelled.
        pub fn configure(command: &mut Command) {
            command.process_group(0);
        }

        pub fn attach(child: &Child) -> Self {
            Self {
                group: child.id().map(|pid| pid as i32),
            }
        }

        /// Signal the whole group.
        ///
        /// This must run before the direct child is reaped: while the unreaped group leader still
        /// exists the kernel cannot reuse its group ID, so the signal can never reach an unrelated
        /// process group.
        pub fn terminate(&self) {
            if let Some(group) = self.group {
                unsafe { libc::killpg(group, libc::SIGKILL) };
            }
        }
    }
}
