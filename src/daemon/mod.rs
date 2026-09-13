//! Persistent local daemon mode.
//!
//! Autonomous work no longer needs a terminal to stay attached to it. A daemon owns the scheduler,
//! the running jobs and the local control endpoint; `lya` commands become clients of it.
//!
//! ```text
//! lya daemon start ──► lya daemon ─── owns the scheduler
//!                          │          owns the active and queued jobs
//!                          │          owns the live control endpoint
//!                          │          survives the terminal that started it
//!                          │
//! lya submit   ────────────┤  hand work over; get the job ids back
//! lya attach   ────────────┤  watch one job's events live; Ctrl+C detaches only the viewer
//! lya control  ────────────┤  pause / resume / stop / status / diff / send, per job
//! lya daemon status ───────┤  what is running, queued and parked
//! lya daemon stop  ────────┘  stop accepting work, shut active jobs down safely, exit
//! ```
//!
//! The boundaries are deliberate:
//!
//! * [`transport`] knows about pipes and sockets and nothing else;
//! * [`protocol`] is the versioned wire contract and renders nothing;
//! * [`lock`] is the operating-system claim that makes one daemon per `LYA_HOME` true;
//! * [`security`] is the Windows access control list on the endpoint and the identity check on both
//!   ends of it;
//! * [`attach`] is the live event fan-out;
//! * [`server`] coordinates the existing scheduler and orchestrator, and owns no job semantics;
//! * [`runner`] is the production job driver;
//! * [`client`] is what the CLI uses, and it formats nothing.
//!
//! Local only, by construction: no TCP socket, no port, no remote address. Access is the operating
//! system's to enforce, and it is asked for explicitly rather than taken as it comes:
//!
//! * **Windows** — the named pipe is created with an access control list naming this user and
//!   `SYSTEM` and nobody else. The default descriptor Windows would otherwise apply grants read
//!   access to `Everyone` and to `ANONYMOUS LOGON`, which is why it is never used. Because the pipe
//!   name is a deterministic fingerprint of `LYA_HOME` and any local account may create a name in
//!   the pipe namespace first, both ends also verify that the other runs as the same user before
//!   anything is trusted. See [`security`].
//! * **Unix** — a `0600` socket inside a `0700` home. File-system permissions are the access
//!   control; there is no equivalent name to squat, because the socket is a path inside a directory
//!   only the owner can traverse.

pub mod attach;
pub mod client;
pub mod events;
pub mod lock;
pub mod protocol;
pub mod runner;
#[cfg(windows)]
pub mod security;
pub mod server;
pub mod transport;
