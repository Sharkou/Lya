//! The client half of the local control protocol.
//!
//! Every `lya` command that talks to a daemon goes through this one type, so discovery, framing,
//! version checking and error classification exist in exactly one place. It renders nothing: a
//! client returns protocol values and the CLI decides how they look, which is what keeps terminal
//! formatting out of the protocol and the protocol out of the terminal.

use std::{error::Error, fmt, time::Duration};

use crate::orchestrator::home::LyaHome;

use super::{
    lock::{DaemonLock, DaemonLockError},
    protocol::{
        DaemonErrorResponse, DaemonIdentity, DaemonRequest, DaemonResponse, decode_response,
        encode_request,
    },
    transport::{DaemonConnection, DaemonEndpoint, TransportError, connect},
};

/// How long a client waits for a daemon to appear, in total.
///
/// Used by `lya daemon start`, which has to report the state the shell will actually find rather
/// than the state it asked for.
const READINESS_ATTEMPTS: u32 = 100;
const READINESS_INTERVAL: Duration = Duration::from_millis(50);

/// How long a client waits for a stopping daemon to release its claim, in total.
///
/// Far longer than readiness, because the two wait for different things. A daemon becomes reachable
/// in milliseconds; a daemon *stops* only once every job it is driving has shut down at a safe
/// boundary, and that is bounded by the work, not by the daemon. An hour is generous enough that
/// the wait ends because the daemon ended, and finite so the command always returns.
const RELEASE_ATTEMPTS: u32 = 14_400;
const RELEASE_INTERVAL: Duration = Duration::from_millis(250);

pub struct DaemonClient {
    connection: DaemonConnection,
}

impl DaemonClient {
    /// Connect to the daemon that owns one Lya home.
    pub async fn connect(home: &LyaHome) -> Result<Self, ClientError> {
        let endpoint = DaemonEndpoint::for_home(home);
        Ok(Self {
            connection: connect(&endpoint).await.map_err(ClientError::Transport)?,
        })
    }

    /// Send one request and read the one response that answers it.
    ///
    /// A refusal from the daemon is an [`ClientError::Daemon`], not a transport failure: the caller
    /// gets the daemon's own code and message instead of having to read prose.
    pub async fn request(&mut self, request: DaemonRequest) -> Result<DaemonResponse, ClientError> {
        let frame = encode_request(request).map_err(ClientError::Protocol)?;
        self.connection
            .write_frame(&frame)
            .await
            .map_err(ClientError::Transport)?;
        match self.receive().await? {
            DaemonResponse::Error { error } => Err(ClientError::Daemon(error)),
            response => Ok(response),
        }
    }

    /// Read the next message on an open stream, or `None` once the daemon closed it.
    pub async fn next_message(&mut self) -> Result<Option<DaemonResponse>, ClientError> {
        match self.connection.read_frame().await {
            Ok(Some(line)) => decode_response(&line)
                .map(Some)
                .map_err(ClientError::Protocol),
            Ok(None) => Ok(None),
            Err(error) => Err(ClientError::Transport(error)),
        }
    }

    /// Stop reading and let the daemon see the stream end.
    ///
    /// This is what detaching is: the client closes its side, the daemon notices and forgets the
    /// viewer. The job it was watching is not told and does not care.
    pub async fn close(mut self) {
        self.connection.shutdown().await;
    }

    async fn receive(&mut self) -> Result<DaemonResponse, ClientError> {
        match self.connection.read_frame().await {
            Ok(Some(line)) => decode_response(&line).map_err(ClientError::Protocol),
            Ok(None) => Err(ClientError::Closed),
            Err(error) => Err(ClientError::Transport(error)),
        }
    }
}

/// The identity of the daemon owning one home, or `None` when none is running.
///
/// Answered by connecting and asking, which is the only answer that cannot be stale: recorded
/// metadata describes a daemon that may be gone, an endpoint that answers cannot be.
pub async fn identify(home: &LyaHome) -> Result<Option<DaemonIdentity>, ClientError> {
    let mut client = match DaemonClient::connect(home).await {
        Ok(client) => client,
        Err(ClientError::Transport(error)) if error.is_not_running() => return Ok(None),
        Err(error) => return Err(error),
    };
    match client.request(DaemonRequest::Ping).await {
        Ok(DaemonResponse::Pong { daemon }) => Ok(Some(daemon)),
        Ok(other) => Err(ClientError::Unexpected(describe(&other))),
        Err(ClientError::Transport(error)) if error.is_not_running() => Ok(None),
        Err(error) => Err(error),
    }
}

/// Wait until the daemon for one home answers, or give up.
pub async fn wait_until_ready(home: &LyaHome) -> Result<Option<DaemonIdentity>, ClientError> {
    for _ in 0..READINESS_ATTEMPTS {
        if let Some(identity) = identify(home).await? {
            return Ok(Some(identity));
        }
        tokio::time::sleep(READINESS_INTERVAL).await;
    }
    Ok(None)
}

/// Wait until the daemon for one home has really gone.
///
/// "Gone" is the release of [`DaemonLock`], never the disappearance of the endpoint. A daemon stops
/// serving its endpoint the moment a shutdown begins and then keeps running — holding its claim,
/// still driving the jobs it has to shut down safely — for as long as that takes. An unreachable
/// endpoint therefore proves only that the daemon stopped accepting; the claim is what proves the
/// process is finished with the home.
///
/// The distinction is what makes `lya daemon stop && lya daemon start` work: the next daemon can
/// only take the claim once this one has released it, so reporting on anything else would report
/// success on a home the next command cannot use.
pub async fn wait_until_released(home: &LyaHome) -> Result<bool, DaemonLockError> {
    for _ in 0..RELEASE_ATTEMPTS {
        if !DaemonLock::is_held(home)? {
            return Ok(true);
        }
        tokio::time::sleep(RELEASE_INTERVAL).await;
    }
    Ok(false)
}

/// A short description of an unexpected response, for error messages only.
pub fn describe(response: &DaemonResponse) -> String {
    match response {
        DaemonResponse::Pong { .. } => "PONG".to_owned(),
        DaemonResponse::Status { .. } => "STATUS".to_owned(),
        DaemonResponse::Submitted { .. } => "SUBMITTED".to_owned(),
        DaemonResponse::Attached { .. } => "ATTACHED".to_owned(),
        DaemonResponse::Event { .. } => "EVENT".to_owned(),
        DaemonResponse::Detached { .. } => "DETACHED".to_owned(),
        DaemonResponse::Controlled { .. } => "CONTROLLED".to_owned(),
        DaemonResponse::ShuttingDown { .. } => "SHUTTING_DOWN".to_owned(),
        DaemonResponse::Error { .. } => "ERROR".to_owned(),
    }
}

#[derive(Debug)]
pub enum ClientError {
    /// The daemon could not be reached, or the connection failed.
    Transport(TransportError),
    /// The daemon refused the request and said why.
    Daemon(DaemonErrorResponse),
    /// The daemon answered something this request cannot use.
    Unexpected(String),
    /// A frame could not be encoded or decoded.
    Protocol(String),
    /// The daemon closed the connection without answering.
    Closed,
}

impl ClientError {
    /// Whether this failure means "no daemon is running".
    pub fn is_not_running(&self) -> bool {
        matches!(self, Self::Transport(error) if error.is_not_running())
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => write!(formatter, "{error}"),
            Self::Daemon(error) => write!(
                formatter,
                "the daemon refused the request ({}): {}",
                error.code.label(),
                error.message
            ),
            Self::Unexpected(response) => {
                write!(
                    formatter,
                    "the daemon answered with an unexpected {response}"
                )
            }
            Self::Protocol(error) => write!(formatter, "{error}"),
            Self::Closed => {
                formatter.write_str("the daemon closed the connection without answering")
            }
        }
    }
}

impl Error for ClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use super::{identify, wait_until_released};
    use crate::{daemon::lock::DaemonLock, orchestrator::home::LyaHome};

    static NEXT_HOME: AtomicUsize = AtomicUsize::new(0);

    fn home() -> LyaHome {
        let path = std::env::temp_dir().join(format!(
            "lya-client-wait-{}-{}",
            std::process::id(),
            NEXT_HOME.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("home should be created");
        LyaHome::from_path(path)
    }

    /// Exactly the state a shutting-down daemon is in, reproduced without one: the endpoint answers
    /// nothing while the claim is still held.
    ///
    /// A wait defined on the endpoint would report the daemon stopped here, and the next
    /// `lya daemon start` would then be refused by a claim that command said was gone. The wait is
    /// defined on the claim, so it does not return until the claim does.
    #[tokio::test]
    async fn waiting_for_a_stop_follows_the_claim_and_not_the_endpoint() {
        let home = home();
        let claim = DaemonLock::acquire(&home).expect("the claim should be taken");

        assert!(
            identify(&home)
                .await
                .expect("probing should not fail")
                .is_none(),
            "nothing is serving the endpoint, so the endpoint alone would say `stopped`"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), wait_until_released(&home))
                .await
                .is_err(),
            "a held claim means the daemon has not finished, whatever the endpoint says"
        );

        drop(claim);

        assert!(
            wait_until_released(&home)
                .await
                .expect("the claim should be readable"),
            "the wait ends when the claim is released"
        );
        let _ = fs::remove_dir_all(home.path());
    }
}
