//! Local-only inter-process transport.
//!
//! The daemon is reachable from the machine it runs on and from nowhere else. No TCP socket is
//! opened, no port is bound and no address is advertised, so there is no remote surface to secure in
//! the first place:
//!
//! * **Windows** uses a named pipe, `\\.\pipe\lya-daemon-<home-fingerprint>`, created with an
//!   explicit access control list granting this user and `SYSTEM` and nothing else. The *default*
//!   descriptor is deliberately not used: it grants `FILE_GENERIC_READ` to `Everyone` and to
//!   `ANONYMOUS LOGON`, which is enough for any local account to open the pipe and occupy an
//!   instance. Remote clients are refused explicitly, and the first instance is created with
//!   `first_pipe_instance`, so Windows itself refuses a second daemon listening on the same name.
//!
//!   The list is not the whole answer, because it protects a pipe that exists and the name is a
//!   deterministic fingerprint any local account can create first. Both ends therefore verify the
//!   other's user before trusting it: the daemon checks every accepted client and disconnects a
//!   stranger, and a client checks the serving process before it sends anything. See
//!   [`security`](super::security).
//! * **Unix** uses a stream socket at `LYA_HOME/daemon.sock`, in a home directory kept at `0700`,
//!   with the socket itself at `0600`. File-system permissions are the access control, and there is
//!   no name to squat: the socket is a path inside a directory only the owner can traverse.
//!
//! The endpoint is derived from `LYA_HOME`, so two homes are two independent daemons and one home is
//! always the same endpoint — a client never has to be told where to look.
//!
//! Framing lives here too, because it is a property of the pipe rather than of any command: one
//! newline-delimited JSON object per message, each bounded by
//! [`MAX_MESSAGE_BYTES`](super::protocol::MAX_MESSAGE_BYTES). A frame longer than that ends the
//! offending connection and nothing else.

use std::{error::Error, fmt, path::PathBuf};

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, ReadHalf, WriteHalf};

use crate::orchestrator::home::LyaHome;

use super::protocol::MAX_MESSAGE_BYTES;

/// Where one `LYA_HOME`'s daemon listens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonEndpoint {
    address: String,
    /// The socket path on Unix. Windows named pipes are not file-system objects.
    path: Option<PathBuf>,
}

impl DaemonEndpoint {
    /// The endpoint that belongs to one Lya home.
    ///
    /// Derived from the home path itself, so discovery needs no registry: any client that can
    /// resolve `LYA_HOME` can address the daemon that owns it. The Windows name uses a fingerprint
    /// of the home path — a path is not a legal pipe name — computed with the same
    /// case-insensitive comparison repository identity uses, so one home never yields two endpoints.
    #[cfg(windows)]
    pub fn for_home(home: &LyaHome) -> Self {
        use crate::orchestrator::repository_lock::{comparable, fingerprint};

        Self {
            address: format!(
                "\\\\.\\pipe\\lya-daemon-{}",
                fingerprint(&comparable(home.path()))
            ),
            path: None,
        }
    }

    /// The endpoint that belongs to one Lya home: a socket inside the home itself.
    #[cfg(unix)]
    pub fn for_home(home: &LyaHome) -> Self {
        let path = home.path().join("daemon.sock");
        Self {
            address: path.display().to_string(),
            path: Some(path),
        }
    }

    /// The address a client connects to, as text. Stable for one home.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// The file-system object backing the endpoint, where there is one.
    pub fn path(&self) -> Option<&std::path::Path> {
        self.path.as_deref()
    }
}

/// Any local stream the daemon protocol can run over.
trait LocalStream: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin> LocalStream for T {}

type BoxedStream = Box<dyn LocalStream>;

/// One accepted or dialled connection, framed.
///
/// The `Debug` rendering names the type only: a live stream has no state worth printing and nothing
/// a client sent belongs in a diagnostic.
pub struct DaemonConnection {
    reader: DaemonReader,
    writer: DaemonWriter,
}

/// The reading half of a connection.
pub struct DaemonReader {
    reader: BufReader<ReadHalf<BoxedStream>>,
}

/// The writing half of a connection.
pub struct DaemonWriter {
    writer: WriteHalf<BoxedStream>,
}

impl DaemonConnection {
    fn new(stream: BoxedStream) -> Self {
        let (reader, writer) = tokio::io::split(stream);
        Self {
            reader: DaemonReader {
                reader: BufReader::new(reader),
            },
            writer: DaemonWriter { writer },
        }
    }

    /// Separate the halves.
    ///
    /// A streaming connection needs both at once — it writes events while watching for the client
    /// to go away — and one `&mut` borrow cannot do both.
    pub fn split(self) -> (DaemonReader, DaemonWriter) {
        (self.reader, self.writer)
    }

    /// Read one frame, or `None` when the peer closed the connection cleanly.
    pub async fn read_frame(&mut self) -> Result<Option<String>, TransportError> {
        self.reader.read_frame().await
    }

    /// Write one already-encoded frame and flush it.
    pub async fn write_frame(&mut self, frame: &[u8]) -> Result<(), TransportError> {
        self.writer.write_frame(frame).await
    }

    pub async fn shutdown(&mut self) {
        self.writer.shutdown().await;
    }
}

impl fmt::Debug for DaemonConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DaemonConnection")
    }
}

impl fmt::Debug for DaemonListener {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DaemonListener")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl DaemonReader {
    /// Read one frame, or `None` when the peer closed the connection cleanly.
    ///
    /// Bounded while it reads rather than afterwards: a peer that never sends a newline cannot make
    /// the daemon grow a buffer without limit.
    pub async fn read_frame(&mut self) -> Result<Option<String>, TransportError> {
        use tokio::io::AsyncBufReadExt;

        let mut frame = Vec::new();
        loop {
            let (complete, consumed) = {
                let available = self
                    .reader
                    .fill_buf()
                    .await
                    .map_err(|error| TransportError::Io(error.to_string()))?;
                if available.is_empty() {
                    if frame.is_empty() {
                        return Ok(None);
                    }
                    return Err(TransportError::Truncated);
                }
                match available.iter().position(|byte| *byte == b'\n') {
                    Some(index) => {
                        frame.extend_from_slice(&available[..index]);
                        (true, index + 1)
                    }
                    None => {
                        frame.extend_from_slice(available);
                        (false, available.len())
                    }
                }
            };
            self.reader.consume(consumed);
            if frame.len() > MAX_MESSAGE_BYTES {
                return Err(TransportError::FrameTooLong);
            }
            if complete {
                let line = String::from_utf8(frame).map_err(|_| {
                    TransportError::Encoding("a frame was not valid UTF-8".to_owned())
                })?;
                return Ok(Some(line.trim_end_matches('\r').to_owned()));
            }
        }
    }
}

impl DaemonWriter {
    /// Write one already-encoded frame and flush it, so a waiting peer never stalls behind a buffer.
    pub async fn write_frame(&mut self, frame: &[u8]) -> Result<(), TransportError> {
        self.writer
            .write_all(frame)
            .await
            .map_err(|error| TransportError::Io(error.to_string()))?;
        self.writer
            .flush()
            .await
            .map_err(|error| TransportError::Io(error.to_string()))
    }

    pub async fn shutdown(&mut self) {
        let _ = self.writer.shutdown().await;
    }
}

/// The daemon's listening endpoint.
pub struct DaemonListener {
    endpoint: DaemonEndpoint,
    inner: ListenerKind,
}

impl DaemonListener {
    /// Start listening.
    ///
    /// The caller is expected to hold the daemon claim already: binding makes the daemon reachable,
    /// it is not what makes it the owner. Binding is nevertheless refused when something is already
    /// serving the endpoint, so the transport is a second independent guarantee that one home has
    /// one daemon.
    pub async fn bind(endpoint: &DaemonEndpoint) -> Result<Self, TransportError> {
        Ok(Self {
            endpoint: endpoint.clone(),
            inner: ListenerKind::bind(endpoint)?,
        })
    }

    pub fn endpoint(&self) -> &DaemonEndpoint {
        &self.endpoint
    }

    /// The access control list the live endpoint carries, where the platform has one, decoded into
    /// principals.
    #[cfg(all(test, windows))]
    pub fn live_dacl(&self) -> Option<crate::daemon::security::Dacl> {
        match &self.inner {
            ListenerKind::Pipe(listener) => listener.live_dacl(),
        }
    }

    /// The same list in its textual form, for a failure message only.
    #[cfg(all(test, windows))]
    pub fn live_dacl_sddl(&self) -> Option<String> {
        match &self.inner {
            ListenerKind::Pipe(listener) => listener.live_dacl_sddl(),
        }
    }

    /// Wait for the next client.
    pub async fn accept(&mut self) -> Result<DaemonConnection, TransportError> {
        self.inner.accept().await
    }
}

/// Connect to the daemon that owns one endpoint.
pub async fn connect(endpoint: &DaemonEndpoint) -> Result<DaemonConnection, TransportError> {
    platform::connect(endpoint).await
}

enum ListenerKind {
    #[cfg(windows)]
    Pipe(platform::PipeListener),
    #[cfg(unix)]
    Socket(tokio::net::UnixListener),
}

impl ListenerKind {
    #[cfg(windows)]
    fn bind(endpoint: &DaemonEndpoint) -> Result<Self, TransportError> {
        Ok(Self::Pipe(platform::PipeListener::bind(
            endpoint.address(),
        )?))
    }

    #[cfg(unix)]
    fn bind(endpoint: &DaemonEndpoint) -> Result<Self, TransportError> {
        Ok(Self::Socket(platform::bind_socket(endpoint)?))
    }

    async fn accept(&mut self) -> Result<DaemonConnection, TransportError> {
        match self {
            #[cfg(windows)]
            Self::Pipe(listener) => listener.accept().await,
            #[cfg(unix)]
            Self::Socket(listener) => {
                let (stream, _address) = listener
                    .accept()
                    .await
                    .map_err(|error| TransportError::Io(error.to_string()))?;
                Ok(DaemonConnection::new(Box::new(stream)))
            }
        }
    }
}

#[cfg(windows)]
mod platform {
    use std::os::windows::io::AsRawHandle;

    use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeServer, ServerOptions};

    use crate::daemon::security::{PipeSecurity, Sid, current_user_sid, verify_pipe_client};

    use super::{BoxedStream, DaemonConnection, DaemonEndpoint, TransportError};

    /// `ERROR_FILE_NOT_FOUND`: no daemon is listening on this name.
    const ERROR_FILE_NOT_FOUND: i32 = 2;
    /// `ERROR_ACCESS_DENIED`: another process already owns the first instance of this name.
    const ERROR_ACCESS_DENIED: i32 = 5;
    /// `ERROR_PIPE_BUSY`: every instance is serving; the daemon creates the next one immediately.
    const ERROR_PIPE_BUSY: i32 = 231;

    /// A named-pipe listener.
    ///
    /// Windows hands out one pipe *instance* per client, so the listener always keeps exactly one
    /// idle instance waiting. The next instance is created as soon as a client takes the current
    /// one, which is what keeps a connecting client from finding the name unserved.
    ///
    /// Every instance carries the same explicit access control list, and every accepted client is
    /// checked against the daemon's own user before it is handed to the protocol. The list decides
    /// who may open the pipe; the check decides who is believed once they have.
    pub(super) struct PipeListener {
        address: String,
        security: PipeSecurity,
        owner: Sid,
        idle: Option<NamedPipeServer>,
    }

    impl PipeListener {
        pub(super) fn bind(address: &str) -> Result<Self, TransportError> {
            let owner =
                current_user_sid().map_err(|error| TransportError::Io(error.to_string()))?;
            let security = PipeSecurity::for_current_user()
                .map_err(|error| TransportError::Io(error.to_string()))?;
            // `first_pipe_instance` makes Windows itself refuse a second listener on this name.
            let server = create_instance(&security, address, true).map_err(|error| match error
                .raw_os_error()
            {
                Some(ERROR_ACCESS_DENIED) => TransportError::AlreadyListening(address.to_owned()),
                _ => TransportError::Io(error.to_string()),
            })?;
            Ok(Self {
                address: address.to_owned(),
                security,
                owner,
                idle: Some(server),
            })
        }

        /// Wait for the next client this daemon is willing to talk to.
        ///
        /// A client that is not the daemon's own user is disconnected and the loop simply waits
        /// again: it is not an error the daemon has to report upwards, and it must not be able to
        /// end the accept loop.
        pub(super) async fn accept(&mut self) -> Result<DaemonConnection, TransportError> {
            loop {
                let server = match self.idle.take() {
                    Some(server) => server,
                    None => self.next_instance()?,
                };
                server
                    .connect()
                    .await
                    .map_err(|error| TransportError::Io(error.to_string()))?;
                // Created before this connection is served, so the name is never momentarily
                // unserved.
                self.idle = Some(self.next_instance()?);

                // Synchronous, and deliberately so: impersonation is a property of the thread, and
                // an `.await` between impersonating and reverting would let the runtime hand that
                // identity to another task.
                // SAFETY: `server` is an open server handle whose client has just connected.
                if unsafe { verify_pipe_client(server.as_raw_handle() as _, &self.owner) }.is_err()
                {
                    drop(server);
                    continue;
                }
                let stream: BoxedStream = Box::new(server);
                return Ok(DaemonConnection::new(stream));
            }
        }

        fn next_instance(&self) -> Result<NamedPipeServer, TransportError> {
            create_instance(&self.security, &self.address, false)
                .map_err(|error| TransportError::Io(error.to_string()))
        }

        /// The access control list the listening pipe actually carries, asked of the pipe itself
        /// and decoded into principals.
        ///
        /// A list that was asked for is not evidence that the kernel object got it, and this is the
        /// only claim in the transport that a test cannot make any other way.
        #[cfg(test)]
        pub(super) fn live_dacl(&self) -> Option<crate::daemon::security::Dacl> {
            use std::os::windows::io::AsRawHandle;

            let idle = self.idle.as_ref()?;
            // SAFETY: `idle` is an open pipe handle owned by this listener.
            unsafe { crate::daemon::security::object_dacl(idle.as_raw_handle() as _) }.ok()
        }

        /// The same list in its textual form, for a failure message. Never asserted against: the
        /// rendering substitutes two-letter aliases for well-known security identifiers.
        #[cfg(test)]
        pub(super) fn live_dacl_sddl(&self) -> Option<String> {
            use std::os::windows::io::AsRawHandle;

            let idle = self.idle.as_ref()?;
            // SAFETY: `idle` is an open pipe handle owned by this listener.
            unsafe { crate::daemon::security::object_dacl_sddl(idle.as_raw_handle() as _) }.ok()
        }
    }

    /// One pipe instance, with this daemon's own access control list rather than the default one.
    fn create_instance(
        security: &PipeSecurity,
        address: &str,
        first: bool,
    ) -> std::io::Result<NamedPipeServer> {
        let mut options = ServerOptions::new();
        options.reject_remote_clients(true);
        if first {
            options.first_pipe_instance(true);
        }
        // SAFETY: the attributes and the descriptor they point at are owned by `security`, which
        // outlives this call; `CreateNamedPipeW` copies what it needs.
        unsafe { options.create_with_security_attributes_raw(address, security.attributes()) }
    }

    pub(super) async fn connect(
        endpoint: &DaemonEndpoint,
    ) -> Result<DaemonConnection, TransportError> {
        let expected = current_user_sid().map_err(|error| TransportError::Io(error.to_string()))?;
        // A busy name means every instance is currently being handed over, not that nobody is
        // listening. The retry is bounded and short; a missing name is reported immediately.
        for _ in 0..50 {
            match ClientOptions::new().open(endpoint.address()) {
                Ok(client) => {
                    // Before a single byte is sent. The name alone proves nothing — any local
                    // account can create it — so a server that is not this user is not the daemon,
                    // whatever it answers.
                    // SAFETY: `client` is an open client handle for this pipe.
                    if let Err(error) = unsafe {
                        crate::daemon::security::verify_pipe_server(
                            client.as_raw_handle() as _,
                            &expected,
                        )
                    } {
                        return Err(TransportError::Untrusted(error.to_string()));
                    }
                    let stream: BoxedStream = Box::new(client);
                    return Ok(DaemonConnection::new(stream));
                }
                Err(error) if error.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                Err(error) if error.raw_os_error() == Some(ERROR_FILE_NOT_FOUND) => {
                    return Err(TransportError::NotRunning(endpoint.address().to_owned()));
                }
                Err(error) => return Err(TransportError::Io(error.to_string())),
            }
        }
        Err(TransportError::Io(format!(
            "{} stayed busy; no pipe instance became available",
            endpoint.address()
        )))
    }
}

#[cfg(unix)]
mod platform {
    use std::{fs, os::unix::fs::PermissionsExt};

    use tokio::net::{UnixListener, UnixStream};

    use super::{BoxedStream, DaemonConnection, DaemonEndpoint, TransportError};

    pub(super) fn bind_socket(endpoint: &DaemonEndpoint) -> Result<UnixListener, TransportError> {
        let path = endpoint
            .path()
            .ok_or_else(|| TransportError::Io("the endpoint has no socket path".to_owned()))?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| TransportError::Io(error.to_string()))?;
            // The directory is the real access control for the socket inside it.
            let _ = fs::set_permissions(parent, fs::Permissions::from_mode(0o700));
        }
        if path.exists() {
            // A socket file cannot say by itself whether its owner is alive, so it is asked: a
            // socket that still accepts a connection belongs to a live daemon and is left alone,
            // and one that refuses was left behind by a dead one and must not block startup for
            // ever.
            if std::os::unix::net::UnixStream::connect(path).is_ok() {
                return Err(TransportError::AlreadyListening(
                    endpoint.address().to_owned(),
                ));
            }
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(TransportError::Io(error.to_string())),
            }
        }
        let listener =
            UnixListener::bind(path).map_err(|error| TransportError::Io(error.to_string()))?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(|error| TransportError::Io(error.to_string()))?;
        Ok(listener)
    }

    pub(super) async fn connect(
        endpoint: &DaemonEndpoint,
    ) -> Result<DaemonConnection, TransportError> {
        let path = endpoint
            .path()
            .ok_or_else(|| TransportError::Io("the endpoint has no socket path".to_owned()))?;
        match UnixStream::connect(path).await {
            Ok(stream) => {
                let stream: BoxedStream = Box::new(stream);
                Ok(DaemonConnection::new(stream))
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                ) =>
            {
                Err(TransportError::NotRunning(endpoint.address().to_owned()))
            }
            Err(error) => Err(TransportError::Io(error.to_string())),
        }
    }
}

#[derive(Debug)]
pub enum TransportError {
    /// Nothing is listening on the endpoint.
    NotRunning(String),
    /// Something else already listens there.
    AlreadyListening(String),
    /// Something answered on the endpoint, and it is not this user's daemon.
    Untrusted(String),
    /// A frame exceeded the protocol bound.
    FrameTooLong,
    /// The peer disappeared mid-frame.
    Truncated,
    Encoding(String),
    Io(String),
}

impl TransportError {
    /// Whether this error means "no daemon", as opposed to a real failure.
    pub fn is_not_running(&self) -> bool {
        matches!(self, Self::NotRunning(_))
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotRunning(address) => {
                write!(formatter, "no Lya daemon is listening on {address}")
            }
            Self::AlreadyListening(address) => {
                write!(formatter, "another process already listens on {address}")
            }
            Self::Untrusted(detail) => write!(
                formatter,
                "the process answering on the Lya endpoint is not this user's daemon: {detail}"
            ),
            Self::FrameTooLong => write!(
                formatter,
                "a protocol frame exceeded {MAX_MESSAGE_BYTES} bytes"
            ),
            Self::Truncated => formatter.write_str("the connection ended inside a protocol frame"),
            Self::Encoding(error) => write!(formatter, "invalid protocol encoding: {error}"),
            Self::Io(error) => write!(formatter, "local transport failure: {error}"),
        }
    }
}

impl Error for TransportError {}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{DaemonEndpoint, DaemonListener, TransportError, connect};
    use crate::{daemon::protocol::MAX_MESSAGE_BYTES, orchestrator::home::LyaHome};

    static NEXT_HOME: AtomicUsize = AtomicUsize::new(0);

    fn home() -> LyaHome {
        let path = std::env::temp_dir().join(format!(
            "lya-transport-{}-{}",
            std::process::id(),
            NEXT_HOME.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("home should be created");
        LyaHome::from_path(path)
    }

    #[test]
    fn one_home_always_resolves_to_one_endpoint_and_two_homes_never_share_one() {
        let first_home = home();
        let second_home = home();

        let first = DaemonEndpoint::for_home(&first_home);
        let again = DaemonEndpoint::for_home(&LyaHome::from_path(first_home.path()));
        let second = DaemonEndpoint::for_home(&second_home);

        assert_eq!(first, again);
        assert!(!first.address().is_empty());
        assert_ne!(first.address(), second.address());
        let _ = std::fs::remove_dir_all(first_home.path());
        let _ = std::fs::remove_dir_all(second_home.path());
    }

    /// The Windows endpoint has to be a legal pipe name whatever the home path looks like, which is
    /// exactly why it is a fingerprint rather than the path.
    #[cfg(windows)]
    #[test]
    fn the_windows_endpoint_is_a_named_pipe() {
        let home = LyaHome::from_path(std::path::PathBuf::from(
            "C:/Users/Someone/With Spaces/.lya",
        ));

        let endpoint = DaemonEndpoint::for_home(&home);

        assert!(
            endpoint.address().starts_with("\\\\.\\pipe\\lya-daemon-"),
            "{}",
            endpoint.address()
        );
        assert!(
            !endpoint.address()["\\\\.\\pipe\\".len()..].contains(['/', ':', ' ']),
            "a pipe name may not carry path punctuation: {}",
            endpoint.address()
        );
        assert_eq!(endpoint.path(), None);
    }

    /// Windows compares paths case-insensitively, so the same home reached through different letter
    /// case must be the same daemon.
    #[cfg(windows)]
    #[test]
    fn windows_letter_case_resolves_to_one_endpoint() {
        let lower = DaemonEndpoint::for_home(&LyaHome::from_path(std::path::PathBuf::from(
            "c:/users/a/.lya",
        )));
        let upper = DaemonEndpoint::for_home(&LyaHome::from_path(std::path::PathBuf::from(
            "C:/Users/A/.lya",
        )));

        assert_eq!(lower, upper);
    }

    #[cfg(unix)]
    #[test]
    fn the_unix_endpoint_is_a_socket_inside_the_home() {
        let home = LyaHome::from_path(std::path::PathBuf::from("/tmp/lya-endpoint"));

        let endpoint = DaemonEndpoint::for_home(&home);

        assert_eq!(
            endpoint.path(),
            Some(std::path::Path::new("/tmp/lya-endpoint/daemon.sock"))
        );
    }

    #[tokio::test]
    async fn a_client_connects_to_a_listening_endpoint_and_frames_round_trip() {
        let home = home();
        let endpoint = DaemonEndpoint::for_home(&home);
        let mut listener = DaemonListener::bind(&endpoint)
            .await
            .expect("the endpoint should bind");
        let server = tokio::spawn(async move {
            let mut connection = listener.accept().await.expect("a client should arrive");
            let frame = connection
                .read_frame()
                .await
                .expect("the frame should read")
                .expect("the client sent a frame");
            connection
                .write_frame(format!("echo:{frame}\n").as_bytes())
                .await
                .expect("the reply should write");
            connection.shutdown().await;
            frame
        });

        let mut client = connect(&endpoint).await.expect("the client should connect");
        client
            .write_frame(b"{\"hello\":true}\n")
            .await
            .expect("the request should write");
        let reply = client
            .read_frame()
            .await
            .expect("the reply should read")
            .expect("the daemon replied");

        assert_eq!(reply, "echo:{\"hello\":true}");
        assert_eq!(
            server.await.expect("the server task should finish"),
            "{\"hello\":true}"
        );
        let _ = std::fs::remove_dir_all(home.path());
    }

    #[tokio::test]
    async fn connecting_to_an_unserved_endpoint_reports_that_no_daemon_runs() {
        let home = home();
        let endpoint = DaemonEndpoint::for_home(&home);

        let error = connect(&endpoint)
            .await
            .expect_err("nothing is listening yet");

        assert!(error.is_not_running(), "{error}");
        assert!(error.to_string().contains("no Lya daemon"));
        let _ = std::fs::remove_dir_all(home.path());
    }

    /// A peer that never terminates a frame must not be able to grow the reader without bound.
    #[tokio::test]
    async fn an_unterminated_oversized_frame_is_refused() {
        let home = home();
        let endpoint = DaemonEndpoint::for_home(&home);
        let mut listener = DaemonListener::bind(&endpoint)
            .await
            .expect("the endpoint should bind");
        let server = tokio::spawn(async move {
            let mut connection = listener.accept().await.expect("a client should arrive");
            connection.read_frame().await
        });

        let mut client = connect(&endpoint).await.expect("the client should connect");
        let flood = vec![b'x'; MAX_MESSAGE_BYTES + 1024];
        // The daemon may refuse and close before the whole flood is written, which is the point.
        let _ = client.write_frame(&flood).await;

        let result = server.await.expect("the server task should finish");
        assert!(
            matches!(result, Err(TransportError::FrameTooLong)),
            "an oversized frame should be refused: {result:?}"
        );
        let _ = std::fs::remove_dir_all(home.path());
    }

    /// The endpoint a daemon really listens on, asked of the pipe itself.
    ///
    /// Windows' default named-pipe descriptor grants `FILE_GENERIC_READ` to `Everyone` and to
    /// `ANONYMOUS LOGON`, which is enough for any local account to open the pipe and hold an
    /// instance. Neither may appear on Lya's.
    #[cfg(windows)]
    #[tokio::test]
    async fn the_listening_pipe_grants_only_this_user_and_system() {
        use crate::daemon::security::{check_endpoint_dacl, current_user_sid};

        let home = home();
        let endpoint = DaemonEndpoint::for_home(&home);
        let listener = DaemonListener::bind(&endpoint)
            .await
            .expect("the endpoint should bind");

        let dacl = listener
            .live_dacl()
            .expect("a listening pipe has an access control list");
        let sddl = listener.live_dacl_sddl().unwrap_or_default();
        let user = current_user_sid().expect("this process has a user SID");

        // Compared by security identifier, not by the rendered SDDL: Windows spells a well-known
        // principal with its two-letter alias, so the current user comes back as `LA` on a machine
        // where that user is the built-in local Administrator account.
        println!("endpoint DACL: {sddl}");
        println!("owning user: {user:?}");
        check_endpoint_dacl(&dacl, &user).unwrap_or_else(|reason| {
            panic!("the endpoint's list is wrong: {reason}\nSDDL: {sddl}")
        });

        drop(listener);
        let _ = std::fs::remove_dir_all(home.path());
    }

    /// The daemon this user's client talks to is the one running as this user.
    ///
    /// A connection that gets through is one where both ends checked the other, so this covers the
    /// accepting side and the dialling side at once; the refusal of a *different* user is the same
    /// comparison with the other answer, covered in
    /// [`crate::daemon::security`].
    #[cfg(windows)]
    #[tokio::test]
    async fn a_connection_is_only_established_between_two_ends_of_the_same_user() {
        let home = home();
        let endpoint = DaemonEndpoint::for_home(&home);
        let mut listener = DaemonListener::bind(&endpoint)
            .await
            .expect("the endpoint should bind");
        let server = tokio::spawn(async move { listener.accept().await.map(|_| ()) });

        let client = connect(&endpoint)
            .await
            .expect("this user's client reaches this user's daemon");

        assert!(
            server.await.expect("the server task should finish").is_ok(),
            "the daemon accepts a client of its own user"
        );
        drop(client);
        let _ = std::fs::remove_dir_all(home.path());
    }

    #[tokio::test]
    async fn a_second_listener_on_the_same_endpoint_is_refused() {
        let home = home();
        let endpoint = DaemonEndpoint::for_home(&home);
        let _first = DaemonListener::bind(&endpoint)
            .await
            .expect("the first listener should bind");

        let error = DaemonListener::bind(&endpoint)
            .await
            .expect_err("one endpoint must never be served by two listeners");

        assert!(
            matches!(error, TransportError::AlreadyListening(_)),
            "{error}"
        );
        let _ = std::fs::remove_dir_all(home.path());
    }
}
