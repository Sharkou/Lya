//! Windows access control and identity for the daemon's local endpoint.
//!
//! Two independent guarantees, both needed, neither sufficient alone:
//!
//! * **Who may open the pipe** is decided by an explicit discretionary access control list built
//!   here and handed to `CreateNamedPipeW`. The Windows *default* security descriptor for a named
//!   pipe grants `FILE_GENERIC_READ` to `Everyone` **and** to `ANONYMOUS LOGON` — enough for any
//!   local account to occupy a pipe instance — so the default is never used.
//! * **Who is on the other end** is decided by comparing security identifiers. A DACL protects a
//!   pipe that exists; it says nothing about a pipe created by somebody else under the name this
//!   daemon was going to use. The endpoint name is a deterministic fingerprint of `LYA_HOME` and
//!   the named-pipe namespace is writable by any local account, so a squatter can own the name
//!   first. Both ends therefore check the other's identity before trusting it.
//!
//! Identity here means a token user SID compared with [`EqualSid`], never a process ID. A process
//! ID is used only as a way to *reach* a token, and every failure on that path is a refusal.
//!
//! Everything is `unsafe` at the boundary and safe above it: each function owns what it allocates,
//! releases it on every path, and returns a plain Rust value.

use std::{ffi::c_void, fmt, ptr};

use windows_sys::Win32::{
    Foundation::{
        CloseHandle, ERROR_INSUFFICIENT_BUFFER, HANDLE, HLOCAL, INVALID_HANDLE_VALUE, LocalFree,
    },
    Security::{
        Authorization::{
            ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
            SDDL_REVISION_1,
        },
        EqualSid, GetLengthSid, GetTokenInformation, PSECURITY_DESCRIPTOR, PSID, RevertToSelf,
        SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER, TokenUser,
    },
    System::{
        Pipes::{
            GetNamedPipeClientProcessId, GetNamedPipeServerProcessId, ImpersonateNamedPipeClient,
        },
        Threading::{
            GetCurrentProcess, GetCurrentThread, OpenProcess, OpenProcessToken, OpenThreadToken,
            PROCESS_QUERY_LIMITED_INFORMATION,
        },
    },
};

/// A security identifier, owned.
///
/// A `TOKEN_USER` points into the buffer it was read from, so the SID is copied out and kept on its
/// own. Comparison goes through `EqualSid`, which is the documented way to decide that two SIDs
/// name the same principal; the string form exists only for the SDDL and for diagnostics.
#[derive(Clone)]
pub struct Sid {
    bytes: Vec<u8>,
}

impl Sid {
    /// Copy a SID out of memory somebody else owns.
    ///
    /// # Safety
    ///
    /// `raw` must point at a valid SID that stays valid for this call.
    unsafe fn from_raw(raw: PSID) -> Result<Self, SecurityError> {
        if raw.is_null() {
            return Err(SecurityError::new("a token carried no user SID"));
        }
        let length = unsafe { GetLengthSid(raw) };
        if length == 0 {
            return Err(SecurityError::last("GetLengthSid"));
        }
        let mut bytes = vec![0u8; length as usize];
        unsafe { ptr::copy_nonoverlapping(raw.cast::<u8>(), bytes.as_mut_ptr(), length as usize) };
        Ok(Self { bytes })
    }

    fn as_psid(&self) -> PSID {
        self.bytes.as_ptr() as PSID
    }

    /// The SDDL string form, `S-1-5-…`.
    pub fn to_sddl(&self) -> Result<String, SecurityError> {
        let mut raw = ptr::null_mut();
        if unsafe { ConvertSidToStringSidW(self.as_psid(), &mut raw) } == 0 {
            return Err(SecurityError::last("ConvertSidToStringSidW"));
        }
        let text = unsafe { wide_to_string(raw) };
        unsafe { LocalFree(raw as HLOCAL) };
        Ok(text)
    }
}

impl PartialEq for Sid {
    fn eq(&self, other: &Self) -> bool {
        unsafe { EqualSid(self.as_psid(), other.as_psid()) != 0 }
    }
}

impl Eq for Sid {}

impl fmt::Debug for Sid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.to_sddl() {
            Ok(text) => formatter.write_str(&text),
            Err(_) => formatter.write_str("<unprintable SID>"),
        }
    }
}

/// The user this process runs as.
pub fn current_user_sid() -> Result<Sid, SecurityError> {
    let mut token = INVALID_HANDLE_VALUE;
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(SecurityError::last("OpenProcessToken"));
    }
    let sid = unsafe { token_user_sid(token) };
    unsafe { CloseHandle(token) };
    sid
}

/// The user a token belongs to.
///
/// # Safety
///
/// `token` must be an open token handle with `TOKEN_QUERY`.
unsafe fn token_user_sid(token: HANDLE) -> Result<Sid, SecurityError> {
    let mut needed = 0u32;
    let probed =
        unsafe { GetTokenInformation(token, TokenUser, ptr::null_mut(), 0, &mut needed) } != 0;
    if !probed {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(ERROR_INSUFFICIENT_BUFFER as i32) {
            return Err(SecurityError::new(format!(
                "GetTokenInformation failed: {error}"
            )));
        }
    }
    if needed == 0 {
        return Err(SecurityError::new("a token reported no user information"));
    }
    let mut buffer = vec![0u8; needed as usize];
    if unsafe {
        GetTokenInformation(
            token,
            TokenUser,
            buffer.as_mut_ptr().cast::<c_void>(),
            needed,
            &mut needed,
        )
    } == 0
    {
        return Err(SecurityError::last("GetTokenInformation"));
    }
    let user = unsafe { &*buffer.as_ptr().cast::<TOKEN_USER>() };
    unsafe { Sid::from_raw(user.User.Sid) }
}

/// An explicit security descriptor for the daemon's named pipe, and the attributes that carry it.
///
/// Self-owning: the descriptor is a `LocalAlloc` block released on drop, and the attributes point at
/// it. Nothing may outlive this value, which is why [`PipeSecurity::attributes`] borrows.
pub struct PipeSecurity {
    descriptor: PSECURITY_DESCRIPTOR,
    attributes: SECURITY_ATTRIBUTES,
    sddl: String,
}

// The descriptor is a plain heap block this value owns exclusively; nothing in it is thread-affine.
unsafe impl Send for PipeSecurity {}
unsafe impl Sync for PipeSecurity {}

impl PipeSecurity {
    /// A pipe only this user and `SYSTEM` may touch.
    ///
    /// `D:P` protects the list from inheritance, and the two `GA` entries are the whole of it.
    /// `Everyone` and `ANONYMOUS LOGON` are absent by construction rather than by removal, and
    /// local administrators are absent too: an administrator can already reach anything on the
    /// machine by other means, so naming them here would widen the pipe without adding a capability
    /// anybody actually needs. `SYSTEM` stays because it is the one principal the operating system
    /// itself may need to act as.
    pub fn for_current_user() -> Result<Self, SecurityError> {
        Self::for_user(&current_user_sid()?)
    }

    fn for_user(user: &Sid) -> Result<Self, SecurityError> {
        let sddl = format!("D:P(A;;GA;;;SY)(A;;GA;;;{})", user.to_sddl()?);
        let wide = to_wide(&sddl);
        let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
        if unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                wide.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                ptr::null_mut(),
            )
        } == 0
        {
            return Err(SecurityError::last(
                "ConvertStringSecurityDescriptorToSecurityDescriptorW",
            ));
        }
        Ok(Self {
            descriptor,
            attributes: SECURITY_ATTRIBUTES {
                nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
                lpSecurityDescriptor: descriptor,
                bInheritHandle: 0,
            },
            sddl,
        })
    }

    /// The attributes to hand to `CreateNamedPipeW`, valid for as long as `self` is.
    pub fn attributes(&self) -> *mut c_void {
        ptr::from_ref(&self.attributes) as *mut c_void
    }

    /// The access control list this was built from, for tests and diagnostics.
    pub fn sddl(&self) -> &str {
        &self.sddl
    }
}

impl Drop for PipeSecurity {
    fn drop(&mut self) {
        if !self.descriptor.is_null() {
            unsafe { LocalFree(self.descriptor as HLOCAL) };
        }
    }
}

impl fmt::Debug for PipeSecurity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PipeSecurity")
            .field("sddl", &self.sddl)
            .finish()
    }
}

/// How a peer's identity was established.
///
/// Recorded because the two are not equally strong and a reader should not have to guess which one
/// ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityProof {
    /// The client's own token, obtained by impersonating it. No process ID is involved, so nothing
    /// can be substituted between the check and the connection it describes.
    Impersonation,
    /// The peer's token, reached through the process ID the kernel reports for *this* pipe handle.
    /// Weaker than impersonation only in that it goes through a process handle; it is still the
    /// kernel answering, and every failure on the path is a refusal.
    PipePeerProcess,
}

/// Refuse the connection unless the client is the same user as this process.
///
/// Impersonation first, because it names the client's token directly. It is not always available —
/// a client that has not yet written to a byte-mode pipe cannot always be impersonated — so the
/// documented fallback is the process ID the kernel associates with this very pipe handle, used
/// only to open that process's token. Nothing is inferred from the process ID itself, and a process
/// that has already exited or cannot be opened is a refusal rather than a guess.
///
/// # Safety
///
/// `pipe` must be an open named-pipe *server* handle whose client has connected.
pub unsafe fn verify_pipe_client(
    pipe: HANDLE,
    expected: &Sid,
) -> Result<IdentityProof, SecurityError> {
    match unsafe { impersonated_client_sid(pipe) } {
        Ok(sid) => {
            if &sid == expected {
                Ok(IdentityProof::Impersonation)
            } else {
                Err(SecurityError::rejected(&sid, expected))
            }
        }
        Err(_) => {
            let mut process_id = 0u32;
            if unsafe { GetNamedPipeClientProcessId(pipe, &mut process_id) } == 0 {
                return Err(SecurityError::last("GetNamedPipeClientProcessId"));
            }
            let sid = process_user_sid(process_id)?;
            if &sid == expected {
                Ok(IdentityProof::PipePeerProcess)
            } else {
                Err(SecurityError::rejected(&sid, expected))
            }
        }
    }
}

/// Refuse the connection unless the pipe server is the same user as this process.
///
/// This is what a squatter fails: any local account can create the daemon's pipe name first, and
/// nothing about the name itself distinguishes the real daemon from that. There is no client-side
/// equivalent of impersonation, so the kernel's own answer for *this handle* — which process is
/// serving it — is used to reach the server's token, and any failure refuses.
///
/// # Safety
///
/// `pipe` must be an open named-pipe *client* handle.
pub unsafe fn verify_pipe_server(
    pipe: HANDLE,
    expected: &Sid,
) -> Result<IdentityProof, SecurityError> {
    let mut process_id = 0u32;
    if unsafe { GetNamedPipeServerProcessId(pipe, &mut process_id) } == 0 {
        return Err(SecurityError::last("GetNamedPipeServerProcessId"));
    }
    let sid = process_user_sid(process_id)?;
    if &sid == expected {
        Ok(IdentityProof::PipePeerProcess)
    } else {
        Err(SecurityError::rejected(&sid, expected))
    }
}

/// The client's SID, read from its own token while impersonating it.
///
/// Impersonation is per-thread and is reverted on every path, including the failing ones: a thread
/// left impersonating would hand the next task on it the wrong identity.
///
/// # Safety
///
/// `pipe` must be an open named-pipe server handle whose client has connected.
unsafe fn impersonated_client_sid(pipe: HANDLE) -> Result<Sid, SecurityError> {
    if unsafe { ImpersonateNamedPipeClient(pipe) } == 0 {
        return Err(SecurityError::last("ImpersonateNamedPipeClient"));
    }
    let mut token = INVALID_HANDLE_VALUE;
    let opened = unsafe { OpenThreadToken(GetCurrentThread(), TOKEN_QUERY, 1, &mut token) } != 0;
    let sid = if opened {
        let sid = unsafe { token_user_sid(token) };
        unsafe { CloseHandle(token) };
        sid
    } else {
        Err(SecurityError::last("OpenThreadToken"))
    };
    if unsafe { RevertToSelf() } == 0 {
        // A thread that cannot stop impersonating must not be reused, and no identity it produced
        // can be trusted.
        return Err(SecurityError::last("RevertToSelf"));
    }
    sid
}

/// The user one process runs as.
fn process_user_sid(process_id: u32) -> Result<Sid, SecurityError> {
    if process_id == 0 {
        return Err(SecurityError::new("the peer reported no process"));
    }
    let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, process_id) };
    if process.is_null() || process == INVALID_HANDLE_VALUE {
        return Err(SecurityError::last("OpenProcess"));
    }
    let mut token = INVALID_HANDLE_VALUE;
    let opened = unsafe { OpenProcessToken(process, TOKEN_QUERY, &mut token) } != 0;
    let sid = if opened {
        let sid = unsafe { token_user_sid(token) };
        unsafe { CloseHandle(token) };
        sid
    } else {
        Err(SecurityError::last("OpenProcessToken"))
    };
    unsafe { CloseHandle(process) };
    sid
}

fn to_wide(value: &str) -> Vec<u16> {
    value.encode_utf16().chain(std::iter::once(0)).collect()
}

/// # Safety
///
/// `raw` must point at a NUL-terminated UTF-16 string.
unsafe fn wide_to_string(raw: *const u16) -> String {
    let mut length = 0;
    while unsafe { *raw.add(length) } != 0 {
        length += 1;
    }
    String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(raw, length) })
}

#[derive(Debug)]
pub struct SecurityError {
    message: String,
}

impl SecurityError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    fn last(call: &str) -> Self {
        Self::new(format!(
            "{call} failed: {}",
            std::io::Error::last_os_error()
        ))
    }

    fn rejected(actual: &Sid, expected: &Sid) -> Self {
        Self::new(format!(
            "the peer runs as {actual:?}, not as {expected:?}; refusing the connection"
        ))
    }
}

impl fmt::Display for SecurityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for SecurityError {}

/// Read back the access control list an already-created kernel object actually carries.
///
/// Exists for tests: an access control list that was *asked for* is not evidence, and this asks the
/// object itself what it ended up with.
///
/// # Safety
///
/// `handle` must be an open kernel object handle carrying `READ_CONTROL`.
#[cfg(test)]
pub unsafe fn object_dacl_sddl(handle: HANDLE) -> Result<String, SecurityError> {
    use windows_sys::Win32::{
        Foundation::ERROR_SUCCESS,
        Security::{
            Authorization::{
                ConvertSecurityDescriptorToStringSecurityDescriptorW, GetSecurityInfo,
                SE_KERNEL_OBJECT,
            },
            DACL_SECURITY_INFORMATION,
        },
    };

    let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
    let status = unsafe {
        GetSecurityInfo(
            handle,
            SE_KERNEL_OBJECT,
            DACL_SECURITY_INFORMATION,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut descriptor,
        )
    };
    if status != ERROR_SUCCESS {
        return Err(SecurityError::new(format!(
            "GetSecurityInfo failed with {status}"
        )));
    }
    let mut raw = ptr::null_mut();
    let converted = unsafe {
        ConvertSecurityDescriptorToStringSecurityDescriptorW(
            descriptor,
            SDDL_REVISION_1,
            DACL_SECURITY_INFORMATION,
            &mut raw,
            ptr::null_mut(),
        )
    } != 0;
    let sddl = if converted {
        let text = unsafe { wide_to_string(raw) };
        unsafe { LocalFree(raw as HLOCAL) };
        Ok(text)
    } else {
        Err(SecurityError::last(
            "ConvertSecurityDescriptorToStringSecurityDescriptorW",
        ))
    };
    unsafe { LocalFree(descriptor as HLOCAL) };
    sddl
}

/// One access control entry of a discretionary access control list.
#[cfg(test)]
#[derive(Debug, Clone)]
pub struct DaclEntry {
    /// The rights the entry grants, already mapped from generic to object-specific by Windows.
    pub mask: u32,
    pub sid: Sid,
}

/// The discretionary access control list an object really carries, decoded into principals.
///
/// The textual SDDL form is *not* a reliable thing to assert against: Windows renders well-known
/// security identifiers using their two-letter aliases rather than the literal `S-1-…` string it
/// was given. A descriptor built from the current user's own SID therefore comes back as `LA` on a
/// machine where that user happens to be the built-in local `Administrator` account (relative
/// identifier 500) — which is exactly what a GitHub-hosted Windows runner is. The SID bytes are
/// unchanged; only their spelling is. So the list is compared by SID, with [`EqualSid`], the same
/// way peer identity is.
#[cfg(test)]
#[derive(Debug, Clone)]
pub struct Dacl {
    /// Whether `SE_DACL_PROTECTED` is set, which is what `D:P` asks for.
    pub protected: bool,
    pub entries: Vec<DaclEntry>,
}

/// Read back the access control list an already-created kernel object actually carries, decoded.
///
/// Exists for tests: a list that was *asked for* is not evidence, and this asks the object itself.
/// A missing or `NULL` list is an error rather than an empty result — a `NULL` DACL grants every
/// principal full access, so reporting it as "no entries" would turn the worst case into a pass.
///
/// # Safety
///
/// `handle` must be an open kernel object handle carrying `READ_CONTROL`.
#[cfg(test)]
pub unsafe fn object_dacl(handle: HANDLE) -> Result<Dacl, SecurityError> {
    use windows_sys::Win32::{
        Foundation::ERROR_SUCCESS,
        Security::{
            ACCESS_ALLOWED_ACE, ACE_HEADER, ACL,
            Authorization::{GetSecurityInfo, SE_KERNEL_OBJECT},
            DACL_SECURITY_INFORMATION, GetAce, GetSecurityDescriptorControl,
            GetSecurityDescriptorDacl, SE_DACL_PROTECTED,
        },
    };

    /// `ACCESS_ALLOWED_ACE_TYPE`. Not exported by `windows-sys`; it is zero by definition.
    const ACCESS_ALLOWED_ACE_TYPE: u8 = 0;

    let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
    let status = unsafe {
        GetSecurityInfo(
            handle,
            SE_KERNEL_OBJECT,
            DACL_SECURITY_INFORMATION,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut descriptor,
        )
    };
    if status != ERROR_SUCCESS {
        return Err(SecurityError::new(format!(
            "GetSecurityInfo failed with {status}"
        )));
    }
    // Decoded in one closure so the descriptor is released on every path, including the failing
    // ones.
    let decoded = (|| {
        let mut control = 0u16;
        let mut revision = 0u32;
        if unsafe { GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) } == 0 {
            return Err(SecurityError::last("GetSecurityDescriptorControl"));
        }
        let mut present = 0;
        let mut acl: *mut ACL = ptr::null_mut();
        let mut defaulted = 0;
        if unsafe { GetSecurityDescriptorDacl(descriptor, &mut present, &mut acl, &mut defaulted) }
            == 0
        {
            return Err(SecurityError::last("GetSecurityDescriptorDacl"));
        }
        if present == 0 || acl.is_null() {
            return Err(SecurityError::new(
                "the object carries no discretionary access control list, which grants every principal full access",
            ));
        }
        let count = unsafe { (*acl).AceCount };
        let mut entries = Vec::with_capacity(count as usize);
        for index in 0..u32::from(count) {
            let mut ace: *mut c_void = ptr::null_mut();
            if unsafe { GetAce(acl, index, &mut ace) } == 0 {
                return Err(SecurityError::last("GetAce"));
            }
            // SAFETY: `GetAce` handed back a pointer to an entry inside the list it owns, which
            // begins with an `ACE_HEADER` whatever its type.
            let header = unsafe { &*ace.cast::<ACE_HEADER>() };
            if header.AceType != ACCESS_ALLOWED_ACE_TYPE {
                return Err(SecurityError::new(format!(
                    "entry {index} is of access control entry type {}, and only allow entries are expected",
                    header.AceType
                )));
            }
            // SAFETY: the type check above establishes the layout, and the security identifier of
            // an allow entry begins at its `SidStart` field.
            let allowed = unsafe { &*ace.cast::<ACCESS_ALLOWED_ACE>() };
            let sid = unsafe { Sid::from_raw(ptr::from_ref(&allowed.SidStart) as PSID) }?;
            entries.push(DaclEntry {
                mask: allowed.Mask,
                sid,
            });
        }
        Ok(Dacl {
            protected: control & SE_DACL_PROTECTED != 0,
            entries,
        })
    })();
    unsafe { LocalFree(descriptor as HLOCAL) };
    decoded
}

/// A security identifier parsed from its literal `S-1-…` form.
///
/// Literal forms only, deliberately: comparing against `S-1-5-18` rather than the `SY` alias is
/// what makes these checks independent of how Windows chooses to *spell* a principal.
#[cfg(test)]
pub fn sid_from_sddl(text: &str) -> Result<Sid, SecurityError> {
    use windows_sys::Win32::Security::Authorization::ConvertStringSidToSidW;

    let wide = to_wide(text);
    let mut raw: PSID = ptr::null_mut();
    if unsafe { ConvertStringSidToSidW(wide.as_ptr(), &mut raw) } == 0 {
        return Err(SecurityError::last("ConvertStringSidToSidW"));
    }
    // SAFETY: the call succeeded, so `raw` points at a valid SID this scope now owns.
    let sid = unsafe { Sid::from_raw(raw) };
    unsafe { LocalFree(raw as HLOCAL) };
    sid
}

/// The one definition of what the daemon endpoint's access control list has to be.
///
/// Shared by the descriptor test and the live-listener test so the two can never disagree about the
/// invariant, and expressed as principals rather than as text. Returns the reason on failure so a
/// caller can report which part of the invariant broke.
#[cfg(test)]
pub fn check_endpoint_dacl(dacl: &Dacl, owner: &Sid) -> Result<(), String> {
    /// `FILE_ALL_ACCESS`: what `GENERIC_ALL` maps to once a descriptor is attached to a pipe.
    const FILE_ALL_ACCESS: u32 = 0x001F_01FF;

    let named = |literal: &str| {
        sid_from_sddl(literal).map_err(|error| format!("could not parse {literal}: {error}"))
    };
    let system = named("S-1-5-18")?;
    let everyone = named("S-1-1-0")?;
    let anonymous = named("S-1-5-7")?;
    let administrators = named("S-1-5-32-544")?;

    if !dacl.protected {
        return Err("the list must be protected from inheritance".to_owned());
    }
    let granted = |who: &Sid| {
        dacl.entries
            .iter()
            .find(|entry| &entry.sid == who)
            .map(|entry| entry.mask)
    };
    match granted(owner) {
        Some(FILE_ALL_ACCESS) => {}
        Some(mask) => {
            return Err(format!(
                "the owning user is granted {mask:#010x}, not full access ({FILE_ALL_ACCESS:#010x})"
            ));
        }
        None => return Err(format!("the owning user {owner:?} is not granted access")),
    }
    match granted(&system) {
        Some(FILE_ALL_ACCESS) => {}
        Some(mask) => {
            return Err(format!(
                "SYSTEM is granted {mask:#010x}, not full access ({FILE_ALL_ACCESS:#010x})"
            ));
        }
        None => return Err("SYSTEM is not granted access".to_owned()),
    }
    for (label, who) in [
        ("Everyone", &everyone),
        ("ANONYMOUS LOGON", &anonymous),
        ("BUILTIN\\Administrators", &administrators),
    ] {
        if granted(who).is_some() {
            return Err(format!("{label} must never be granted access"));
        }
    }
    // Anything beyond those two principals would be an unrelated account gaining control, so the
    // count is part of the invariant rather than a detail.
    if dacl.entries.len() != 2 {
        return Err(format!(
            "the list must name exactly this user and SYSTEM, but it has {} entries: {:?}",
            dacl.entries.len(),
            dacl.entries
                .iter()
                .map(|entry| format!("{:?}", entry.sid))
                .collect::<Vec<_>>()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{PipeSecurity, Sid, current_user_sid};

    #[test]
    fn the_current_user_sid_is_readable_and_compares_equal_to_itself() {
        let user = current_user_sid().expect("this process has a user SID");
        let again = current_user_sid().expect("this process has a user SID");

        assert_eq!(user, again);
        let sddl = user.to_sddl().expect("a SID renders as SDDL");
        assert!(sddl.starts_with("S-1-"), "{sddl}");
    }

    /// The whole point of building a descriptor by hand: the two principals Windows would have
    /// granted by default must not be in it.
    #[test]
    fn the_pipe_access_control_list_names_only_this_user_and_system() {
        let user = current_user_sid().expect("this process has a user SID");
        let security = PipeSecurity::for_current_user().expect("a descriptor should be built");

        let sddl = security.sddl();
        assert!(
            sddl.contains(&user.to_sddl().expect("a SID renders as SDDL")),
            "the owning user must be granted access: {sddl}"
        );
        assert!(sddl.contains("(A;;GA;;;SY)"), "{sddl}");
        assert!(
            !sddl.contains(";WD)"),
            "Everyone must never be granted access: {sddl}"
        );
        assert!(
            !sddl.contains(";AN)"),
            "ANONYMOUS LOGON must never be granted access: {sddl}"
        );
        assert!(
            sddl.starts_with("D:P"),
            "the list must be protected from inheritance: {sddl}"
        );
    }

    #[test]
    fn two_different_sids_are_not_equal() {
        let user = current_user_sid().expect("this process has a user SID");
        let mut other = user.clone();
        // Flip the last sub-authority byte: still a well-formed SID, a different principal.
        let last = other.bytes.len() - 1;
        other.bytes[last] = other.bytes[last].wrapping_add(1);

        assert_ne!(user, other);
    }

    /// End to end against real kernel objects: the pipe carries the list that was asked for, and
    /// both ends recognise each other. Same-user only is all one process can prove directly — the
    /// refusal of a *different* user is the same `EqualSid` comparison with the other answer, which
    /// [`two_different_sids_are_not_equal`] covers.
    #[tokio::test]
    async fn a_created_pipe_carries_the_restricted_list_and_both_ends_verify_each_other() {
        use std::os::windows::io::AsRawHandle;

        use tokio::net::windows::named_pipe::{ClientOptions, ServerOptions};

        use super::{
            check_endpoint_dacl, object_dacl, object_dacl_sddl, verify_pipe_client,
            verify_pipe_server,
        };

        let address = format!(
            "\\\\.\\pipe\\lya-security-probe-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        );
        let user = current_user_sid().expect("this process has a user SID");
        let security = PipeSecurity::for_current_user().expect("a descriptor should be built");
        let server = unsafe {
            ServerOptions::new()
                .first_pipe_instance(true)
                .reject_remote_clients(true)
                .create_with_security_attributes_raw(&address, security.attributes())
        }
        .expect("the pipe should be created");

        // What the object actually ended up with, asked of the object, and compared by security
        // identifier rather than by the text Windows chooses to render one as.
        let live =
            unsafe { object_dacl(server.as_raw_handle() as _) }.expect("the DACL should be read");
        let sddl = unsafe { object_dacl_sddl(server.as_raw_handle() as _) }
            .expect("the DACL should render");
        // Printed so a CI log shows which principal each rendered alias actually is.
        println!("live DACL: {sddl}");
        println!("owning user: {user:?}");
        for entry in &live.entries {
            println!("  entry {:?} mask {:#010x}", entry.sid, entry.mask);
        }
        check_endpoint_dacl(&live, &user).unwrap_or_else(|reason| {
            panic!("the live pipe's list is wrong: {reason}\nSDDL: {sddl}")
        });

        let client = ClientOptions::new()
            .open(&address)
            .expect("this user may open its own pipe");
        server.connect().await.expect("the client should arrive");

        let client_proof = unsafe { verify_pipe_client(server.as_raw_handle() as _, &user) }
            .expect("a client of the same user is accepted");
        let server_proof = unsafe { verify_pipe_server(client.as_raw_handle() as _, &user) }
            .expect("a server of the same user is accepted");
        println!("client proof: {client_proof:?}, server proof: {server_proof:?}");
    }

    /// What a squatter actually runs into.
    ///
    /// A pipe owned by another local account is, to these functions, a peer whose token user SID is
    /// not the expected one — the pipe, the name and the protocol are otherwise identical. Both
    /// checks are driven by that comparison, so pointing them at a real connection with the wrong
    /// expectation reproduces the refusal exactly, without needing a second account.
    #[tokio::test]
    async fn a_peer_that_is_not_the_expected_user_is_refused_on_both_ends() {
        use std::os::windows::io::AsRawHandle;

        use tokio::net::windows::named_pipe::{ClientOptions, ServerOptions};

        use super::{verify_pipe_client, verify_pipe_server};

        let address = format!(
            r"\\.\pipe\lya-security-stranger-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        );
        let user = current_user_sid().expect("this process has a user SID");
        let mut stranger = user.clone();
        let last = stranger.bytes.len() - 1;
        stranger.bytes[last] = stranger.bytes[last].wrapping_add(1);
        assert_ne!(user, stranger, "the stranger is a different principal");

        let security = PipeSecurity::for_current_user().expect("a descriptor should be built");
        let server = unsafe {
            ServerOptions::new()
                .first_pipe_instance(true)
                .reject_remote_clients(true)
                .create_with_security_attributes_raw(&address, security.attributes())
        }
        .expect("the pipe should be created");
        let client = ClientOptions::new()
            .open(&address)
            .expect("this user may open its own pipe");
        server.connect().await.expect("the client should arrive");

        let refused_client = unsafe { verify_pipe_client(server.as_raw_handle() as _, &stranger) }
            .expect_err("a client that is not the expected user is refused");
        let refused_server = unsafe { verify_pipe_server(client.as_raw_handle() as _, &stranger) }
            .expect_err("a server that is not the expected user is refused");

        assert!(
            refused_client
                .to_string()
                .contains("refusing the connection"),
            "{refused_client}"
        );
        assert!(
            refused_server
                .to_string()
                .contains("refusing the connection"),
            "{refused_server}"
        );
    }

    /// Every failure on the way to a token is a refusal, never a pass.
    #[test]
    fn a_peer_whose_identity_cannot_be_established_is_refused() {
        use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;

        use super::{verify_pipe_client, verify_pipe_server};

        let user = current_user_sid().expect("this process has a user SID");

        assert!(
            unsafe { verify_pipe_client(INVALID_HANDLE_VALUE, &user) }.is_err(),
            "a handle that names no client must never verify"
        );
        assert!(
            unsafe { verify_pipe_server(INVALID_HANDLE_VALUE, &user) }.is_err(),
            "a handle that names no server must never verify"
        );
        assert!(
            super::process_user_sid(0).is_err(),
            "a peer that reports no process must never verify"
        );
    }

    #[test]
    fn a_sid_survives_being_moved() {
        let user = current_user_sid().expect("this process has a user SID");
        let moved = Sid {
            bytes: user.bytes.clone(),
        };

        assert_eq!(user, moved);
    }
}
