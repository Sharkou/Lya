use std::{
    env,
    error::Error,
    ffi::OsString,
    fmt,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LyaHome {
    path: PathBuf,
}

impl LyaHome {
    pub fn resolve() -> Result<Self, HomeError> {
        Self::resolve_with_environment(|name| env::var_os(name))
    }

    pub fn from_path(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn context_path(&self) -> PathBuf {
        self.path.join("context.md")
    }

    pub fn state_path(&self) -> PathBuf {
        self.path.join("state.json")
    }

    fn resolve_with_environment(
        environment: impl Fn(&str) -> Option<OsString>,
    ) -> Result<Self, HomeError> {
        if let Some(path) = non_empty(environment("LYA_HOME")) {
            return Ok(Self::from_path(path));
        }

        let home =
            default_home_directory(&environment).ok_or(HomeError::HomeDirectoryUnavailable)?;
        Ok(Self::from_path(home.join(".lya")))
    }
}

fn non_empty(value: Option<OsString>) -> Option<OsString> {
    value.filter(|value| !value.is_empty())
}

fn default_home_directory(environment: &impl Fn(&str) -> Option<OsString>) -> Option<PathBuf> {
    #[cfg(windows)]
    {
        if let Some(home) = non_empty(environment("USERPROFILE")) {
            return Some(PathBuf::from(home));
        }
        if let (Some(drive), Some(path)) = (
            non_empty(environment("HOMEDRIVE")),
            non_empty(environment("HOMEPATH")),
        ) {
            return Some(PathBuf::from(drive).join(path));
        }
    }

    if let Some(home) = non_empty(environment("HOME")) {
        return Some(PathBuf::from(home));
    }

    None
}

#[derive(Debug)]
pub enum HomeError {
    HomeDirectoryUnavailable,
}

impl fmt::Display for HomeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HomeDirectoryUnavailable => {
                formatter.write_str("could not determine a home directory; set LYA_HOME explicitly")
            }
        }
    }
}

impl Error for HomeError {}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

    use super::LyaHome;

    #[test]
    fn lya_home_environment_override_takes_precedence() {
        let environment = BTreeMap::from([
            ("LYA_HOME", OsString::from("D:/private/lya")),
            ("HOME", OsString::from("D:/home")),
        ]);

        let home = LyaHome::resolve_with_environment(|name| environment.get(name).cloned())
            .expect("LYA_HOME should resolve");

        assert_eq!(home.path(), PathBuf::from("D:/private/lya"));
    }

    #[test]
    fn resolves_default_path_from_home_directory() {
        #[cfg(windows)]
        let environment = BTreeMap::from([("USERPROFILE", OsString::from("D:/home"))]);
        #[cfg(not(windows))]
        let environment = BTreeMap::from([("HOME", OsString::from("D:/home"))]);

        let home = LyaHome::resolve_with_environment(|name| environment.get(name).cloned())
            .expect("default LYA_HOME should resolve");

        assert_eq!(home.path(), PathBuf::from("D:/home/.lya"));
    }
}
