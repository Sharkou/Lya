use std::{error::Error, fmt, fs, io::ErrorKind};

use super::home::LyaHome;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateContext {
    content: String,
}

impl PrivateContext {
    pub fn load(home: &LyaHome) -> Result<Option<Self>, ContextError> {
        match fs::read(home.context_path()) {
            Ok(content) => String::from_utf8(content)
                .map(|content| Some(Self { content }))
                .map_err(|error| ContextError::InvalidUtf8(error.to_string())),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(ContextError::Read(error.to_string())),
        }
    }

    pub fn content(&self) -> &str {
        &self.content
    }
}

#[derive(Debug)]
pub enum ContextError {
    Read(String),
    InvalidUtf8(String),
}

impl fmt::Display for ContextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(error) => write!(formatter, "could not read private context: {error}"),
            Self::InvalidUtf8(error) => {
                write!(formatter, "private context is not valid UTF-8: {error}")
            }
        }
    }
}

impl Error for ContextError {}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::PrivateContext;
    use crate::orchestrator::home::LyaHome;

    static NEXT_TEMP_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    fn home() -> std::path::PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "lya-context-test-{}-{}",
            std::process::id(),
            NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).expect("home directory should be created");
        directory
    }

    #[test]
    fn loads_present_utf8_context() {
        let directory = home();
        fs::write(directory.join("context.md"), "private notes")
            .expect("context should be written");

        let context = PrivateContext::load(&LyaHome::from_path(&directory))
            .expect("context should load")
            .expect("context should be present");

        assert_eq!(context.content(), "private notes");
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn reports_missing_context_without_creating_it() {
        let directory = home();

        let context = PrivateContext::load(&LyaHome::from_path(&directory))
            .expect("missing context should not be an error");

        assert!(context.is_none());
        assert!(!directory.join("context.md").exists());
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn reports_invalid_utf8_context() {
        let directory = home();
        fs::write(directory.join("context.md"), [0xff, 0xfe]).expect("context should be written");

        let error = PrivateContext::load(&LyaHome::from_path(&directory))
            .expect_err("invalid UTF-8 should be rejected");

        assert!(error.to_string().contains("not valid UTF-8"));
        fs::remove_dir_all(directory).expect("home should be removed");
    }
}
