use std::{env, path::PathBuf};

use super::{context::PrivateContext, home::LyaHome};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoctorReport {
    pub home: PathBuf,
    pub context: ContextCheck,
    pub programs: Vec<ProgramCheck>,
}

impl DoctorReport {
    pub fn inspect(home: &LyaHome) -> Self {
        Self::inspect_with_lookup(home, find_program)
    }

    pub fn inspect_with_lookup(
        home: &LyaHome,
        find_program: impl Fn(&str) -> Option<PathBuf>,
    ) -> Self {
        let context = match PrivateContext::load(home) {
            Ok(Some(_)) => ContextCheck::Available,
            Ok(None) => ContextCheck::Missing,
            Err(error) => ContextCheck::Unreadable(error.to_string()),
        };
        let programs = ["git", "codex", "claude"]
            .into_iter()
            .map(|name| ProgramCheck {
                name: name.to_owned(),
                path: find_program(name),
            })
            .collect();

        Self {
            home: home.path().to_owned(),
            context,
            programs,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.context == ContextCheck::Available
            && self.programs.iter().all(|program| program.path.is_some())
    }

    pub fn render(&self) -> String {
        let mut lines = vec![
            "Lya doctor".to_owned(),
            String::new(),
            format!("{:<14} OK  {}", "LYA_HOME", self.home.display()),
            match &self.context {
                ContextCheck::Available => format!("{:<14} OK", "context.md"),
                ContextCheck::Missing => format!("{:<14} MISSING", "context.md"),
                ContextCheck::Unreadable(error) => format!("{:<14} ERROR  {error}", "context.md"),
            },
        ];
        lines.extend(self.programs.iter().map(|program| match &program.path {
            Some(path) => format!("{:<14} OK  {}", program.name, path.display()),
            None => format!("{:<14} MISSING", program.name),
        }));
        lines.push(String::new());
        lines.push(
            if self.is_ready() {
                "Ready for orchestration."
            } else {
                "Not ready for orchestration."
            }
            .to_owned(),
        );
        lines.join("\n")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContextCheck {
    Available,
    Missing,
    Unreadable(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgramCheck {
    pub name: String,
    pub path: Option<PathBuf>,
}

fn find_program(program: &str) -> Option<PathBuf> {
    let extensions = executable_extensions();
    let path = env::var_os("PATH")?;
    env::split_paths(&path).find_map(|directory| {
        let direct = directory.join(program);
        if direct.is_file() {
            return Some(direct);
        }
        extensions
            .iter()
            .map(|extension| directory.join(format!("{program}{extension}")))
            .find(|candidate| candidate.is_file())
    })
}

#[cfg(windows)]
fn executable_extensions() -> Vec<String> {
    env::var("PATHEXT")
        .unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD".to_owned())
        .split(';')
        .filter(|extension| !extension.is_empty())
        .map(str::to_owned)
        .collect()
}

#[cfg(not(windows))]
fn executable_extensions() -> Vec<String> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::{ContextCheck, DoctorReport};
    use crate::orchestrator::home::LyaHome;

    static NEXT_TEMP_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    fn home() -> PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "lya-doctor-test-{}-{}",
            std::process::id(),
            NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).expect("home directory should be created");
        directory
    }

    #[test]
    fn ready_report_uses_injected_program_lookup() {
        let directory = home();
        fs::write(directory.join("context.md"), "private context")
            .expect("context should be written");
        let report =
            DoctorReport::inspect_with_lookup(&LyaHome::from_path(&directory), |program| {
                Some(PathBuf::from(format!("C:/tools/{program}.exe")))
            });

        assert!(report.is_ready());
        assert_eq!(report.context, ContextCheck::Available);
        assert!(report.render().contains("Ready for orchestration."));
        fs::remove_dir_all(directory).expect("home should be removed");
    }

    #[test]
    fn missing_requirements_make_report_not_ready() {
        let directory = home();
        let report =
            DoctorReport::inspect_with_lookup(&LyaHome::from_path(&directory), |program| {
                (program == "git").then(|| PathBuf::from("C:/tools/git.exe"))
            });

        assert!(!report.is_ready());
        assert_eq!(report.context, ContextCheck::Missing);
        assert!(report.render().contains("claude         MISSING"));
        fs::remove_dir_all(directory).expect("home should be removed");
    }
}
