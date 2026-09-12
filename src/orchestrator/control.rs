use std::{
    collections::VecDeque,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::sync::{Mutex, mpsc};

use crate::process::ProcessCancellation;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlCommand {
    Pause,
    Resume,
    Stop,
    Status,
    Diff,
    Send(String),
}

pub fn parse_control_command(input: &str) -> Result<Option<ControlCommand>, ControlParseError> {
    if input.trim().is_empty() {
        return Ok(None);
    }
    if !input.starts_with('/') {
        return Err(ControlParseError::InstructionMustUseSend);
    }
    let (command, argument) = input.split_once(char::is_whitespace).unwrap_or((input, ""));
    match command {
        "/help" if argument.trim().is_empty() => Ok(None),
        "/pause" if argument.trim().is_empty() => Ok(Some(ControlCommand::Pause)),
        "/resume" if argument.trim().is_empty() => Ok(Some(ControlCommand::Resume)),
        "/stop" if argument.trim().is_empty() => Ok(Some(ControlCommand::Stop)),
        "/status" if argument.trim().is_empty() => Ok(Some(ControlCommand::Status)),
        "/diff" if argument.trim().is_empty() => Ok(Some(ControlCommand::Diff)),
        "/send" if !argument.trim().is_empty() => {
            Ok(Some(ControlCommand::Send(argument.to_owned())))
        }
        "/send" => Err(ControlParseError::MissingInstruction),
        command
            if matches!(
                command,
                "/help" | "/pause" | "/resume" | "/stop" | "/status" | "/diff"
            ) =>
        {
            Err(ControlParseError::UnexpectedArgument(command.to_owned()))
        }
        command => Err(ControlParseError::UnknownCommand(command.to_owned())),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlParseError {
    InstructionMustUseSend,
    MissingInstruction,
    UnexpectedArgument(String),
    UnknownCommand(String),
}

impl fmt::Display for ControlParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InstructionMustUseSend => {
                formatter.write_str("instructions must use /send <instruction>")
            }
            Self::MissingInstruction => formatter.write_str("/send requires an instruction"),
            Self::UnexpectedArgument(command) => {
                write!(formatter, "{command} does not accept an argument")
            }
            Self::UnknownCommand(command) => {
                write!(formatter, "unknown command: {command} (use /help)")
            }
        }
    }
}

#[derive(Debug, Default)]
struct ControlFlags {
    pause_requested: AtomicBool,
    stop_requested: AtomicBool,
}

#[derive(Clone)]
pub struct ControlSender {
    sender: mpsc::UnboundedSender<ControlCommand>,
    flags: Arc<ControlFlags>,
    cancellation: ProcessCancellation,
}

impl ControlSender {
    pub fn send(&self, command: ControlCommand) -> Result<(), ControlCommand> {
        match command {
            ControlCommand::Pause => self.flags.pause_requested.store(true, Ordering::Release),
            ControlCommand::Resume => self.flags.pause_requested.store(false, Ordering::Release),
            ControlCommand::Stop => {
                self.flags.stop_requested.store(true, Ordering::Release);
                self.cancellation.cancel();
            }
            ControlCommand::Status | ControlCommand::Diff | ControlCommand::Send(_) => {}
        }
        self.sender.send(command).map_err(|error| error.0)
    }

    pub fn request_stop(&self) {
        let _ = self.send(ControlCommand::Stop);
    }
}

pub struct ControlReceiver {
    receiver: Mutex<mpsc::UnboundedReceiver<ControlCommand>>,
    flags: Arc<ControlFlags>,
    cancellation: ProcessCancellation,
}

impl ControlReceiver {
    pub fn new() -> (ControlSender, Self) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let flags = Arc::new(ControlFlags::default());
        let cancellation = ProcessCancellation::new();
        (
            ControlSender {
                sender,
                flags: flags.clone(),
                cancellation: cancellation.clone(),
            },
            Self {
                receiver: Mutex::new(receiver),
                flags,
                cancellation,
            },
        )
    }

    pub fn disabled() -> Self {
        let (_sender, receiver) = Self::new();
        receiver
    }

    pub async fn drain(&self) -> Vec<ControlCommand> {
        let mut receiver = self.receiver.lock().await;
        let mut commands = VecDeque::new();
        while let Ok(command) = receiver.try_recv() {
            commands.push_back(command);
        }
        commands.into()
    }

    pub async fn next(&self) -> Option<ControlCommand> {
        self.receiver.lock().await.recv().await
    }

    pub fn pause_requested(&self) -> bool {
        self.flags.pause_requested.load(Ordering::Acquire)
    }

    pub fn stop_requested(&self) -> bool {
        self.flags.stop_requested.load(Ordering::Acquire)
    }

    pub fn cancellation(&self) -> ProcessCancellation {
        self.cancellation.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::{ControlCommand, ControlParseError, parse_control_command};

    #[test]
    fn parses_commands_and_preserves_send_text() {
        assert_eq!(
            parse_control_command("/pause"),
            Ok(Some(ControlCommand::Pause))
        );
        assert_eq!(
            parse_control_command("/send Preserve  every  space  "),
            Ok(Some(ControlCommand::Send(
                "Preserve  every  space  ".to_owned()
            )))
        );
        assert_eq!(parse_control_command("   "), Ok(None));
    }

    #[test]
    fn rejects_ambiguous_or_invalid_commands() {
        assert_eq!(
            parse_control_command("plain text"),
            Err(ControlParseError::InstructionMustUseSend)
        );
        assert_eq!(
            parse_control_command("/send"),
            Err(ControlParseError::MissingInstruction)
        );
        assert_eq!(
            parse_control_command("/unknown"),
            Err(ControlParseError::UnknownCommand("/unknown".to_owned()))
        );
    }
}
