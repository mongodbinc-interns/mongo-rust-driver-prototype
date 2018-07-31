use std::fmt::{Display, Error, Formatter};

use bson::Document;
use error::Error as MongoError;
use separator::Separatable;

/// Contains the information about a given command that started.
#[derive(Debug, Clone, PartialEq)]
pub struct CommandStarted {
    pub command: Document,
    pub database_name: String,
    pub command_name: String,
    pub request_id: i64,
    pub connection_string: String,
}

impl Display for CommandStarted {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        write!(
            fmt,
            "COMMAND.{} {} STARTED: {}",
            self.command_name,
            self.connection_string,
            self.command
        )
    }
}

/// Contains the information about a given command that completed.
#[derive(Debug, Clone)]
pub enum CommandResult<'a> {
    Success {
        duration: u64,
        reply: Document,
        command_name: String,
        request_id: i64,
        connection_string: String,
    },
    Failure {
        duration: u64,
        command_name: String,
        failure: &'a MongoError,
        request_id: i64,
        connection_string: String,
    },
}

impl<'a> Display for CommandResult<'a> {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), Error> {
        match *self {
            CommandResult::Success {
                duration,
                ref reply,
                ref command_name,
                ref connection_string,
                ..
            } => {
                write!(
                    fmt,
                    "COMMAND.{} {} COMPLETED: {} ({} ns)",
                    command_name,
                    connection_string,
                    reply,
                    duration.separated_string()
                )
            }
            CommandResult::Failure {
                duration,
                ref command_name,
                failure,
                ref connection_string,
                ..
            } => {
                write!(
                    fmt,
                    "COMMAND.{} {} FAILURE: {} ({} ns)",
                    command_name,
                    connection_string,
                    failure,
                    duration.separated_string()
                )
            }
        }
    }
}
