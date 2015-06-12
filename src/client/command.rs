use bson::Document as BsonDocument;
use bson::Bson::I32;
use client::wire_protocol::flags::OpQueryFlags;
use client::wire_protocol::operations::Message;
use std::io::{Read, Write};

pub enum DatabaseCommand {
    IsMaster,
    ListDatabases,
}

pub struct Command {
    request_id: i32,
    command: DatabaseCommand,
}

impl DatabaseCommand {
    fn is_admin(&self) -> bool {
        match self {
            &DatabaseCommand::IsMaster => false,
            &DatabaseCommand::ListDatabases => true
        }
    }
}

impl Command {
    fn get_database_string(&self) -> String {
        let string = if self.command.is_admin() {
            "admin"
        } else {
            "local"
        };

        string.to_owned()
    }
}

impl ToString for DatabaseCommand {
    fn to_string(&self) -> String {
        let string = match self {
            &DatabaseCommand::IsMaster => "isMaster",
            &DatabaseCommand::ListDatabases => "listDatabases"
        };

        string.to_owned()
    }
}

impl Command {
    pub fn new(request_id: i32, command: DatabaseCommand) -> Command {
        Command { request_id: request_id, command: command }
    }

    // This function will eventually be merged into Client as methods, at which
    // time it will be rewritten to use the Client methods instead of directly
    // using the write protocol.
    pub fn run<T: Read + Write>(&self, buffer: &mut T) -> Result<Vec<BsonDocument>, String> {
        let flags = OpQueryFlags::no_flags();
        let full_collection_name = format!("{}.$cmd", self.get_database_string());
        let command = self.command.to_string();

        let mut bson = BsonDocument::new();
        bson.insert(command.clone(), I32(1));

        let message_result = Message::with_query(self.request_id, flags,
                                                 full_collection_name, 0, 1,
                                                 bson, None);
        let message = match message_result {
            Ok(m) => m,
            Err(e) => {
                let s = format!("Unable to run command {}: {}", command, e);
                return Err(s)
            }
        };

        match message.write(buffer) {
            Ok(_) => (),
            Err(e) => {
                let s = format!("Unable to run command {}: {}", command, e);
                return Err(s)
            }
        };

        match Message::read(buffer) {
            Ok(Message::OpReply { header: _, flags: _, cursor_id: _,
                               starting_from: _, number_returned: _,
                               documents: d }) => Ok(d),
            Err(s) => Err(s),
            _ => Err("Invalid response received from server".to_owned())
        }
    }
}
