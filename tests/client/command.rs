use bson;
use bson::Bson;
use mongodb::client::command::Command;
use mongodb::client::command::DatabaseCommand::{IsMaster, ListDatabases};
use mongodb::client::wire_protocol::flags::{OpInsertFlags, OpQueryFlags};
use mongodb::client::wire_protocol::operations::Message;
use std::io::Write;
use std::net::TcpStream;

#[test]
fn drop_database() {
    match TcpStream::connect("localhost:27017") {
        Ok(mut stream) => {
            let mut doc = bson::Document::new();
            doc.insert("foo".to_owned(), Bson::FloatingPoint(42.0));
            let docs = vec![doc];
            let flags = OpInsertFlags::no_flags();
            let name = "test.test".to_owned();
            let res = Message::with_insert(1, flags, name, docs);

            let cm = match res {
                Ok(message) => message,
                Err(_) => panic!("Could not create message!")
            };

            match cm.write(&mut stream) {
                Ok(_) => (),
                Err(s) => panic!("{}", s)
            };

            let cmd = Command::new_drop_database(1, "test");

            let docs = match cmd.run(&mut stream) {
                Ok(d) => d,
                Err(e) => panic!("{}", e)
            };

            assert_eq!(docs.len() as i32, 1);

            match docs[0].get("ok") {
                Some(&Bson::FloatingPoint(1.0)) => (),
                _ => panic!("Database unable to be dropped")
            };

            let doc = bson::Document::new();
            let flags = OpQueryFlags::no_flags();
            let name = "test.test".to_owned();
            let res = Message::with_query(1, flags, name, 0, 0, doc, None);

            let cm = match res {
                Ok(message) => message,
                Err(s) => panic!("{}", s)
            };

            match cm.write(&mut stream) {
                Ok(_) => (),
                Err(s) => panic!("{}", s)
            };

            let reply = match Message::read(&mut stream) {
                Ok(m) => m,
                Err(s) => panic!("{}", s)
            };

            let docs = match reply {
                Message::OpReply { header: _, flags: _, cursor_id:_,
                                   starting_from: _, number_returned: _,
                                   documents: d } => d,
                _ => panic!("Invalid response read from server")
            };

            assert_eq!(docs.len() as i32, 0);
        },
        Err(_) => {
            panic!("Could not connect to server")
        }
    }
}

#[test]
fn is_master() {
    match TcpStream::connect("localhost:27017") {
        Ok(mut stream) => {
            let cmd = Command::new(1, IsMaster);

            let docs = match cmd.run(&mut stream) {
                Ok(d) => d,
                Err(e) => panic!("{}", e)
            };

            assert_eq!(docs.len() as i32, 1);

            match docs[0].get("ismaster") {
                Some(&Bson::Boolean(true)) => (),
                _ => panic!("Invalid isMaster response")
            };
        },
        Err(_) => {
            panic!("Could not connect to server")
        }
    }
}

#[test]
fn list_databases() {
    match TcpStream::connect("localhost:27017") {
        Ok(mut stream) => {
             let cmd = Command::new(1, ListDatabases);

            let docs = match cmd.run(&mut stream) {
                Ok(docs) => docs,
                Err(e) => panic!("{}", e)
            };

            assert_eq!(docs.len() as i32, 1);

            let databases = match docs[0].get("databases") {
                Some(&Bson::Array(ref dbs)) => dbs,
                _ => panic!("Invalid listDatabases response")
            };

            let bson = match databases[0] {
                Bson::Document(ref d) => d,
                _ => panic!("Invalid array returned by listDatabases")
            };

            match bson.get("name") {
                Some(&Bson::String(ref s)) => assert_eq!(s, "local"),
                _ => panic!("Local database not found")
            };
        },
        Err(_) => {
            panic!("Could not connect to server")
        }
    }
}
