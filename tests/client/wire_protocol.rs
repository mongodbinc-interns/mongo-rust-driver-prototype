use bson;
use bson::Bson;
use mongodb::client::command::Command;
use mongodb::client::wire_protocol::flags::{OpInsertFlags, OpQueryFlags};
use mongodb::client::wire_protocol::operations::Message;
use std::io::Write;
use std::net::TcpStream;

#[test]
fn insert_single_key_doc() {
    match TcpStream::connect("localhost:27017") {
        Ok(mut stream) => {
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

            assert_eq!(docs.len() as i32, 1);

            match docs[0].get("foo") {
                Some(&Bson::FloatingPoint(42.0)) => (),
                _ => panic!("Wrong value returned!")
            };
        },
        Err(_) => {
            panic!("Could not connect to server")
        }
    }
}

#[test]
fn insert_multi_key_doc() {
    match TcpStream::connect("localhost:27017") {
        Ok(mut stream) => {
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

            let mut doc = bson::Document::new();
            doc.insert("foo".to_owned(), Bson::FloatingPoint(42.0));
            doc.insert("bar".to_owned(), Bson::String("__z&".to_owned()));
            let docs = vec![doc];
            let flags = OpInsertFlags::no_flags();
            let name = "test.test".to_owned();
            let res = Message::with_insert(1, flags, name, docs);

            let cm = match res {
                Ok(message) => message,
                Err(s) => panic!("{}", s)
            };

            match cm.write(&mut stream) {
                Ok(_) => (),
                Err(s) => panic!("{}", s)
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

            assert_eq!(docs.len() as i32, 1);

            match docs[0].get("foo") {
                Some(&Bson::FloatingPoint(42.0)) => (),
                _ => panic!("Wrong value returned!")
            };

            match docs[0].get("bar") {
                Some(&Bson::String(ref s)) => assert_eq!(s, "__z&"),
                _ => panic!("Wrong value returned!")
            };
        },
        Err(_) => {
            panic!("Could not connect to server")
        }
    }
}

#[test]
fn insert_docs() {
    match TcpStream::connect("localhost:27017") {
        Ok(mut stream) => {

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

            let mut doc1 = bson::Document::new();
            doc1.insert("foo".to_owned(), Bson::FloatingPoint(42.0));
            doc1.insert("bar".to_owned(), Bson::String("__z&".to_owned()));

            let mut doc2 = bson::Document::new();
            doc2.insert("booyah".to_owned(), Bson::I32(23));

            let docs = vec![doc1, doc2];
            let flags = OpInsertFlags::no_flags();
            let name = "test.test".to_owned();
            let res = Message::with_insert(1, flags, name, docs);

            let cm = match res {
                Ok(message) => message,
                Err(s) => panic!("{}", s)
            };

            match cm.write(&mut stream) {
                Ok(_) => (),
                Err(s) => panic!("{}", s)
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

            assert_eq!(docs.len() as i32, 2);

            match docs[0].get("foo") {
                Some(&Bson::FloatingPoint(42.0)) => (),
                _ => panic!("Wrong value returned!")
            };

            match docs[0].get("bar") {
                Some(&Bson::String(ref s)) => assert_eq!(s, "__z&"),
                _ => panic!("Wrong value returned!")
            };

            match docs[1].get("booyah") {
                Some(&Bson::I32(23)) => (),
                _ => panic!("Wrong value returned!")
            };
        },
        Err(_) => {
            panic!("Could not connect to server")
        }
    }
}
