use bson::Document;
use bson::Bson::{FloatingPoint, I32};
use bson::Bson::String as BsonString;
use mongodb::client::wire_protocol::flags::{OpInsertFlags, OpQueryFlags};
use mongodb::client::wire_protocol::operations::Message;
use std::io::Write;
use std::net::TcpStream;

#[test]
fn insert_single_key_doc() {
    match TcpStream::connect("localhost:27017") {
        Ok(mut stream) => {
            let mut doc = Document::new();
            doc.insert("foo".to_owned(), FloatingPoint(42.0));
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

            let doc = Document::new();
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
                Some(&FloatingPoint(42.0)) => (),
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
            let mut doc = Document::new();
            doc.insert("foo".to_owned(), FloatingPoint(42.0));
            doc.insert("bar".to_owned(), BsonString("__z&".to_owned()));
            let docs = vec![doc];
            let flags = OpInsertFlags::no_flags();
            let name = "test.test2".to_owned();
            let res = Message::with_insert(1, flags, name, docs);

            let cm = match res {
                Ok(message) => message,
                Err(s) => panic!("{}", s)
            };

            match cm.write(&mut stream) {
                Ok(_) => (),
                Err(s) => panic!("{}", s)
            };

            let doc = Document::new();
            let flags = OpQueryFlags::no_flags();
            let name = "test.test2".to_owned();
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
                Some(&FloatingPoint(42.0)) => (),
                _ => panic!("Wrong value returned!")
            };

            match docs[0].get("bar") {
                Some(&BsonString(ref s)) => assert_eq!(s, "__z&"),
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
            let mut doc1 = Document::new();
            doc1.insert("foo".to_owned(), FloatingPoint(42.0));
            doc1.insert("bar".to_owned(), BsonString("__z&".to_owned()));

            let mut doc2 = Document::new();
            doc2.insert("booyah".to_owned(), I32(23));

            let docs = vec![doc1, doc2];
            let flags = OpInsertFlags::no_flags();
            let name = "test.test3".to_owned();
            let res = Message::with_insert(1, flags, name, docs);

            let cm = match res {
                Ok(message) => message,
                Err(s) => panic!("{}", s)
            };

            match cm.write(&mut stream) {
                Ok(_) => (),
                Err(s) => panic!("{}", s)
            };

            let doc = Document::new();
            let flags = OpQueryFlags::no_flags();
            let name = "test.test3".to_owned();
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
                Some(&FloatingPoint(42.0)) => (),
                _ => panic!("Wrong value returned!")
            };

            match docs[0].get("bar") {
                Some(&BsonString(ref s)) => assert_eq!(s, "__z&"),
                _ => panic!("Wrong value returned!")
            };

            match docs[1].get("booyah") {
                Some(&I32(23)) => (),
                _ => panic!("Wrong value returned!")
            };
        },
        Err(_) => {
            panic!("Could not connect to server")
        }
    }
}
