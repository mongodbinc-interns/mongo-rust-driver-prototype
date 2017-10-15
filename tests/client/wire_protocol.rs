use bson::{Bson, Document};
use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use mongodb::wire_protocol::flags::{OpInsertFlags, OpQueryFlags, OpUpdateFlags};
use mongodb::wire_protocol::operations::Message;
use std::net::TcpStream;

#[test]
fn insert_single_key_doc() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-wire_protocol-insert_single_key_doc");
    db.drop_database().unwrap();

    match TcpStream::connect("localhost:27017") {
        Ok(mut stream) => {
            let doc = doc! { "foo": 42.0 };

            let docs = vec![doc];
            let flags = OpInsertFlags::empty();
            let name = "test-client-wire_protocol-insert_single_key_doc.single_key".to_owned();
            let res = Message::new_insert(1, flags, name, docs);

            let cm = match res {
                Ok(message) => message,
                Err(_) => panic!("Could not create message!"),
            };

            match cm.write(&mut stream) {
                Ok(_) => (),
                Err(s) => panic!("{}", s),
            };

            let doc = Document::new();
            let flags = OpQueryFlags::empty();
            let name = "test-client-wire_protocol-insert_single_key_doc.single_key".to_owned();
            let res = Message::new_query(1, flags, name, 0, 0, doc, None);

            let cm = match res {
                Ok(message) => message,
                Err(s) => panic!("{}", s),
            };

            match cm.write(&mut stream) {
                Ok(_) => (),
                Err(s) => panic!("{}", s),
            };

            let reply = match Message::read(&mut stream) {
                Ok(m) => m,
                Err(s) => panic!("{}", s),
            };

            let docs = match reply {
                Message::OpReply { documents: d, .. } => d,
                _ => panic!("Invalid response read from server"),
            };

            assert_eq!(docs.len() as i32, 1);

            match docs[0].get("foo") {
                Some(&Bson::FloatingPoint(f)) if f == 42.0 => (),
                _ => panic!("Wrong value returned!"),
            };
        }
        Err(_) => panic!("Could not connect to server"),
    }
}

#[test]
fn insert_multi_key_doc() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-wire_protocol-insert_multi_key_doc");
    db.drop_database().unwrap();

    match TcpStream::connect("localhost:27017") {
        Ok(mut stream) => {
            let doc =
                doc! {
                "foo": 42.0,
                "bar": "__z&"
            };

            let docs = vec![doc];
            let flags = OpInsertFlags::empty();
            let name = "test-client-wire_protocol-insert_multi_key_doc.multi_key".to_owned();
            let res = Message::new_insert(1, flags, name, docs);

            let cm = match res {
                Ok(message) => message,
                Err(s) => panic!("{}", s),
            };

            match cm.write(&mut stream) {
                Ok(_) => (),
                Err(s) => panic!("{}", s),
            };

            let doc = Document::new();
            let flags = OpQueryFlags::empty();
            let name = "test-client-wire_protocol-insert_multi_key_doc.multi_key".to_owned();
            let res = Message::new_query(1, flags, name, 0, 0, doc, None);

            let cm = match res {
                Ok(message) => message,
                Err(s) => panic!("{}", s),
            };

            match cm.write(&mut stream) {
                Ok(_) => (),
                Err(s) => panic!("{}", s),
            };

            let reply = match Message::read(&mut stream) {
                Ok(m) => m,
                Err(s) => panic!("{}", s),
            };

            let docs = match reply {
                Message::OpReply { documents: d, .. } => d,
                _ => panic!("Invalid response read from server"),
            };

            assert_eq!(docs.len() as i32, 1);

            match docs[0].get("foo") {
                Some(&Bson::FloatingPoint(f)) if f == 42.0 => (),
                _ => panic!("Wrong value returned!"),
            };

            match docs[0].get("bar") {
                Some(&Bson::String(ref s)) => assert_eq!(s, "__z&"),
                _ => panic!("Wrong value returned!"),
            };
        }
        Err(_) => panic!("Could not connect to server"),
    }
}

#[test]
fn insert_docs() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-wire_protocol-insert_docs");
    db.drop_database().unwrap();

    match TcpStream::connect("localhost:27017") {
        Ok(mut stream) => {
            let doc1 =
                doc! {
                "foo": 42.0,
                "bar": "__z&"
            };

            let doc2 =
                doc! {
                "booyah": 23
            };

            let docs = vec![doc1, doc2];
            let flags = OpInsertFlags::empty();
            let name = "test-client-wire_protocol-insert_docs.multi_doc".to_owned();
            let res = Message::new_insert(1, flags, name, docs);

            let cm = match res {
                Ok(message) => message,
                Err(s) => panic!("{}", s),
            };

            match cm.write(&mut stream) {
                Ok(_) => (),
                Err(s) => panic!("{}", s),
            };

            let doc = Document::new();
            let flags = OpQueryFlags::empty();
            let name = "test-client-wire_protocol-insert_docs.multi_doc".to_owned();
            let res = Message::new_query(1, flags, name, 0, 0, doc, None);

            let cm = match res {
                Ok(message) => message,
                Err(s) => panic!("{}", s),
            };

            match cm.write(&mut stream) {
                Ok(_) => (),
                Err(s) => panic!("{}", s),
            };


            let reply = match Message::read(&mut stream) {
                Ok(m) => m,
                Err(s) => panic!("{}", s),
            };

            let docs = match reply {
                Message::OpReply { documents: d, .. } => d,
                _ => panic!("Invalid response read from server"),
            };

            assert_eq!(docs.len() as i32, 2);

            match docs[0].get("foo") {
                Some(&Bson::FloatingPoint(f)) if f == 42.0 => (),
                _ => panic!("Wrong value returned!"),
            };

            match docs[0].get("bar") {
                Some(&Bson::String(ref s)) => assert_eq!(s, "__z&"),
                _ => panic!("Wrong value returned!"),
            };

            match docs[1].get("booyah") {
                Some(&Bson::I32(23)) => (),
                _ => panic!("Wrong value returned!"),
            };
        }
        Err(_) => panic!("Could not connect to server"),
    }
}


#[test]
fn insert_update_then_query() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-wire_protocol-insert_update_then_query");
    db.drop_database().unwrap();

    match TcpStream::connect("localhost:27017") {
        Ok(mut stream) => {
            let doc = doc! { "foo": 42.0 };

            let docs = vec![doc];
            let flags = OpInsertFlags::empty();
            let name = "test-client-wire_protocol-insert_update_then_query.update".to_owned();
            let res = Message::new_insert(1, flags, name, docs);

            let cm = match res {
                Ok(message) => message,
                Err(_) => panic!("Could not create insert message!"),
            };

            match cm.write(&mut stream) {
                Ok(_) => (),
                Err(s) => panic!("{}", s),
            };

            let selector = Document::new();

            let update = doc! { "foo": "bar" };

            let flags = OpUpdateFlags::empty();
            let name = "test-client-wire_protocol-insert_update_then_query.update".to_owned();
            let res = Message::new_update(2, name, flags, selector, update);

            let cm = match res {
                Ok(message) => message,
                Err(_) => panic!("Could not create update message!"),
            };

            match cm.write(&mut stream) {
                Ok(_) => (),
                Err(s) => panic!("{}", s),
            };

            let doc = Document::new();
            let flags = OpQueryFlags::empty();
            let name = "test-client-wire_protocol-insert_update_then_query.update".to_owned();
            let res = Message::new_query(3, flags, name, 0, 0, doc, None);

            let cm = match res {
                Ok(message) => message,
                Err(_) => panic!("Could not create query message!"),
            };

            match cm.write(&mut stream) {
                Ok(_) => (),
                Err(s) => panic!("{}", s),
            };

            let reply = match Message::read(&mut stream) {
                Ok(m) => m,
                Err(s) => panic!("Could not read response: {}", s),
            };

            let docs = match reply {
                Message::OpReply { documents: d, .. } => d,
                _ => panic!("Invalid response read from server"),
            };

            assert_eq!(docs.len() as i32, 1);

            match docs[0].get("foo") {
                Some(&Bson::String(ref s)) => assert_eq!(s, "bar"),
                _ => panic!("Wrong value returned!"),
            };
        }
        Err(_) => panic!("Could not connect to server"),
    }
}
