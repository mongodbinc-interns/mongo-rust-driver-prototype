use bson::Document;
use bson::Bson;
use mongodb::client::wire_protocol::operations::Message;
use mongodb::client::MongoClient;
use mongodb::client::db::Database;
use mongodb::client::coll::{Collection, FindOptions};
use std::io::Write;
use std::net::TcpStream;

#[test]
fn find() {
    let client = MongoClient::with_uri("mongodb://localhost:27017").unwrap();
    let db = client.db("sample");
    let coll = db.collection("movies");

    let results = coll.find(None, None).unwrap();

    assert!(results.len() > 0);
    let expected_val = &Bson::String("Jaws".to_owned());

    match results[0].get("title") {
        Some(expected_val) => (),
        _ => panic!("Wrong value returned!"),
    };
}

#[test]
fn find_one() {
    let client = MongoClient::with_uri("mongodb://localhost:27017").unwrap();
    let db = client.db("sample");
    let coll = db.collection("movies");

    let result = coll.find_one(None, None).unwrap();

    assert!(result.is_some());
    let expected_val = &Bson::String("Jaws".to_owned());

    match result.unwrap().get("title") {
        Some(expected_val) => (),
        _ => panic!("Wrong value returned!"),
    };
}

//#[test]
fn list_collections() {
    let client = MongoClient::with_uri("mongodb://localhost:27017").unwrap();
    let db = client.db("sample");
    let result = db.list_collections().unwrap();
    assert!(result.len() > 0);
}
