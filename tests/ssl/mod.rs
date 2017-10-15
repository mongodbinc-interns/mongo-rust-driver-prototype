use std::path::PathBuf;

use mongodb::{Client, ClientOptions, ThreadedClient};
use mongodb::db::ThreadedDatabase;

#[test]
fn ssl_connect_and_insert() {
    let mut test_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    test_path.push("tests");
    test_path.push("ssl");

    let options = ClientOptions::with_ssl(
        test_path.join("ca.pem").to_str().unwrap(),
        test_path.join("client.crt").to_str().unwrap(),
        test_path.join("client.key").to_str().unwrap(),
        false,
    );
    let client = Client::connect_with_options("127.0.0.1", 27018, options).unwrap();
    let db = client.db("test");
    let coll = db.collection("stuff");

    let doc = doc! { "x": 1 };

    coll.insert_one(doc, None).unwrap();
}

#[test]
fn unauthenticated_ssl_connect_and_insert() {
    let mut test_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    test_path.push("tests");
    test_path.push("ssl");

    let options =
        ClientOptions::with_unauthenticated_ssl(test_path.join("ca.pem").to_str().unwrap(), false);
    let client = Client::connect_with_options("127.0.0.1", 27018, options).unwrap();
    let db = client.db("test");
    let coll = db.collection("stuff");

    let doc = doc! { "x": 1 };

    coll.insert_one(doc, None).unwrap();
}
