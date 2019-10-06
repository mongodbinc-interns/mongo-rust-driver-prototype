use std::env;

extern crate mongodb;

use mongodb::{Client, ThreadedClient, ClientOptions};
use mongodb::db::ThreadedDatabase;

#[cfg(feature = "ssl")]
fn main() -> Result<(), std::io::Error> {
    let args: Vec<String> = env::args().collect();
    let uri = args.get(1).expect("First parameter should be a connection string");
    let username = args.get(2).expect("Second parameter should be the username");
    let password = args.get(3).expect("Third parameter should be the password");

    let options = ClientOptions::with_unauthenticated_ssl(None, false);
    let client = Client::with_uri_and_options(uri, options).unwrap();
    let db = client.db("admin");

    db.auth(username, password).unwrap();

    let db = client.db("foo");
    let coll = db.collection("count");

    let count = coll.count(None, None).unwrap();

    assert_eq!(count, 0);

    println!("DONE!");

    Ok(())
}

#[cfg(not(feature = "ssl"))]
fn main() {
    println!("Skipped: please enable ssl feature");
}
