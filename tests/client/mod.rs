mod bulk;
mod coll;
mod connstring;
mod crud_spec;
mod db;
mod cursor;
mod error;
mod gridfs;
mod wire_protocol;

use bson;
use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use std::thread;

#[test]
fn database_names() {
    let client = Client::connect("localhost", 27018).unwrap();
    let state_results = client.database_names().expect("Failed to execute database_names.");
    for name in state_results {
        if name != "local" {
            client.drop_database(&name[..]).expect("Failed to drop database from server.");
        }
    }

    let base_results = client.database_names().expect("Failed to execute database_names.");
    assert_eq!(1, base_results.len());
    assert_eq!("local", base_results[0]);

    // Build dbs
    let db1 = client.db("new_db");
    let db2 = client.db("new_db_2");
    db1.collection("test1")
        .insert_one(bson::Document::new(), None)
        .expect("Failed to insert placeholder document into collection");
    db2.collection("test2")
        .insert_one(bson::Document::new(), None)
        .expect("Failed to insert placeholder document into collection");

    // Check new dbs
    let results = client.database_names().expect("Failed to execute database_names.");
    assert_eq!(3, results.len());
    assert!(results.contains(&"local".to_owned()));
    assert!(results.contains(&"new_db".to_owned()));
    assert!(results.contains(&"new_db_2".to_owned()));
}


#[test]
fn is_master() {
    let client = Client::connect("localhost", 27017).unwrap();
    let res = client.is_master().expect("Failed to execute is_master.");
    assert!(res);
}

#[test]
fn is_sync() {
    let client = Client::connect("localhost", 27018).unwrap();
    let state_results = client.database_names().expect("Failed to execute database_names.");
    for name in state_results {
        if name != "local" {
            client.drop_database(&name[..]).expect("Failed to drop database from server.");
        }
    }

    let client1 = client.clone();
    let client2 = client.clone();

    let base_results = client.database_names().expect("Failed to execute database_names.");
    assert_eq!(1, base_results.len());
    assert_eq!("local", base_results[0]);

    let child1 = thread::spawn(move || {
        let db = client1.db("concurrent_db");
        db.collection("test1")
            .insert_one(bson::Document::new(), None)
            .expect("Failed to insert placeholder document into collection");
        let results = client1.database_names().expect("Failed to execute database_names.");
        assert!(results.contains(&"concurrent_db".to_owned()));
    });

    let child2 = thread::spawn(move || {
        let db = client2.db("concurrent_db_2");
        db.collection("test2")
            .insert_one(bson::Document::new(), None)
            .expect("Failed to insert placeholder document into collection");
        let results = client2.database_names().expect("Failed to execute database_names.");
        assert!(results.contains(&"concurrent_db_2".to_owned()));
    });

    let _ = child1.join();
    let _ = child2.join();

    // Check new dbs
    let results = client.database_names().expect("Failed to execute database_names.");
    assert_eq!(3, results.len());
    assert!(results.contains(&"local".to_owned()));
    assert!(results.contains(&"concurrent_db".to_owned()));
    assert!(results.contains(&"concurrent_db_2".to_owned()));
}
