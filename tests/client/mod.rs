mod batch_size;
mod bulk;
mod coll;
mod connstring;
mod crud_spec;
mod db;
mod cursor;
mod error;
mod gridfs;
mod handshake;
mod wire_protocol;

use bson;
use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use std::thread;

#[test]
fn is_master() {
    let client = Client::connect("localhost", 27017).unwrap();
    let res = client.is_master().expect("Failed to execute is_master.");
    assert!(res);
}

#[test]
fn database_names() {
    let client = Client::connect("localhost", 27017).unwrap();
    client
        .drop_database("test-client-mod-database_names")
        .expect("Failed to drop database");
    client
        .drop_database("test-client-mod-database_names_2")
        .expect("Failed to drop database");

    let base_results = client.database_names().expect(
        "Failed to execute database_names.",
    );

    assert!(base_results.contains(&"admin".to_owned()));
    assert!(base_results.contains(&"local".to_owned()));
    assert!(!base_results.contains(
        &"test-client-mod-database_names".to_owned(),
    ));
    assert!(!base_results.contains(
        &"test-client-mod-database_names_2".to_owned(),
    ));

    // Build dbs
    let db1 = client.db("test-client-mod-database_names");
    let db2 = client.db("test-client-mod-database_names_2");
    db1.collection("test1")
        .insert_one(bson::Document::new(), None)
        .expect("Failed to insert placeholder document into collection");
    db2.collection("test2")
        .insert_one(bson::Document::new(), None)
        .expect("Failed to insert placeholder document into collection");

    // Check new dbs
    let results = client.database_names().expect(
        "Failed to execute database_names.",
    );
    assert!(results.contains(&"admin".to_owned()));
    assert!(results.contains(&"local".to_owned()));
    assert!(results.contains(
        &"test-client-mod-database_names".to_owned(),
    ));
    assert!(results.contains(
        &"test-client-mod-database_names_2".to_owned(),
    ));
}

#[test]
fn is_sync() {
    let client = Client::connect("localhost", 27017).unwrap();
    let client1 = client.clone();
    let client2 = client.clone();

    client.drop_database("test-client-mod-is_sync").expect(
        "failed to drop database",
    );
    client.drop_database("test-client-mod-is_sync_2").expect(
        "failed to drop database",
    );

    let base_results = client.database_names().expect(
        "Failed to execute database_names.",
    );

    assert!(base_results.contains(&"admin".to_owned()));
    assert!(base_results.contains(&"local".to_owned()));

    assert!(!base_results.contains(
        &"test-client-mod-is_sync".to_owned(),
    ));
    assert!(!base_results.contains(
        &"test-client-mod-is_sync_2".to_owned(),
    ));

    let child1 = thread::spawn(move || {
        let db = client1.db("test-client-mod-is_sync");
        db.collection("test1")
            .insert_one(bson::Document::new(), None)
            .expect("Failed to insert placeholder document into collection");
        let results = client1.database_names().expect(
            "Failed to execute database_names.",
        );
        assert!(results.contains(&"test-client-mod-is_sync".to_owned()));
    });

    let child2 = thread::spawn(move || {
        let db = client2.db("test-client-mod-is_sync_2");
        db.collection("test2")
            .insert_one(bson::Document::new(), None)
            .expect("Failed to insert placeholder document into collection");
        let results = client2.database_names().expect(
            "Failed to execute database_names.",
        );
        assert!(results.contains(&"test-client-mod-is_sync_2".to_owned()));
    });

    let _ = child1.join();
    let _ = child2.join();

    // Check new dbs
    let results = client.database_names().expect(
        "Failed to execute database_names.",
    );
    assert!(results.contains(&"admin".to_owned()));
    assert!(results.contains(&"local".to_owned()));
    assert!(results.contains(&"test-client-mod-is_sync".to_owned()));
    assert!(results.contains(&"test-client-mod-is_sync_2".to_owned()));
}
