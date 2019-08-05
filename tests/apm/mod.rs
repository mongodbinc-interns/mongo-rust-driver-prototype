use std::fs::{self, File};
use std::io::{BufRead, BufReader};

use bson::Bson;
use mongodb::{Client, ClientOptions, CommandResult, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use rand;

fn timed_query(_client: Client, command_result: &CommandResult) {
    let (command_name, duration) = match *command_result {
        CommandResult::Success {
            ref command_name,
            duration,
            ..
        } => (command_name.clone(), duration),
        _ => panic!("Command failed!"),
    };

    if command_name.eq("find") {
        // Sanity check
        assert!(duration >= 1500000000);
        // There is no need to assert a upper duration bound since it is not the purpose of this test
    }
}

#[test]
fn command_duration() {
    let mut client = Client::connect("localhost", 27017).expect("damn it!");
    let db = client.db("test-apm-mod");
    let coll = db.collection("command_duration");
    coll.drop().unwrap();

    let docs = (1..4)
        .map(|i| {
            doc! {
                "_id": i,
                "x": rand::random::<u8>() as u32,
            }
        })
        .collect();
    coll.insert_many(docs, None).unwrap();
    client.add_completion_hook(timed_query).unwrap();

    let doc = doc! {
        "$where": Bson::JavaScriptCode(
            "function() { sleep(500); }".to_owned()
        )
    };

    coll.find(Some(doc), None).unwrap();
}

fn read_first_non_monitor_line(file: &mut BufReader<&File>, line: &mut String) {
    loop {
        file.read_line(line).unwrap();
        if !line.starts_with("COMMAND.is_master") {
            break;
        }
        line.clear();
    }
}

#[test]
fn logging() {
    let _ = fs::remove_file("test_log.txt");

    // Reset State
    let reset_client =
        Client::connect("localhost", 27017).expect("Failed to connect to localhost:27017");
    let reset_db = reset_client.db("test-apm-mod");
    let reset_coll = reset_db.collection("logging");
    reset_coll.drop().unwrap();

    let db_version = reset_db.version().unwrap();
    let v3_3 = db_version.major <= 3 && db_version.minor <= 3;

    // Start logging
    let client_options = ClientOptions::with_log_file("test_log.txt");
    let client = Client::connect_with_options("localhost", 27017, client_options).unwrap();

    let db = client.db("test-apm-mod");
    let coll = db.collection("logging");
    db.create_collection("logging", None).unwrap();
    coll.drop().unwrap();

    let doc1 = doc! { "_id": 1 };
    let doc2 = doc! { "_id": 2 };
    let doc3 = doc! { "_id": 3 };

    coll.insert_one(doc1, None).unwrap();
    coll.insert_one(doc2, None).unwrap();
    coll.insert_one(doc3, None).unwrap();

    let filter =
        doc! {
        "_id": { "$gt": 1 }
    };

    coll.find(Some(filter), None).unwrap();

    let f = File::open("test_log.txt").unwrap();
    let mut file = BufReader::new(&f);
    let mut line = String::new();

    // Create collection started
    read_first_non_monitor_line(&mut file, &mut line);
    assert_eq!(
        "COMMAND.create_collection 127.0.0.1:27017 STARTED: { create: \"logging\" }\n",
        &line
    );

    // Create collection completed
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert!(line.starts_with(
        "COMMAND.create_collection 127.0.0.1:27017 COMPLETED: { ok: 1 } (",
    ));
    assert!(line.ends_with(" ns)\n"));

    // Drop collection started
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert_eq!(
        "COMMAND.drop_collection 127.0.0.1:27017 STARTED: { drop: \"logging\" }\n",
        &line
    );

    // Drop collection completed
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert!(line.starts_with(
        "COMMAND.drop_collection 127.0.0.1:27017 COMPLETED: { ns: \"test-apm-mod.logging\", nIndexesWas: 1, ok: 1 } (",
    ));
    assert!(line.ends_with(" ns)\n"));

    // First insert started
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert_eq!(
        "COMMAND.insert_one 127.0.0.1:27017 STARTED: { insert: \"logging\", documents: [{ _id: 1 }] }\n",
        &line
    );


    let insert_one_line_start = if v3_3 {
        "COMMAND.insert_one 127.0.0.1:27017 COMPLETED: { ok: 1, n: 1 } ("
    } else {
        "COMMAND.insert_one 127.0.0.1:27017 COMPLETED: { n: 1, ok: 1 } ("
    };

    // First insert completed
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert!(line.starts_with(insert_one_line_start));
    assert!(line.ends_with(" ns)\n"));

    // Second insert started
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert_eq!(
        "COMMAND.insert_one 127.0.0.1:27017 STARTED: { insert: \"logging\", documents: [{ _id: 2 }] }\n",
        &line
    );

    // Second insert completed
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert!(line.starts_with(insert_one_line_start));
    assert!(line.ends_with(" ns)\n"));

    // Third insert started
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert_eq!(
        "COMMAND.insert_one 127.0.0.1:27017 STARTED: { insert: \"logging\", documents: [{ _id: 3 }] }\n",
        &line
    );

    // Third insert completed
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert!(line.starts_with(insert_one_line_start));
    assert!(line.ends_with(" ns)\n"));

    // Find command started
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert_eq!(
        "COMMAND.find 127.0.0.1:27017 STARTED: { find: \"logging\", filter: { _id: { $gt: 1 } } }\n",
        &line
    );

    // Find command completed
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert!(line.starts_with(
        "COMMAND.find 127.0.0.1:27017 COMPLETED: { cursor: { id: 0, ns: \
            \"test-apm-mod.logging\", firstBatch: [{ _id: 2 }, { _id: 3 }] }, ok: 1 } (",
    ));
    assert!(line.ends_with(" ns)\n"));

    coll.drop().unwrap();

    fs::remove_file("test_log.txt").unwrap();
}
