use std::fs::{self, File};
use std::io::{BufRead, BufReader};

use bson::Bson;
use mongodb::{Client, ClientOptions, CommandResult, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use rand;

fn timed_query(_client: Client, command_result: &CommandResult) {
    let (command_name, duration) = match *command_result {
        CommandResult::Success { ref command_name, duration, .. } => {
            (command_name.clone(), duration)
        }
        _ => panic!("Command failed!"),
    };

    if command_name.eq("find") {
        // Sanity check
        assert!(duration >= 1500000000);

        // Technically not guaranteed, but since the query is running locally, it shouldn't even be
        // close
        assert!(duration < 2000000000);
    }
}

#[test]
fn command_duration() {
    let mut client = Client::connect("localhost", 27017).expect("damn it!");
    let db = client.db("test");
    let coll = db.collection("event_hooks");
    coll.drop().unwrap();

    let docs = (1..4).map(|i| doc! { "_id" => i, "x" => (rand::random::<u8>() as u32) }).collect();
    coll.insert_many(docs, None).unwrap();
    client.add_completion_hook(timed_query).unwrap();

    let doc = doc! {
        "$where" => (Bson::JavaScriptCode("function() { sleep(500); }".to_owned()))
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

    let client_options = ClientOptions::with_log_file("test_log.txt");
    let client = Client::connect_with_options("localhost", 27017, client_options).unwrap();

    let db = client.db("test");
    db.create_collection("logging", None).unwrap();
    let coll = db.collection("logging");
    coll.drop().unwrap();

    let doc1 = doc! { "_id" => 1 };
    let doc2 = doc! { "_id" => 2 };
    let doc3 = doc! { "_id" => 3 };

    coll.insert_one(doc1, None).unwrap();
    coll.insert_one(doc2, None).unwrap();
    coll.insert_one(doc3, None).unwrap();

    let filter = doc! {
        "_id" => { "$gt" => 1 }
    };

    coll.find(Some(filter), None).unwrap();

    let f = File::open("test_log.txt").unwrap();
    let mut file = BufReader::new(&f);
    let mut line = String::new();

    // Create collection started
    read_first_non_monitor_line(&mut file, &mut line);
    assert_eq!("COMMAND.create_collection 127.0.0.1:27017 STARTED: { create: \"logging\", \
                capped: false, auto_index_id: true, flags: 1 }\n",
               &line);

    // Create Collection completed
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert!(line.starts_with("COMMAND.create_collection 127.0.0.1:27017 COMPLETED: { ok: 1 } ("));
    assert!(line.ends_with(" ns)\n"));

    // Drop collection started
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert_eq!("COMMAND.drop_collection 127.0.0.1:27017 STARTED: { drop: \"logging\" }\n",
               &line);

    // Drop collection completed
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert!(line.starts_with("COMMAND.drop_collection 127.0.0.1:27017 COMPLETED: { ns: \
                              \"test.logging\", nIndexesWas: 1, ok: 1 } ("));
    assert!(line.ends_with(" ns)\n"));

    // First insert started
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert_eq!("COMMAND.insert_one 127.0.0.1:27017 STARTED: { insert: \"logging\", documents: [{ \
                _id: 1 }], ordered: true, writeConcern: { w: 1, wtimeout: 0, j: false } }\n",
               &line);

    // First insert completed
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert!(line.starts_with("COMMAND.insert_one 127.0.0.1:27017 COMPLETED: { ok: 1, n: 1 } ("));
    assert!(line.ends_with(" ns)\n"));

    // Second insert started
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert_eq!("COMMAND.insert_one 127.0.0.1:27017 STARTED: { insert: \"logging\", documents: [{ \
                _id: 2 }], ordered: true, writeConcern: { w: 1, wtimeout: 0, j: false } }\n",
               &line);

    // Second insert completed
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert!(line.starts_with("COMMAND.insert_one 127.0.0.1:27017 COMPLETED: { ok: 1, n: 1 } ("));
    assert!(line.ends_with(" ns)\n"));

    // Third insert started
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert_eq!("COMMAND.insert_one 127.0.0.1:27017 STARTED: { insert: \"logging\", documents: [{ \
                _id: 3 }], ordered: true, writeConcern: { w: 1, wtimeout: 0, j: false } }\n",
               &line);

    // Third insert completed
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert!(line.starts_with("COMMAND.insert_one 127.0.0.1:27017 COMPLETED: { ok: 1, n: 1 } ("));
    assert!(line.ends_with(" ns)\n"));

    // Find command started
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert_eq!("COMMAND.find 127.0.0.1:27017 STARTED: { find: \"logging\", filter: {  }, \
                projection: {  }, skip: 0, limit: 0, batchSize: 20, sort: {  } }\n",
               &line);

    // Find command completed
    line.clear();
    read_first_non_monitor_line(&mut file, &mut line);
    assert!(line.starts_with("COMMAND.find 127.0.0.1:27017 COMPLETED: { cursor: { id: 0, ns: \
                              \"test.logging\", firstBatch: [{ _id: 2 }, { _id: 3 }] }, ok: 1 } \
                              ("));
    assert!(line.ends_with(" ns)\n"));

    coll.drop().unwrap();

    fs::remove_file("test_log.txt").unwrap();
}
