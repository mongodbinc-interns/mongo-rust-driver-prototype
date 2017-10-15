use bson::{Bson, Document};

use mongodb::{Client, CommandType, ThreadedClient};
use mongodb::common::{ReadMode, ReadPreference};
use mongodb::coll::options::FindOptions;
use mongodb::db::ThreadedDatabase;
use mongodb::cursor::Cursor;
use mongodb::wire_protocol::flags::OpQueryFlags;

#[test]
fn cursor_features() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-cursor");
    let coll = db.collection("cursor_test");

    coll.drop().expect("Failed to drop collection.");

    let docs = (0..10)
        .map(|i| {
            doc! { "foo": i as i64 }
        })
        .collect();

    assert!(coll.insert_many(docs, None).is_ok());

    let doc = Document::new();
    let flags = OpQueryFlags::empty();

    let mut options = FindOptions::new();
    options.batch_size = Some(3);

    let result = Cursor::query(
        client.clone(),
        "test-client-cursor.cursor_test".to_owned(),
        flags,
        doc,
        options,
        CommandType::Find,
        false,
        ReadPreference::new(ReadMode::Primary, None),
    );

    let mut cursor = match result {
        Ok(c) => c,
        Err(s) => panic!("{}", s),
    };

    let batch = cursor.drain_current_batch().expect(
        "Failed to get current batch from cursor.",
    );

    assert_eq!(batch.len(), 3 as usize);

    for (i, item) in batch.iter().enumerate() {
        match item.get("foo") {
            Some(&Bson::I64(j)) => assert_eq!(i as i64, j),
            _ => panic!("Wrong value returned from Cursor#next_batch"),
        };
    }

    let bson = match cursor.next() {
        Some(Ok(b)) => b,
        Some(Err(_)) => panic!("Received error on 'cursor.next()'"),
        None => panic!("Nothing returned from Cursor#next"),
    };

    match bson.get("foo") {
        Some(&Bson::I64(3)) => (),
        _ => panic!("Wrong value returned from Cursor#next"),
    };

    assert!(cursor.has_next().expect("Failed to execute 'has_next()'."));
    let vec = cursor.next_n(20).expect("Failed to get next 20 results.");

    assert_eq!(vec.len(), 6 as usize);
    assert!(!cursor.has_next().expect("Failed to execute 'has_next()'."));

    for (i, item) in vec.iter().enumerate() {
        match item.get("foo") {
            Some(&Bson::I64(j)) => assert_eq!(4 + i as i64, j),
            _ => panic!("Wrong value returned from Cursor#next_batch"),
        };
    }
}
