use mongodb::coll::Collection;
use mongodb::cursor::Cursor;
use mongodb::db::ThreadedDatabase;
use mongodb::{Client, Result, ThreadedClient};

fn test_batch_size<F>(coll_name: &str, query: F)
where
    F: Fn(&Collection) -> Result<Cursor>,
{
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db(coll_name);
    let coll = db.collection("aggregate_batch_size");
    coll.drop().unwrap();

    let contents = (0..512).into_iter().map(|i| doc! { "x": i }).collect();
    coll.insert_many(contents, None).unwrap();

    let mut cursor = query(&coll).unwrap();

    for _ in 0..(512 / 101) {
        let batch = cursor.drain_current_batch().unwrap();
        assert_eq!(101, batch.len());
    }

    let final_batch = cursor.drain_current_batch().unwrap();

    println!("last: {}", final_batch.last().unwrap());

    assert_eq!(512 % 101, final_batch.len());
    assert!(cursor.next().is_none());
}

#[test]
fn aggregate_batch_size() {
    test_batch_size("aggregate_batch_size", |coll| {
        coll.aggregate(Vec::new(), None)
    });
}

#[test]
fn find_batch_size() {
    test_batch_size("find_batch_size", |coll| coll.find(None, None));
}
