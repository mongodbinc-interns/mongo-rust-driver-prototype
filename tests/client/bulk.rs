use bson::Bson;
use mongodb::coll::options::WriteModel;
use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;

#[test]
fn bulk_ordered_insert_only() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-bulk");
    let coll = db.collection("bulk_ordered_insert_only");
    coll.drop().unwrap();

    let models = (1..5)
        .map(|i| {
            WriteModel::InsertOne {
                document: doc! {
                    "_id": i,
                    "x": i * 11,
                },
            }
        })
        .collect();

    coll.bulk_write(models, true);

    let cursor: Vec<_> = coll.find(None, None).unwrap().collect();

    assert_eq!(cursor.len(), 4);

    for (i, result) in cursor.into_iter().enumerate() {
        let doc = result.unwrap();
        let expected_id = i + 1;

        match doc.get("_id") {
            Some(&Bson::I32(j)) => assert_eq!(expected_id as i32, j),
            _ => panic!("Invalid id: {:?}", doc),
        }

        match doc.get("x") {
            Some(&Bson::I32(j)) => assert_eq!(11 * expected_id as i32, j),
            _ => panic!("Invalid id: {:?}", doc),
        }
    }
}

#[test]
fn bulk_unordered_insert_only() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-bulk");
    let coll = db.collection("bulk_unordered_insert_only");

    coll.drop().unwrap();

    let models = (1..5)
        .map(|i| {
            WriteModel::InsertOne {
                document: doc! {
        "_id": i,
        "x": i * 11
    },
            }
        })
        .collect();

    coll.bulk_write(models, false);

    let cursor: Vec<_> = coll.find(None, None).unwrap().collect();

    assert_eq!(cursor.len(), 4);

    for (i, result) in cursor.into_iter().enumerate() {
        let doc = result.unwrap();
        let expected_id = i + 1;

        match doc.get("_id") {
            Some(&Bson::I32(j)) => assert_eq!(expected_id as i32, j),
            _ => panic!("Invalid id: {:?}", doc),
        }

        match doc.get("x") {
            Some(&Bson::I32(j)) => assert_eq!(11 * expected_id as i32, j),
            _ => panic!("Invalid id: {:?}", doc),
        }
    }
}

#[test]
fn bulk_ordered_mix() {
    let models = vec![
        WriteModel::InsertOne {
            document: doc! {
            "_id": 1,
            "x": 11
        },
        },
        WriteModel::InsertOne {
            document: doc! {
            "_id": 2,
            "x": 22
        },
        },
        WriteModel::InsertOne {
            document: doc! {
            "_id": 3,
            "x": 33
        },
        },
        WriteModel::InsertOne {
            document: doc! {
            "_id": 4,
            "x": 44
        },
        },
        WriteModel::ReplaceOne {
            filter: doc! { "_id": 3 },
            replacement: doc! { "x": 37 },
            upsert: Some(true),
        },
        WriteModel::UpdateMany {
            filter: doc! { "_id": { "$lt": 3 } },
            update: doc! { "$inc": { "x": 1 } },
            upsert: Some(false),
        },
        WriteModel::DeleteOne {
            filter: doc! {
            "_id": 4
        },
        },
        WriteModel::InsertOne {
            document: doc! {
            "_id": 5,
            "x": 55
        },
        },
        WriteModel::UpdateOne {
            filter: doc! { "_id": 6 },
            update: doc! { "$set":  { "x": 62 } },
            upsert: Some(true),
        },
        WriteModel::InsertOne {
            document: doc! {
            "_id": 101,
            "x": "dalmations"
        },
        },
        WriteModel::InsertOne {
            document: doc! {
            "_id": 102,
            "x": "strawberries"
        },
        },
        WriteModel::InsertOne {
            document: doc! {
            "_id": 103,
            "x": "blueberries"
        },
        },
        WriteModel::InsertOne {
            document: doc! {
            "_id": 104,
            "x": "bananas"
        },
        },
        WriteModel::DeleteMany {
            filter: doc! {
            "_id": { "$gte": 103 }
        },
        },
    ];

    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-bulk");
    let coll = db.collection("bulk_ordered_mix");
    coll.drop().unwrap();

    let result = coll.bulk_write(models, true);

    assert_eq!(result.inserted_count, 9);
    assert_eq!(result.inserted_ids.len() as i32, result.inserted_count);
    assert_eq!(result.matched_count, 3);
    assert_eq!(result.modified_count, 3);
    assert_eq!(result.deleted_count, 3);
    assert_eq!(result.upserted_count, 1);
    assert_eq!(result.upserted_ids.len() as i32, result.upserted_count);

    macro_rules! check_value_in_tree {
        ($tree:expr, $key:expr, $value:expr) => {
            match $tree.get(&$key).unwrap() {
                &Bson::I32($value) => (),
                &Bson::I64($value) => (),
                id => panic!("Invalid inserted id at index {}: {:?}", $key, id)
            }
        };
    }

    check_value_in_tree!(result.inserted_ids, 0, 1);
    check_value_in_tree!(result.inserted_ids, 1, 2);
    check_value_in_tree!(result.inserted_ids, 2, 3);
    check_value_in_tree!(result.inserted_ids, 3, 4);
    check_value_in_tree!(result.inserted_ids, 7, 5);
    check_value_in_tree!(result.inserted_ids, 9, 101);
    check_value_in_tree!(result.inserted_ids, 10, 102);
    check_value_in_tree!(result.inserted_ids, 11, 103);
    check_value_in_tree!(result.inserted_ids, 12, 104);
    check_value_in_tree!(result.upserted_ids, 8, 6);
}
