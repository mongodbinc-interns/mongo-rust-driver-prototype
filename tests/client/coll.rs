use bson::Bson;

use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use mongodb::coll::options::{FindOptions, FindOneAndUpdateOptions, IndexModel, IndexOptions,
                             ReturnDocument};

#[test]
fn find_sorted() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("find_sorted");

    coll.drop().expect("Failed to drop collection");

    // Insert document
    let doc1 = doc! { "title": "Jaws" };
    let doc2 = doc! { "title": "Back to the Future" };
    let doc3 = doc! { "title": "Dobby" };

    coll.insert_many(vec![doc1.clone(), doc2.clone(), doc3.clone()], None)
        .expect("Failed to insert documents.");

    // Find document
    let mut opts = FindOptions::new();
    opts.sort = Some(doc! { "title": 1 });

    let mut cursor = coll.find(None, Some(opts)).expect(
        "Failed to execute find command.",
    );
    let results = cursor.next_n(3).expect("Failed to retrieve documents.");

    // Assert expected titles of documents
    match results[0].get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("Back to the Future", title),
        _ => panic!("Expected Bson::String!"),
    };

    match results[1].get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("Dobby", title),
        _ => panic!("Expected Bson::String!"),
    };

    match results[2].get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("Jaws", title),
        _ => panic!("Expected Bson::String!"),
    };


    assert!(cursor.next().is_none());
}

#[test]
fn find_and_insert() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("find_and_insert");

    coll.drop().expect("Failed to drop collection");

    // Insert document
    let doc = doc! { "title": "Jaws" };
    coll.insert_one(doc, None).expect(
        "Failed to insert document",
    );

    // Find document
    let mut cursor = coll.find(None, None).expect(
        "Failed to execute find command.",
    );
    let result = match cursor.next() {
        Some(Ok(res)) => res,
        Some(Err(_)) => panic!("Received error from 'cursor.next()'."),
        None => panic!("Expected bson."),
    };

    // Assert expected title of document
    match result.get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("Jaws", title),
        _ => panic!("Expected Bson::String!"),
    };

    assert!(cursor.next().is_none());
}

#[test]
fn find_and_insert_one() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("find_and_insert_one");

    coll.drop().expect("Failed to drop database");

    // Insert document
    let doc = doc! { "title": "Jaws" };
    coll.insert_one(doc, None).expect(
        "Failed to insert document",
    );

    // Find single document
    let result = coll.find_one(None, None).expect(
        "Failed to execute find command.",
    );
    assert!(result.is_some());

    // Assert expected title of document
    match result.unwrap().get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("Jaws", title),
        _ => panic!("Expected Bson::String!"),
    };
}

#[test]
fn find_one_and_delete() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("find_one_and_delete");

    coll.drop().expect("Failed to drop database");

    // Insert documents
    let doc1 = doc! { "title": "Jaws" };
    let doc2 = doc! { "title": "Back to the Future" };

    coll.insert_many(vec![doc1.clone(), doc2.clone()], None)
        .expect("Failed to insert documents.");

    // Find and Delete document
    let result = coll.find_one_and_delete(doc2.clone(), None).expect(
        "Failed to execute find_one_and_delete command.",
    );

    match result.unwrap().get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("Back to the Future", title),
        _ => panic!("Expected Bson::String!"),
    }

    // Validate state of collection
    let mut cursor = coll.find(None, None).expect(
        "Failed to execute find command.",
    );
    let result = match cursor.next() {
        Some(Ok(res)) => res,
        Some(Err(_)) => panic!("Received error from 'cursor.next()'."),
        None => panic!("Expected bson."),
    };

    match result.get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("Jaws", title),
        _ => panic!("Expected Bson::String!"),
    }

    assert!(cursor.next().is_none());
}

#[test]
fn find_one_and_replace() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("find_one_and_replace");

    coll.drop().expect("Failed to drop database");

    // Insert documents
    let doc1 = doc! { "title": "Jaws" };
    let doc2 = doc! { "title": "Back to the Future" };
    let doc3 = doc! { "title": "12 Angry Men" };

    coll.insert_many(vec![doc1.clone(), doc2.clone(), doc3.clone()], None)
        .expect("Failed to insert documents into collection.");

    // Replace single document
    let result = coll.find_one_and_replace(doc2.clone(), doc3.clone(), None)
        .expect("Failed to execute find_one_and_replace command.");

    match result.unwrap().get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("Back to the Future", title),
        _ => panic!("Expected Bson::String!"),
    }

    // Validate state of collection
    let mut cursor = coll.find(None, None).expect(
        "Failed to execute find command.",
    );
    let results = cursor.next_n(3).expect("Failed to get next 3 from cursor.");
    assert_eq!(3, results.len());

    // Assert expected title of documents
    match results[0].get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("Jaws", title),
        _ => panic!("Expected Bson::String!"),
    };
    match results[1].get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("12 Angry Men", title),
        _ => panic!("Expected Bson::String!"),
    };
    match results[2].get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("12 Angry Men", title),
        _ => panic!("Expected Bson::String!"),
    };

    // Replace with 'new' option
    let mut opts = FindOneAndUpdateOptions::new();
    opts.return_document = Some(ReturnDocument::After);
    let result = coll.find_one_and_replace(doc3.clone(), doc2.clone(), Some(opts))
        .expect("Failed to execute find_one_and_replace command.");

    match result.unwrap().get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("Back to the Future", title),
        _ => panic!("Expected Bson::String!"),
    }
}

#[test]
fn find_one_and_update() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("find_one_and_update");

    coll.drop().expect("Failed to drop database");

    // Insert documents
    let doc1 = doc! { "title": "Jaws" };
    let doc2 = doc! { "title": "Back to the Future" };
    let doc3 = doc! { "title": "12 Angry Men" };

    coll.insert_many(vec![doc1.clone(), doc2.clone(), doc3.clone()], None)
        .expect("Failed to insert documents into collection.");

    // Update single document
    let update = doc! { "$set": { "director": "Robert Zemeckis" } };

    let result = coll.find_one_and_update(doc2.clone(), update, None)
        .expect("Failed to execute find_one_and_update command.");

    match result.unwrap().get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("Back to the Future", title),
        _ => panic!("Expected Bson::String!"),
    }

    // Validate state of collection
    let mut cursor = coll.find(None, None).expect(
        "Failed to execute find command.",
    );
    let results = cursor.next_n(3).expect("Failed to get next 3 from cursor.");
    assert_eq!(3, results.len());

    // Assert director attributes
    assert!(results[0].get("director").is_none());
    assert!(results[2].get("director").is_none());
    match results[1].get("director") {
        Some(&Bson::String(ref director)) => assert_eq!("Robert Zemeckis", director),
        _ => panic!("Expected Bson::String!"),
    }
}

#[test]
fn aggregate() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("aggregate");

    coll.drop().expect("Failed to drop database");

    // Insert documents
    let doc1 = doc! { "tags": ["a", "b", "c"] };
    let doc2 = doc! { "tags": ["a", "b", "d"] };
    let doc3 = doc! { "tags": ["d", "e", "f"] };

    coll.insert_many(vec![doc1.clone(), doc2.clone(), doc3.clone()], None)
        .expect("Failed to execute insert_many command.");

    // Build aggregation pipeline to unwind tag arrays and group distinct tags
    let project = doc! { "$project": { "tags": 1 } };
    let unwind = doc! { "$unwind": "$tags" };
    let group = doc! { "$group": { "_id": "$tags" } };

    // Aggregate
    let mut cursor = coll.aggregate(vec![project, unwind, group], None).expect(
        "Failed to execute aggregate command.",
    );

    let results = cursor.next_n(10).expect(
        "Failed to get next 10 from cursor.",
    );
    assert_eq!(6, results.len());

    // Grab ids from aggregated docs
    let vec: Vec<_> = results
        .iter()
        .filter_map(|bdoc| match bdoc.get("_id") {
            Some(&Bson::String(ref tag)) => Some(tag.to_owned()),
            _ => None,
        })
        .collect();

    // Validate that all distinct tags were received.
    assert_eq!(6, vec.len());
    assert!(vec.contains(&"a".to_owned()));
    assert!(vec.contains(&"b".to_owned()));
    assert!(vec.contains(&"c".to_owned()));
    assert!(vec.contains(&"d".to_owned()));
    assert!(vec.contains(&"e".to_owned()));
    assert!(vec.contains(&"f".to_owned()));
}

#[test]
fn count() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("count");

    coll.drop().expect("Failed to drop database");

    // Insert documents
    let doc1 = doc! { "title": "Jaws" };
    let doc2 = doc! { "title": "Back to the Future" };

    let mut vec = vec![doc1.clone()];
    for _ in 0..10 {
        vec.push(doc2.clone());
    }

    coll.insert_many(vec, None).expect(
        "Failed to insert documents.",
    );
    let count_doc1 = coll.count(Some(doc1), None).expect(
        "Failed to execute count.",
    );
    assert_eq!(1, count_doc1);

    let count_doc2 = coll.count(Some(doc2), None).expect(
        "Failed to execute count.",
    );
    assert_eq!(10, count_doc2);

    let count_all = coll.count(None, None).expect("Failed to execute count.");
    assert_eq!(11, count_all);

    let no_doc = doc! { "title": "Houdini" };
    let count_none = coll.count(Some(no_doc), None).expect(
        "Failed to execute count.",
    );
    assert_eq!(0, count_none);
}

#[test]
fn distinct_none() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("distinct_none");

    coll.drop().expect("Failed to drop database");
    let distinct_titles = coll.distinct("title", None, None).expect(
        "Failed to execute 'distinct'.",
    );
    assert_eq!(0, distinct_titles.len());
}

#[test]
fn distinct_one() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("distinct_one");

    coll.drop().expect("Failed to drop database");
    let doc2 = doc! { "title": "Back to the Future" };
    coll.insert_one(doc2, None).expect(
        "Failed to insert document.",
    );

    let distinct_titles = coll.distinct("title", None, None).expect(
        "Failed to execute 'distinct'.",
    );
    assert_eq!(1, distinct_titles.len());
}

#[test]
fn distinct() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("distinct");

    coll.drop().expect("Failed to drop database");

    // Insert documents
    let doc1 =
        doc! {
            "title": "Jaws",
            "director": "MB"
        };

    let doc2 = doc! { "title": "Back to the Future" };

    let doc3 =
        doc! {
            "title": "12 Angry Men",
            "director": "MB"
        };

    let mut vec = vec![doc1.clone()];
    for _ in 0..4 {
        vec.push(doc2.clone());
    }
    for _ in 0..60 {
        vec.push(doc3.clone());
    }

    coll.insert_many(vec, None).expect(
        "Failed to insert documents.",
    );

    // Distinct titles over all documents
    let distinct_titles = coll.distinct("title", None, None).expect(
        "Failed to execute 'distinct'.",
    );
    assert_eq!(3, distinct_titles.len());

    let titles: Vec<_> = distinct_titles
        .iter()
        .filter_map(|bson| match *bson {
            Bson::String(ref title) => Some(title.to_owned()),
            _ => None,
        })
        .collect();

    assert_eq!(3, titles.len());
    assert!(titles.contains(&"Jaws".to_owned()));
    assert!(titles.contains(&"Back to the Future".to_owned()));
    assert!(titles.contains(&"12 Angry Men".to_owned()));

    // Distinct titles over documents with certain director
    let filter = doc! { "director": "MB" };
    let distinct_titles = coll.distinct("title", Some(filter), None).expect(
        "Failed to execute 'distinct'.",
    );

    assert_eq!(2, distinct_titles.len());

    let titles: Vec<_> = distinct_titles
        .iter()
        .filter_map(|bson| match *bson {
            Bson::String(ref title) => Some(title.to_owned()),
            _ => None,
        })
        .collect();

    assert_eq!(2, titles.len());
    assert!(titles.contains(&"Jaws".to_owned()));
    assert!(titles.contains(&"12 Angry Men".to_owned()));
}

#[test]
fn insert_many() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("insert_many");

    coll.drop().expect("Failed to drop database");

    // Insert documents
    let doc1 = doc! { "title": "Jaws" };
    let doc2 = doc! { "title": "Back to the Future" };

    coll.insert_many(vec![doc1, doc2], None).expect(
        "Failed to insert documents.",
    );

    // Find documents
    let mut cursor = coll.find(None, None).expect(
        "Failed to execute find command.",
    );
    let results = cursor.next_n(2).expect("Failed to get next 2 from cursor.");
    assert_eq!(2, results.len());

    // Assert expected title of documents
    match results[0].get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("Jaws", title),
        _ => panic!("Expected Bson::String!"),
    }
    match results[1].get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("Back to the Future", title),
        _ => panic!("Expected Bson::String!"),
    }
}

#[test]
fn delete_one() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("delete_one");

    coll.drop().expect("Failed to drop database");

    // Insert documents
    let doc1 = doc! { "title": "Jaws" };
    let doc2 = doc! { "title": "Back to the Future" };

    coll.insert_many(vec![doc1.clone(), doc2.clone()], None)
        .expect("Failed to insert documents.");

    // Delete document
    coll.delete_one(doc2.clone(), None).expect(
        "Failed to delete document.",
    );
    let mut cursor = coll.find(None, None).expect(
        "Failed to execute find command.",
    );
    let result = match cursor.next() {
        Some(Ok(res)) => res,
        Some(Err(_)) => panic!("Received error from 'cursor.next()'."),
        None => panic!("Expected bson."),
    };

    match result.get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("Jaws", title),
        _ => panic!("Expected Bson::String!"),
    }

    assert!(cursor.next().is_none());
}

#[test]
fn delete_many() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("delete_many");

    coll.drop().expect("Failed to drop database");

    // Insert documents
    let doc1 = doc! { "title": "Jaws" };
    let doc2 = doc! { "title": "Back to the Future" };

    coll.insert_many(vec![doc1.clone(), doc2.clone(), doc2.clone()], None)
        .expect("Failed to insert documents into collection.");

    // Delete document
    coll.delete_many(doc2.clone(), None).expect(
        "Failed to delete documents.",
    );
    let mut cursor = coll.find(None, None).expect(
        "Failed to execute find command.",
    );
    let result = match cursor.next() {
        Some(Ok(res)) => res,
        Some(Err(_)) => panic!("Received error from 'cursor.next()'."),
        None => panic!("Expected bson."),
    };

    match result.get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("Jaws", title),
        _ => panic!("Expected Bson::String!"),
    }

    assert!(cursor.next().is_none());
}

#[test]
fn replace_one() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("replace_one");

    coll.drop().expect("Failed to drop database");

    // Insert documents
    let doc1 = doc! { "title": "Jaws" };
    let doc2 = doc! { "title": "Back to the Future" };
    let doc3 = doc! { "title": "12 Angry Men" };

    coll.insert_many(vec![doc1.clone(), doc2.clone(), doc3.clone()], None)
        .expect("Failed to insert documents into collection.");

    // Replace single document
    coll.replace_one(doc2.clone(), doc3.clone(), None).expect(
        "Failed to replace document.",
    );
    let mut cursor = coll.find(None, None).expect(
        "Failed to execute find command.",
    );
    let results = cursor.next_n(3).expect("Failed to get next 3 from cursor.");
    assert_eq!(3, results.len());

    // Assert expected title of documents
    match results[0].get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("Jaws", title),
        _ => panic!("Expected Bson::String!"),
    };
    match results[1].get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("12 Angry Men", title),
        _ => panic!("Expected Bson::String!"),
    };
    match results[2].get("title") {
        Some(&Bson::String(ref title)) => assert_eq!("12 Angry Men", title),
        _ => panic!("Expected Bson::String!"),
    };
}

#[test]
fn update_one() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("update_one");

    coll.drop().expect("Failed to drop database");

    // Insert documents
    let doc1 = doc! { "title": "Jaws" };
    let doc2 = doc! { "title": "Back to the Future" };
    let doc3 = doc! { "title": "12 Angry Men" };

    coll.insert_many(vec![doc1.clone(), doc2.clone(), doc3.clone()], None)
        .expect("Failed to insert documents into collection.");

    // Update single document
    let update = doc! { "$set": { "director": "Robert Zemeckis" } };

    coll.update_one(doc2.clone(), update, None).expect(
        "Failed to update document.",
    );

    let mut cursor = coll.find(None, None).expect(
        "Failed to execute find command.",
    );
    let results = cursor.next_n(3).expect("Failed to get next 3 from cursor.");
    assert_eq!(3, results.len());

    // Assert director attributes
    assert!(results[0].get("director").is_none());
    assert!(results[2].get("director").is_none());
    match results[1].get("director") {
        Some(&Bson::String(ref director)) => assert_eq!("Robert Zemeckis", director),
        _ => panic!("Expected Bson::String!"),
    }
}

#[test]
fn update_many() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("update_many");

    coll.drop().expect("Failed to drop database");

    // Insert documents
    let doc1 = doc! { "title": "Jaws" };
    let doc2 = doc! { "title": "Back to the Future" };
    let doc3 = doc! { "title": "12 Angry Men" };

    coll.insert_many(
        vec![doc1.clone(), doc2.clone(), doc3.clone(), doc2.clone()],
        None,
    ).expect("Failed to insert documents into collection.");

    // Update single document
    let update = doc! { "$set": { "director": "Robert Zemeckis" } };

    coll.update_many(doc2.clone(), update, None).expect(
        "Failed to update documents.",
    );

    let mut cursor = coll.find(None, None).expect(
        "Failed to execute find command.",
    );
    let results = cursor.next_n(4).expect("Failed to get next 4 from cursor.");
    assert_eq!(4, results.len());

    // Assert director attributes
    assert!(results[0].get("director").is_none());
    assert!(results[2].get("director").is_none());

    match results[1].get("director") {
        Some(&Bson::String(ref director)) => assert_eq!("Robert Zemeckis", director),
        _ => panic!("Expected Bson::String for director!"),
    }

    match results[3].get("director") {
        Some(&Bson::String(ref director)) => assert_eq!("Robert Zemeckis", director),
        _ => panic!("Expected Bson::String for director!"),
    }
}

#[test]
fn create_list_drop_indexes() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("create_list_drop_indexes");

    coll.drop().expect("Failed to drop database.");

    let mut opts1 = IndexOptions::new();
    opts1.name = Some("nid".to_owned());

    // Test name option
    let index1 = IndexModel::new(
        doc! {
            "n": 1,
            "id": 1
        },
        Some(opts1),
    );

    // Test negative value and index name generation
    let index2 = IndexModel::new(
        doc! {
            "test": -1,
            "height": 1,
        },
        None,
    );

    coll.create_indexes(vec![index1, index2]).unwrap();
    let mut cursor = coll.list_indexes().unwrap();
    let results = cursor.next_n(5).unwrap();

    assert_eq!(3, results.len());

    // Assert first inserted index
    match results[1].get("key") {
        Some(&Bson::Document(ref keys)) => {
            match keys.get("n") {
                Some(&Bson::I32(ref i)) => assert_eq!(1, *i),
                _ => panic!("Expected key 'n => 1'."),
            }
            match keys.get("id") {
                Some(&Bson::I32(ref i)) => assert_eq!(1, *i),
                _ => panic!("Expected key 'id => 1'."),
            }
        }
        _ => panic!("keys not found for first index."),
    }

    match results[1].get("name") {
        Some(&Bson::String(ref name)) => assert_eq!("nid", name),
        _ => panic!("Expected first index to have a name."),
    }

    // Assert second inserted index
    match results[2].get("key") {
        Some(&Bson::Document(ref keys)) => {
            match keys.get("test") {
                Some(&Bson::I32(ref i)) => assert_eq!(-1, *i),
                _ => panic!("Expected key 'test => -1'."),
            }
            match keys.get("height") {
                Some(&Bson::I32(ref i)) => assert_eq!(1, *i),
                _ => panic!("Expected key 'height => 1'."),
            }
        }
        _ => panic!("keys not found for second index."),
    }

    match results[2].get("name") {
        Some(&Bson::String(ref name)) => assert_eq!("test_-1_height_1", name),
        _ => panic!("Expected first index to have a name."),
    }

    // Drop and ensure
    coll.drop_index_string("nid".to_owned()).unwrap();
    let mut cursor = coll.list_indexes().unwrap();
    let results = cursor.next_n(5).unwrap();

    assert_eq!(2, results.len());

    coll.drop_index(
        doc! {
            "test": -1,
            "height": 1,
        },
        None,
    ).unwrap();

    let mut cursor = coll.list_indexes().unwrap();
    let results = cursor.next_n(5).unwrap();

    assert_eq!(1, results.len());
}

#[test]
fn create_text_hashed_2d_2dsphere_index() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("create_text_hashed_2d_2dsphere_index");

    coll.create_index(doc! {"a": "text" }, None).expect(
        "Failed to create text indexes",
    );

    coll.create_index(doc! { "b": "hashed" }, None).expect(
        "Failed to create hashed indexes",
    );

    coll.create_index(doc! { "c": "2d" }, None).expect(
        "Failed to create 2d indexes",
    );

    coll.create_index(doc! { "d": "2dsphere" }, None).expect(
        "Failed to create 2dsphere indexes",
    );
}

#[test]
fn block_ill_formed_index_models() {
    assert!(IndexModel::new(doc!{ "test": 1.1 }, None).name().is_err());
    assert!(
        IndexModel::new(doc!{ "test": 1i64 }, None)
            .name()
            .is_err()
    );
    assert!(
        IndexModel::new(doc!{ "test": "arbitrystr" }, None)
            .name()
            .is_err()
    );
}

#[test]
fn create_query_text_index() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("create_query_text_index");

    coll.drop().expect("Failed to drop collection");

    let mut index_opt = IndexOptions::new();
    index_opt.weights = Some(doc!{
        "title": 10,
        "content": 5
    });
    coll.create_index(
        doc!{
            "title": "text",
            "content": "text",
        },
        Some(index_opt),
    ).unwrap();

    let doc1 = doc! { "title": "keyword keyword keyword", "content": "nonkeyword", "id": 1 };
    let doc2 = doc! { "title": "nonkeyword", "content": "keyword keyword keyword", "id": 2 };
    let doc3 = doc! { "title": "nonkeyword", "content": "nonkeyword", "id": 3 };

    coll.insert_many(vec![doc1.clone(), doc2.clone(), doc3.clone()], None)
        .unwrap();

    let count = coll.count(Some(doc!{ "$text": { "$search": "keyword" }}), None)
        .unwrap();
    assert_eq!(count, 2);

    let mut opts = FindOptions::new();
    opts.projection = Some(doc!{ "score": { "$meta": "textScore" }});
    opts.sort = Some(doc!{ "score": { "$meta": "textScore" }});

    let mut cursor = coll.find(
        Some(doc!{ "$text": { "$search": "keyword" }}),
        Some(opts),
    ).unwrap();
    let results = cursor.next_n(2).unwrap();

    match results[0].get("id").unwrap() {
        &Bson::I32(id) => assert_eq!(1, id),
        _ => panic!("Why is id not a i32?"),
    };

    match results[1].get("id").unwrap() {
        &Bson::I32(id) => assert_eq!(2, id),
        _ => panic!("Why is id not a i32?"),
    };

    let count = coll.count(
        Some(doc!{ "$text": { "$search": "keyword nonkeyword" }}),
        None,
    ).unwrap();
    assert_eq!(count, 3);
}

#[test]
fn drop_all_indexes() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-coll");
    let coll = db.collection("drop_all_indexes");

    coll.drop().expect("Failed to drop database.");

    let mut opts1 = IndexOptions::new();
    opts1.name = Some("nid".to_owned());

    // Test name option
    let index1 = IndexModel::new(
        doc! {
            "n": 1,
            "id": 1,
        },
        Some(opts1),
    );

    // Test negative value and index name generation
    let index2 = IndexModel::new(
        doc! {
            "test": -1,
            "height": 1,
        },
        None,
    );

    coll.create_indexes(vec![index1, index2]).unwrap();
    coll.drop_indexes().unwrap();
    let mut cursor = coll.list_indexes().unwrap();
    let results = cursor.next_n(5).unwrap();

    assert_eq!(1, results.len());
}
