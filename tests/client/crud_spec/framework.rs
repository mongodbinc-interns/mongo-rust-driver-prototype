macro_rules! run_aggregate_test {
    ( $db:expr, $coll:expr, $pipeline:expr, $opt:expr, $out:expr, $outcome:expr ) => {{
        let mut cursor = $coll.aggregate($pipeline, $opt).unwrap();

        if !$out {
            let array = match $outcome.result {
                Bson::Array(ref arr) => arr.clone(),
                _ => panic!("Invalid `result` of aggregate test")
            };

            for bson in array {
                let b2 = &Bson::Document(cursor.next().unwrap().unwrap());
                assert!(eq::bson_eq(&bson, b2));
            }

            assert!(!cursor.has_next().expect("Failed to execute 'has_next()' on cursor"));
        }

        check_coll!($db, $coll, $outcome.collection);
    }};
}

macro_rules! run_count_test {
    ( $db:expr, $coll:expr, $filter:expr, $opt:expr, $outcome:expr ) => {{
        let n = $coll.count($filter, $opt).unwrap();
        assert!($outcome.result.int_eq(n));
        check_coll!($db, $coll, $outcome.collection);
    }};
}

macro_rules! run_delete_test {
    ( $db:expr, $coll:expr, $filter:expr, $outcome:expr, $many:expr ) => {{
        let count = if $many {
                        $coll.delete_many($filter, None)
                    } else {
                        $coll.delete_one($filter, None)
                    };

        let expected = count.unwrap().deleted_count;

        let actual = match $outcome.result {
            Bson::Document(ref doc) => doc.get("deletedCount").unwrap(),
            _ => panic!("`delete` test result should be a document")
        };

        assert!(actual.int_eq(expected as i64));
        check_coll!($db, $coll, $outcome.collection);
    }};
}

macro_rules! run_distinct_test {
    ( $db:expr, $coll:expr, $field_name:expr, $filter:expr, $outcome:expr ) => {{
        let actual = $coll.distinct(&$field_name, $filter, None).unwrap();

        let expected = match $outcome.result {
            Bson::Array(ref arr) => arr.clone(),
            _ => panic!("Invalid `result` of distinct test")
        };

        assert_eq!(actual.len(), expected.len());

        for i in 0..actual.len() {
            assert!(eq::bson_eq(&actual[i], &expected[i]));
        }

        check_coll!($db, $coll, $outcome.collection);
    }};
}

macro_rules! run_find_one_and_delete_test {
    ( $db:expr, $coll:expr, $filter:expr, $opt:expr, $outcome:expr ) => {{
        let doc_opt = $coll.find_one_and_delete($filter, $opt).unwrap();

        let bson = match doc_opt {
            Some(ref doc) => Bson::Document(doc.clone()),
            None => Bson::Null
        };

        assert!(eq::bson_eq(&bson, &$outcome.result));
        check_coll!($db, $coll, $outcome.collection);
    }};
}

macro_rules! run_find_one_and_replace_test {
    ( $db:expr, $coll:expr, $filter:expr, $replacement:expr, $opt:expr,
      $outcome:expr ) => {{
          let doc_opt = $coll.find_one_and_replace($filter, $replacement,
                                                   $opt).unwrap();

          let bson = match doc_opt {
              Some(ref doc) => Bson::Document(doc.clone()),
              None => Bson::Null
          };

          assert!(eq::bson_eq(&bson, &$outcome.result));
          check_coll!($db, $coll, $outcome.collection);
    }};
}

macro_rules! run_find_one_and_update_test {
    ( $db:expr, $coll:expr, $filter:expr, $update:expr, $opt:expr,
      $outcome:expr ) => {{
          let doc_opt = $coll.find_one_and_update($filter, $update, $opt).unwrap();

          let bson = match doc_opt {
              Some(ref doc) => Bson::Document(doc.clone()),
              None => Bson::Null
          };

          assert!(eq::bson_eq(&bson, &$outcome.result));
          check_coll!($db, $coll, $outcome.collection);
    }};
}

macro_rules! run_find_test {
    ( $db:expr, $coll:expr, $filter:expr, $opt:expr, $outcome:expr ) => {{
        let mut cursor = $coll.find($filter, $opt).unwrap();

        let array = match $outcome.result {
            Bson::Array(ref arr) => arr.clone(),
            _ => panic!("Invalid `result` of find test")
        };

        for bson in array {
            assert!(eq::bson_eq(&bson, &Bson::Document(cursor.next().unwrap().unwrap())));
        }

        assert!(!cursor.has_next().expect("Failed to execute 'has_next()' on cursor"));
        check_coll!($db, $coll, $outcome.collection);
    }};
}

macro_rules! run_insert_many_test {
    ( $db:expr, $coll:expr, $docs:expr, $outcome:expr ) => {{
        let options = Some(InsertManyOptions::new());
        let inserted = $coll.insert_many($docs, options).unwrap().inserted_ids.unwrap();
        let ids_bson = match $outcome.result {
            Bson::Document(ref doc) => doc.get("insertedIds").unwrap(),
            _ => panic!("`insert_many` test result should be a document")
        };

        let ids = match *ids_bson {
            Bson::Array(ref arr) => arr.into_iter(),
            _ => panic!("`insertedIds` test result should be an array")
        };

        let mut actual_ids = inserted.values();

        for expected_id in ids {
            assert!(eq::bson_eq(&expected_id, actual_ids.next().unwrap()));
        }

        check_coll!($db, $coll, $outcome.collection);
    }};
}

macro_rules! run_insert_one_test {
    ( $db:expr, $coll:expr, $doc:expr, $outcome:expr ) => {{
        let inserted = $coll.insert_one($doc, None).unwrap().inserted_id.unwrap();
        let id = match $outcome.result {
            Bson::Document(ref doc) => doc.get("insertedId").unwrap(),
            _ => panic!("`insert_one` test result should be a document")
        };

        assert!(eq::bson_eq(&id, &inserted));
        check_coll!($db, $coll, $outcome.collection);
    }};
}

macro_rules! run_replace_one_test {
    ( $db:expr, $coll:expr, $filter:expr, $replacement:expr, $upsert:expr,
        $outcome:expr ) => {{
            let options = ReplaceOptions { upsert: $upsert, write_concern: None };
            let actual = $coll.replace_one($filter, $replacement, Some(options)).unwrap();

            let (matched, modified, upserted) = match $outcome.result {
                Bson::Document(ref doc) => (
                    doc.get("matchedCount").unwrap(),
                    doc.get("modifiedCount").unwrap(),
                    doc.get("upsertedId"),
                    ),
                _ => panic!("`delete` test result should be a document")
            };

            assert!(matched.int_eq(actual.matched_count as i64));
            assert!(modified.int_eq(actual.modified_count as i64));

            let id = match actual.upserted_id {
                Some(Bson::Document(ref doc)) => doc.get("_id"),
                _ => None
            };

            match (upserted, id) {
                (None, None) => (),
                (Some(ref bson1), Some(ref bson2)) =>
                assert!(eq::bson_eq(&bson1, &bson2)),
                _ => panic!("Wrong `upsertedId` returned")
            };

            check_coll!($db, $coll, $outcome.collection);
    }};
}

macro_rules! run_update_test {
    ( $db:expr, $coll:expr, $filter:expr, $update:expr, $options:expr,
      $many:expr, $outcome:expr ) => {{
          let result = if $many {
                           $coll.update_many($filter, $update, $options)
                       } else {
                           $coll.update_one($filter, $update, $options)
                       };

          let actual = result.unwrap();

          let (matched, modified, upserted) = match $outcome.result {
              Bson::Document(ref doc) => (
                  doc.get("matchedCount").unwrap(),
                  doc.get("modifiedCount").unwrap(),
                  doc.get("upsertedId"),
              ),
              _ => panic!("`update` test result should be a document")
          };

          assert!(matched.int_eq(actual.matched_count as i64));
          assert!(modified.int_eq(actual.modified_count as i64));

          let id = match actual.upserted_id {
	          Some(Bson::Document(ref doc)) => doc.get("_id"),
              _ => None
          };

          match (upserted, id) {
              (None, None) => (),
              (Some(ref bson1), Some(ref bson2)) =>
                  assert!(eq::bson_eq(&bson1, &bson2)),
              _ => panic!("Wrong `upsertedId` returned")
          };

          check_coll!($db, $coll, $outcome.collection);
    }};
}

#[macro_export]
macro_rules! run_suite {
    ( $file:expr, $coll:expr ) => {{
        let json = Value::from_file($file).unwrap();
        let suite = json.get_suite().unwrap();
        let client =  Client::connect("localhost", 27017).unwrap();
        let db = client.db("test");
        let coll = db.collection($coll);

        for test in suite.tests {
            coll.drop().unwrap();
            let options = Some(InsertManyOptions::new());
            coll.insert_many(suite.data.clone(), options).unwrap();

            match test.operation {
                Arguments::Aggregate { pipeline, options, out } =>
                    run_aggregate_test!(db, coll, pipeline, Some(options), out,
                                        test.outcome),
                Arguments::Count { filter, options } =>
                    run_count_test!(db, coll, filter, Some(options), test.outcome),
                Arguments::Delete { filter, many } =>
                    run_delete_test!(db, coll, filter, test.outcome, many),
                Arguments::Distinct { field_name, filter } =>
                    run_distinct_test!(db, coll, field_name, filter, test.outcome),
                Arguments::Find { filter, options } =>
                    run_find_test!(db, coll, filter, Some(options), test.outcome),
                Arguments::FindOneAndDelete { filter, options } =>
                    run_find_one_and_delete_test!(db, coll, filter, Some(options), test.outcome),
                Arguments::FindOneAndReplace { filter, replacement, options } =>
                    run_find_one_and_replace_test!(db, coll, filter, replacement, Some(options), test.outcome),
                Arguments::FindOneAndUpdate { filter, update, options } =>
                    run_find_one_and_update_test!(db, coll, filter, update, Some(options), test.outcome),
                Arguments::InsertMany { documents } =>
                    run_insert_many_test!(db, coll, documents, test.outcome),
                Arguments::InsertOne { document } =>
                    run_insert_one_test!(db, coll, document, test.outcome),
                Arguments::ReplaceOne { filter, replacement, upsert } =>
                    run_replace_one_test!(db, coll, filter, replacement, Some(upsert), test.outcome),
                Arguments::Update { filter, update, upsert, many } => {
                    let options = UpdateOptions { upsert: Some(upsert), ..Default::default() };

                    run_update_test!(db, coll, filter, update, Some(options), many, test.outcome)

                }
            };
        }
    }};
}

#[macro_export]
macro_rules! check_coll {
    ( $db:expr, $coll:expr, $coll_opt:expr ) => {{
        let outcome_coll = match $coll_opt {
            Some(ref coll) => coll.clone(),
            None => continue
        };

        let coll = match outcome_coll.name {
            Some(ref str) => $db.collection(&str),
            None => $db.collection(&$coll.name())
        };

        let mut cursor = coll.find(None, None).unwrap();

        for doc in &outcome_coll.data {
            assert!(eq::bson_eq(&Bson::Document(doc.clone()),
                                &Bson::Document(cursor.next().unwrap().unwrap())));
        }

        assert!(!cursor.has_next().expect("Failed to execute 'has_next()' on cursor"));

        $coll.drop().unwrap();
    }};
}
