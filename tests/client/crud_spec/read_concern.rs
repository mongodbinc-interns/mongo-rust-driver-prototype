use bson::Bson;
use json::crud::arguments::Arguments;
use json::crud::reader::SuiteContainer;
use json::eq::{self, NumEq};
use mongodb::{Client, ClientOptions, ThreadedClient};
use mongodb::coll::options::{InsertManyOptions, ReplaceOptions, UpdateOptions};
use mongodb::common::{ReadConcern, ReadConcernLevel};
use mongodb::db::ThreadedDatabase;
use serde_json::Value;

#[test]
fn aggregate() {
    // Skip for MongoDB < 3.2 since it does not support read concerns.
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-db-concern_aggregate");
    db.drop_database().unwrap();
    let db_version = db.version().unwrap();
    if db_version.major <= 3 && db_version.minor < 2 {
        return;
    }

    let mut options = ClientOptions::new();
    options.max_time_ms = Some(2000);
    options.read_concern = Some(ReadConcern::new(ReadConcernLevel::Majority));
    run_suite_with_options!(
        "tests/json/data/specs/source/crud/tests/read/aggregate.json",
        "concern_aggregate",
        options
    );
}

#[test]
fn count() {
    let mut options = ClientOptions::new();
    options.max_time_ms = Some(2000);
    options.read_concern = Some(ReadConcern::new(ReadConcernLevel::Majority));
    run_suite_with_options!(
        "tests/json/data/specs/source/crud/tests/read/count.json",
        "concern_count",
        options
    );
}

#[test]
fn distinct() {
    let mut options = ClientOptions::new();
    options.max_time_ms = Some(2000);
    options.read_concern = Some(ReadConcern::new(ReadConcernLevel::Majority));
    run_suite_with_options!(
        "tests/json/data/specs/source/crud/tests/read/distinct.json",
        "concern_distinct",
        options
    );
}

#[test]
fn find() {
    let mut options = ClientOptions::new();
    options.max_time_ms = Some(2000);
    options.read_concern = Some(ReadConcern::new(ReadConcernLevel::Majority));
    run_suite_with_options!(
        "tests/json/data/specs/source/crud/tests/read/find.json",
        "concern_find",
        options
    );
}
