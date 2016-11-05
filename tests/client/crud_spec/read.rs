use bson::Bson;
use json::crud::arguments::Arguments;
use json::crud::reader::SuiteContainer;
use json::eq::{self, NumEq};
use mongodb::{Client, ThreadedClient};
use mongodb::coll::options::{InsertManyOptions, ReplaceOptions, UpdateOptions};
use mongodb::db::ThreadedDatabase;
use rustc_serialize::json::Json;

#[test]
fn aggregate() {
    run_suite!("tests/json/data/specs/source/crud/tests/read/aggregate.json",
               "aggregate");
}

#[test]
fn count() {
    run_suite!("tests/json/data/specs/source/crud/tests/read/count.json",
               "count");
}

#[test]
fn distinct() {
    run_suite!("tests/json/data/specs/source/crud/tests/read/distinct.json",
               "distinct");
}

#[test]
fn find() {
    run_suite!("tests/json/data/specs/source/crud/tests/read/find.json",
               "find");
}
