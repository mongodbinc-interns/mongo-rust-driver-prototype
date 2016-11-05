use bson::Bson;
use json::crud::arguments::Arguments;
use json::crud::reader::SuiteContainer;
use json::eq::{self, NumEq};
use mongodb::{Client, ThreadedClient};
use mongodb::coll::options::{InsertManyOptions, ReplaceOptions, UpdateOptions};
use mongodb::db::ThreadedDatabase;
use rustc_serialize::json::Json;

#[test]
fn delete_many() {
    run_suite!("tests/json/data/specs/source/crud/tests/write/deleteMany.json",
               "delete_many");
}

#[test]
fn delete_one() {
    run_suite!("tests/json/data/specs/source/crud/tests/write/deleteOne.json",
               "delete_one");
}

#[test]
fn find_one_and_delete() {
    run_suite!("tests/json/data/specs/source/crud/tests/write/findOneAndDelete.json",
               "find_one_and_delete_one");
}

#[test]
fn find_one_and_replace() {
    run_suite!("tests/json/data/specs/source/crud/tests/write/findOneAndReplace.json",
               "find_one_and_replace_one");
}

#[test]
fn find_one_and_update() {
    run_suite!("tests/json/data/specs/source/crud/tests/write/findOneAndUpdate.json",
               "find_one_and_update_one");
}

#[test]
fn insert_many() {
    run_suite!("tests/json/data/specs/source/crud/tests/write/insertMany.json",
               "insert_many");
}

#[test]
fn insert_one() {
    run_suite!("tests/json/data/specs/source/crud/tests/write/insertOne.json",
               "insert_one");
}

#[test]
fn replace_one() {
    run_suite!("tests/json/data/specs/source/crud/tests/write/replaceOne.json",
               "replace_one");
}

#[test]
fn update_many() {
    run_suite!("tests/json/data/specs/source/crud/tests/write/updateMany.json",
               "update_many");
}

#[test]
fn update_one() {
    run_suite!("tests/json/data/specs/source/crud/tests/write/updateOne.json",
               "update_one");
}
