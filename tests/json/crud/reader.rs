use bson::{Bson, Document};
use serde_json::{self, Map, Value};
use std::fs::File;

use super::arguments::Arguments;
use super::outcome::Outcome;

pub struct Test {
    pub operation: Arguments,
    pub outcome: Outcome,
}

impl Test {
    fn from_json(object: &Map<String, Value>) -> Result<Test, String> {
        macro_rules! res_or_err {
            ($exp:expr) => { match $exp {
                Ok(a) => a,
                Err(s) => return Err(s)
            }};
        }

        let op = val_or_err!(object.get("operation"),
                             Some(&Value::Object(ref obj)) => obj.clone(),
                             "`operation` must be an object");

        let args_obj = val_or_err!(op.get("arguments"),
                                   Some(&Value::Object(ref obj)) => obj.clone(),
                                   "`arguments` must be an object");

        let name = val_or_err!(op.get("name"),
                               Some(&Value::String(ref s)) => s,
                               "`name` must be a string");

        let args = match name.as_ref() {
            "aggregate" => res_or_err!(Arguments::aggregate_from_json(&args_obj)),
            "count" => Arguments::count_from_json(&args_obj),
            "deleteMany" => res_or_err!(Arguments::delete_from_json(&args_obj, true)),
            "deleteOne" => res_or_err!(Arguments::delete_from_json(&args_obj, false)),
            "distinct" => res_or_err!(Arguments::distinct_from_json(&args_obj)),
            "find" => Arguments::find_from_json(&args_obj),
            "findOneAndDelete" => res_or_err!(Arguments::find_one_and_delete_from_json(&args_obj)),
            "findOneAndReplace" => {
                res_or_err!(Arguments::find_one_and_replace_from_json(&args_obj))
            }
            "findOneAndUpdate" => res_or_err!(Arguments::find_one_and_update_from_json(&args_obj)),
            "insertMany" => res_or_err!(Arguments::insert_many_from_json(&args_obj)),
            "insertOne" => res_or_err!(Arguments::insert_one_from_json(&args_obj)),
            "replaceOne" => res_or_err!(Arguments::replace_one_from_json(&args_obj)),
            "updateMany" => res_or_err!(Arguments::update_from_json(&args_obj, true)),
            "updateOne" => res_or_err!(Arguments::update_from_json(&args_obj, false)),
            _ => return Err(String::from("Invalid operation name")),
        };


        let outcome_obj = val_or_err!(object.get("outcome"),
                                      Some(&Value::Object(ref obj)) => obj.clone(),
                                      "`outcome` must be an object");

        let outcome = match Outcome::from_json(&outcome_obj) {
            Ok(outcome) => outcome,
            Err(s) => return Err(s),
        };

        Ok(Test {
            operation: args,
            outcome: outcome,
        })
    }
}

pub struct Suite {
    pub data: Vec<Document>,
    pub tests: Vec<Test>,
}

fn get_data(object: &Map<String, Value>) -> Result<Vec<Document>, String> {
    let array = val_or_err!(object.get("data"),
                            Some(&Value::Array(ref arr)) => arr.clone(),
                            "No `data` array found");
    let mut data = vec![];

    for json in array {
        match Bson::from_json(&json) {
            Bson::Document(doc) => data.push(doc),
            _ => return Err(String::from("`data` array must contain only objects")),
        }
    }

    Ok(data)
}

fn get_tests(object: &Map<String, Value>) -> Result<Vec<Test>, String> {
    let array = val_or_err!(object.get("tests"),
                            Some(&Value::Array(ref array)) => array.clone(),
                            "No `tests` array found");

    let mut tests = vec![];

    for json in array {
        let obj = val_or_err!(json,
                              Value::Object(ref obj) => obj.clone(),
                              "`tests` array must only contain objects");

        let test = match Test::from_json(&obj) {
            Ok(test) => test,
            Err(s) => return Err(s),
        };

        tests.push(test);
    }

    Ok(tests)
}

pub trait SuiteContainer: Sized {
    fn from_file(path: &str) -> Result<Self, String>;
    fn get_suite(&self) -> Result<Suite, String>;
}

impl SuiteContainer for Value {
    fn from_file(path: &str) -> Result<Value, String> {
        let mut file = File::open(path).expect(&format!("Unable to open file: {}", path));
        Ok(serde_json::from_reader(&mut file).expect(&format!("Invalid JSON file: {}", path)))
    }

    fn get_suite(&self) -> Result<Suite, String> {
        let object = val_or_err!(*self,
                                 Value::Object(ref object) => object.clone(),
                                 "`get_suite` requires a JSON object");

        let data = try!(get_data(&object));
        let tests = try!(get_tests(&object));

        Ok(Suite {
            data: data,
            tests: tests,
        })
    }
}
