use bson::Bson;
use json::FromValue;

use mongodb::coll::options::{AggregateOptions, CountOptions, FindOneAndDeleteOptions,
                             FindOneAndUpdateOptions, FindOptions, ReturnDocument};

use serde_json::{Map, Value};

impl FromValue for AggregateOptions {
    fn from_json(object: &Map<String, Value>) -> AggregateOptions {
        let mut options = AggregateOptions::new();

        options.batch_size = match object.get("batchSize") {
            Some(&Value::Number(ref x)) => x
                .as_i64().map(|v| v as i32)
                .or(x.as_f64().map(|v| v as i32))
                .expect("Invalid numerical format"),
            _ => options.batch_size,
        };

        options
    }
}

impl FromValue for CountOptions {
    fn from_json(object: &Map<String, Value>) -> CountOptions {
        let mut options = CountOptions::new();

        options.skip = match object.get("skip") {
            Some(&Value::Number(ref x)) => x
                .as_i64()
                .or(x.as_f64().map(|v| v as i64)),
            _ => options.skip,
        };

        options.limit = match object.get("limit") {
            Some(&Value::Number(ref x)) => x
                .as_i64()
                .or(x.as_f64().map(|v| v as i64)),
            _ => options.limit,
        };

        options
    }
}

impl FromValue for FindOptions {
    fn from_json(object: &Map<String, Value>) -> FindOptions {
        let mut options = FindOptions::new();

        let f = |x| Some(Bson::from_json(x));

        options.sort = match object.get("sort").and_then(f) {
            Some(Bson::Document(doc)) => Some(doc),
            _ => None,
        };

        options.skip = match object.get("skip") {
            Some(&Value::Number(ref x)) => x
                .as_i64()
                .or(x.as_f64().map(|v| v as i64)),
            _ => options.skip,
        };

        options.limit = match object.get("limit") {
            Some(&Value::Number(ref x)) => x
                .as_i64()
                .or(x.as_f64().map(|v| v as i64)),
            _ => options.limit,
        };

        options.batch_size = match object.get("batchSize") {
            Some(&Value::Number(ref x)) => x
                .as_i64().map(|v| v as i32)
                .or(x.as_f64().map(|v| v as i32)),
            _ => options.batch_size,
        };
        options

    }
}

impl FromValue for FindOneAndDeleteOptions {
    fn from_json(object: &Map<String, Value>) -> FindOneAndDeleteOptions {
        let mut options = FindOneAndDeleteOptions::new();

        if let Some(Bson::Document(projection)) = object.get("projection").map(Bson::from_json) {
            options.projection = Some(projection);
        }

        if let Some(Bson::Document(sort)) = object.get("sort").map(Bson::from_json) {
            options.sort = Some(sort);
        }

        options
    }
}

impl FromValue for FindOneAndUpdateOptions {
    fn from_json(object: &Map<String, Value>) -> FindOneAndUpdateOptions {
        let mut options = FindOneAndUpdateOptions::new();

        if let Some(Bson::Document(projection)) = object.get("projection").map(Bson::from_json) {
            options.projection = Some(projection);
        }

        if let Some(Bson::String(s)) = object.get("returnDocument").map(Bson::from_json) {
            match s.as_ref() {
                "After" => options.return_document = Some(ReturnDocument::After),
                "Before" => options.return_document = Some(ReturnDocument::Before),
                _ => {}
            };
        }


        if let Some(Bson::Document(sort)) = object.get("sort").map(Bson::from_json) {
            options.sort = Some(sort);
        }

        if let Some(Bson::Boolean(upsert)) = object.get("upsert").map(Bson::from_json) {
            options.upsert = Some(upsert);
        }

        options
    }
}
