use bson::Bson;
use json::FromValue;

use mongodb::coll::options::{AggregateOptions, CountOptions, FindOneAndDeleteOptions,
                             FindOneAndUpdateOptions, FindOptions, ReturnDocument};

use serde_json::{Map, Value};

impl FromValue for AggregateOptions {
    fn from_json(object: &Map<String, Value>) -> AggregateOptions {
        let mut options = AggregateOptions::new();

        if let Some(Bson::I64(x)) = object.get("batchSize").map(Value::clone).map(Into::into) {
            options.batch_size = x as i32;
        };

        options
    }
}

impl FromValue for CountOptions {
    fn from_json(object: &Map<String, Value>) -> CountOptions {
        let mut options = CountOptions::new();

        if let Some(Bson::I64(x)) = object.get("skip").map(Value::clone).map(Into::into) {
            options.skip = Some(x);
        }

        if let Some(Bson::I64(x)) = object.get("limit").map(Value::clone).map(Into::into) {
            options.limit = Some(x);
        }

        options
    }
}

impl FromValue for FindOptions {
    fn from_json(object: &Map<String, Value>) -> FindOptions {
        let mut options = FindOptions::new();

        if let Some(Bson::Document(doc)) = object.get("sort").map(Value::clone).map(Into::into) {
            options.sort = Some(doc);
        }

        if let Some(Bson::I64(x)) = object.get("skip").map(Value::clone).map(Into::into) {
            options.skip = Some(x);
        }

        if let Some(Bson::I64(x)) = object.get("limit").map(Value::clone).map(Into::into) {
            options.limit = Some(x);
        }

        if let Some(Bson::I64(x)) = object.get("batchSize").map(Value::clone).map(Into::into) {
            options.batch_size = Some(x as i32);
        }

        options

    }
}

impl FromValue for FindOneAndDeleteOptions {
    fn from_json(object: &Map<String, Value>) -> FindOneAndDeleteOptions {
        let mut options = FindOneAndDeleteOptions::new();

        if let Some(Bson::Document(projection)) = object.get("projection").map(Value::clone).map(Into::into) {
            options.projection = Some(projection);
        }

        if let Some(Bson::Document(sort)) = object.get("sort").map(Value::clone).map(Into::into) {
            options.sort = Some(sort);
        }

        options
    }
}

impl FromValue for FindOneAndUpdateOptions {
    fn from_json(object: &Map<String, Value>) -> FindOneAndUpdateOptions {
        let mut options = FindOneAndUpdateOptions::new();

        if let Some(Bson::Document(projection)) = object.get("projection").map(Value::clone).map(Into::into) {
            options.projection = Some(projection);
        }

        if let Some(Bson::String(s)) = object.get("returnDocument").map(Value::clone).map(Into::into) {
            match s.as_ref() {
                "After" => options.return_document = Some(ReturnDocument::After),
                "Before" => options.return_document = Some(ReturnDocument::Before),
                _ => {}
            };
        }


        if let Some(Bson::Document(sort)) = object.get("sort").map(Value::clone).map(Into::into) {
            options.sort = Some(sort);
        }

        if let Some(Bson::Boolean(upsert)) = object.get("upsert").map(Value::clone).map(Into::into) {
            options.upsert = Some(upsert);
        }

        options
    }
}
