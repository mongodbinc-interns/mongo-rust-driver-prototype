use bson::{Bson, Document};
use json::options::FromValue;
use mongodb::coll::options::{AggregateOptions, CountOptions, FindOneAndDeleteOptions,
                             FindOneAndUpdateOptions, FindOptions};
use serde_json::Map;

pub enum Arguments {
    Aggregate {
        pipeline: Vec<Document>,
        options: AggregateOptions,
        out: bool,
    },
    Count {
        filter: Option<Document>,
        options: CountOptions,
    },
    Delete { filter: Document, many: bool },
    Distinct {
        field_name: String,
        filter: Option<Document>,
    },
    Find {
        filter: Option<Document>,
        options: FindOptions,
    },
    FindOneAndDelete {
        filter: Document,
        options: FindOneAndDeleteOptions,
    },
    FindOneAndReplace {
        filter: Document,
        replacement: Document,
        options: FindOneAndUpdateOptions,
    },
    FindOneAndUpdate {
        filter: Document,
        update: Document,
        options: FindOneAndUpdateOptions,
    },
    InsertOne { document: Document },
    InsertMany { documents: Vec<Document> },
    ReplaceOne {
        filter: Document,
        replacement: Document,
        upsert: bool,
    },
    Update {
        filter: Document,
        update: Document,
        upsert: bool,
        many: bool,
    },
}

impl Arguments {
    pub fn aggregate_from_json(object: &Map<String, Value>) -> Result<Arguments, String> {
        let options = AggregateOptions::from_json(object);

        let f = |x| Some(Bson::from_json(x));

        let array = val_or_err!(object.get("pipeline").and_then(f),
                                   Some(Bson::Array(arr)) => arr,
                                   "`aggregate` requires pipeline array");

        let mut docs = vec![];
        let mut out = false;

        for bson in array {
            let doc = match bson {
                Bson::Document(doc) => {
                    out = out || doc.contains_key("$out");
                    doc
                }
                _ => return Err(String::from("aggregate pipeline can only contain documents")),
            };

            docs.push(doc);
        }

        Ok(Arguments::Aggregate {
            pipeline: docs,
            options: options,
            out: out,
        })
    }

    pub fn count_from_json(object: &Map<String, Value>) -> Arguments {
        let options = CountOptions::from_json(object);

        let f = |x| Some(Bson::from_json(x));
        let filter = match object.get("filter").and_then(f) {
            Some(Bson::Document(doc)) => Some(doc),
            _ => None,
        };

        Arguments::Count {
            filter: filter,
            options: options,
        }
    }

    pub fn delete_from_json(object: &Map<String, Value>, many: bool) -> Result<Arguments, String> {
        let f = |x| Some(Bson::from_json(x));
        let document = val_or_err!(object.get("filter").and_then(f),
                                   Some(Bson::Document(doc)) => doc,
                                   "`delete` requires document");

        Ok(Arguments::Delete {
            filter: document,
            many: many,
        })
    }

    pub fn distinct_from_json(object: &Map<String, Value>) -> Result<Arguments, String> {
        let f = |x| Some(Bson::from_json(x));
        let field_name = val_or_err!(object.get("fieldName").and_then(f),
                                     Some(Bson::String(ref s)) => s.to_owned(),
                                     "`distinct` requires field name");

        let f = |x| Some(Bson::from_json(x));
        let filter = match object.get("filter").and_then(f) {
            Some(Bson::Document(doc)) => Some(doc),
            _ => None,
        };

        Ok(Arguments::Distinct {
            field_name: field_name,
            filter: filter,
        })
    }

    pub fn find_from_json(object: &Map<String, Value>) -> Arguments {
        let options = FindOptions::from_json(object);

        let f = |x| Some(Bson::from_json(x));
        let filter = match object.get("filter").and_then(f) {
            Some(Bson::Document(doc)) => Some(doc),
            _ => None,
        };

        Arguments::Find {
            filter: filter,
            options: options,
        }
    }

    pub fn find_one_and_delete_from_json(object: &Map<String, Value>) -> Result<Arguments, String> {
        let options = FindOneAndDeleteOptions::from_json(object);

        let f = |x| Some(Bson::from_json(x));
        let filter = val_or_err!(object.get("filter").and_then(f),
                                 Some(Bson::Document(doc)) => doc,
                                 "`find_one_and_delete` requires filter document");

        Ok(Arguments::FindOneAndDelete {
            filter: filter,
            options: options,
        })
    }

    pub fn find_one_and_replace_from_json(object: &Map<String, Value>) -> Result<Arguments, String> {
        let options = FindOneAndUpdateOptions::from_json(object);

        let f = |x| Some(Bson::from_json(x));
        let filter = val_or_err!(object.get("filter").and_then(f),
                                 Some(Bson::Document(doc)) => doc,
                                 "`find_one_and_update` requires filter document");

        let f = |x| Some(Bson::from_json(x));
        let replacement = val_or_err!(object.get("replacement").and_then(f),
                                 Some(Bson::Document(doc)) => doc,
                                 "`find_one_and_replace` requires replacement document");

        Ok(Arguments::FindOneAndReplace {
            filter: filter,
            replacement: replacement,
            options: options,
        })
    }

    pub fn find_one_and_update_from_json(object: &Map<String, Value>) -> Result<Arguments, String> {
        let options = FindOneAndUpdateOptions::from_json(object);

        let f = |x| Some(Bson::from_json(x));
        let filter = val_or_err!(object.get("filter").and_then(f),
                                 Some(Bson::Document(doc)) => doc,
                                 "`find_one_and_update` requires filter document");

        let f = |x| Some(Bson::from_json(x));
        let update = val_or_err!(object.get("update").and_then(f),
                                 Some(Bson::Document(doc)) => doc,
                                 "`find_one_and_update` requires update document");

        Ok(Arguments::FindOneAndUpdate {
            filter: filter,
            update: update,
            options: options,
        })
    }

    pub fn insert_many_from_json(object: &Map<String, Value>) -> Result<Arguments, String> {
        let f = |x| Some(Bson::from_json(x));

        let bsons = val_or_err!(object.get("documents").and_then(f),
                                Some(Bson::Array(arr)) => arr,
                                "`insert_many` requires documents");

        let mut docs = vec![];

        for bson in bsons.into_iter() {
            match bson {
                Bson::Document(doc) => docs.push(doc),
                _ => return Err(String::from("`insert_many` can only insert documents")),
            };
        }

        Ok(Arguments::InsertMany { documents: docs })
    }

    pub fn insert_one_from_json(object: &Map<String, Value>) -> Result<Arguments, String> {
        let f = |x| Some(Bson::from_json(x));
        let document = val_or_err!(object.get("document").and_then(f),
                                   Some(Bson::Document(doc)) => doc,
                                   "`delete_one` requires document");

        Ok(Arguments::InsertOne { document: document })
    }

    pub fn replace_one_from_json(object: &Map<String, Value>) -> Result<Arguments, String> {
        let f = |x| Some(Bson::from_json(x));
        let filter = val_or_err!(object.get("filter").and_then(f),
                                 Some(Bson::Document(doc)) => doc,
                                 "`update` requires filter document");

        let f = |x| Some(Bson::from_json(x));
        let replacement = val_or_err!(object.get("replacement").and_then(f),
                                 Some(Bson::Document(doc)) => doc,
                                 "`update` requires update document");

        let f = |x| Some(Bson::from_json(x));
        let upsert = var_match!(object.get("upsert").and_then(f),
                                Some(Bson::Boolean(b)) => b);

        Ok(Arguments::ReplaceOne {
            filter: filter,
            replacement: replacement,
            upsert: upsert,
        })
    }

    pub fn update_from_json(object: &Map<String, Value>, many: bool) -> Result<Arguments, String> {
        let f = |x| Some(Bson::from_json(x));
        let filter = val_or_err!(object.get("filter").and_then(f),
                                 Some(Bson::Document(doc)) => doc,
                                 "`update` requires filter document");

        let f = |x| Some(Bson::from_json(x));
        let update = val_or_err!(object.get("update").and_then(f),
                                 Some(Bson::Document(doc)) => doc,
                                 "`update` requires update document");

        let f = |x| Some(Bson::from_json(x));
        let upsert = var_match!(object.get("upsert").and_then(f),
                                Some(Bson::Boolean(b)) => b);

        Ok(Arguments::Update {
            filter: filter,
            update: update,
            upsert: upsert,
            many: many,
        })
    }
}
