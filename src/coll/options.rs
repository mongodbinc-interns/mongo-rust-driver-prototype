//! Options for collection-level operations.
use bson::{self, Bson};
use common::{ReadPreference, WriteConcern};
use Error::ArgumentError;
use Result;

/// Describes the type of cursor to return on collection queries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CursorType {
    NonTailable,
    Tailable,
    TailableAwait,
}

impl Default for CursorType {
    fn default() -> Self {
        CursorType::NonTailable
    }
}

/// Describes the type of document to return on write operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReturnDocument {
    Before,
    After,
}

impl ReturnDocument {
    pub fn as_bool(&self) -> bool {
        match *self {
            ReturnDocument::Before => false,
            ReturnDocument::After => true,
        }
    }
}

/// Marker interface for writes that can be batched together.
#[derive(Debug, Clone)]
pub enum WriteModel {
    InsertOne { document: bson::Document },
    DeleteOne { filter: bson::Document },
    DeleteMany { filter: bson::Document },
    ReplaceOne {
        filter: bson::Document,
        replacement: bson::Document,
        upsert: Option<bool>,
    },
    UpdateOne {
        filter: bson::Document,
        update: bson::Document,
        upsert: Option<bool>,
    },
    UpdateMany {
        filter: bson::Document,
        update: bson::Document,
        upsert: Option<bool>,
    },
}

/// Options for aggregation queries.
#[derive(Clone, Debug, Default)]
pub struct AggregateOptions {
    pub allow_disk_use: Option<bool>,
    pub use_cursor: Option<bool>,
    pub batch_size: i32,
    pub max_time_ms: Option<i64>,
    pub read_preference: Option<ReadPreference>,
}

impl AggregateOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

impl From<AggregateOptions> for bson::Document {
    fn from(options: AggregateOptions) -> Self {
        let mut document = bson::Document::new();

        if let Some(allow_disk_use) = options.allow_disk_use {
            document.insert("allowDiskUse", Bson::Boolean(allow_disk_use));
        }

        // useCursor not currently used by the driver.


        let cursor = doc! { "batchSize" => (options.batch_size) };
        document.insert("cursor", Bson::Document(cursor));

        // maxTimeMS is not currently used by the driver.

        // read_preference is used directly by Collection::aggregate.

        document
    }
}

/// Options for count queries.
#[derive(Clone, Debug, Default)]
pub struct CountOptions {
    pub skip: Option<i64>,
    pub limit: Option<i64>,
    pub hint: Option<String>,
    pub hint_doc: Option<bson::Document>,
    pub max_time_ms: Option<i64>,
    pub read_preference: Option<ReadPreference>,
}

impl CountOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

impl From<CountOptions> for bson::Document {
    fn from(options: CountOptions) -> Self {
        let mut document = bson::Document::new();

        if let Some(skip) = options.skip {
            document.insert("skip", Bson::I64(skip));
        }

        if let Some(limit) = options.limit {
            document.insert("limit", Bson::I64(limit));
        }

        if let Some(hint) = options.hint {
            document.insert("hint", Bson::String(hint));
        }

        if let Some(hint_doc) = options.hint_doc {
            document.insert("hint_doc", Bson::Document(hint_doc));
        }

        // maxTimeMS is not currently used by the driver.

        // read_preference is used directly by Collection::count.

        document
    }
}

/// Options for distinct queries.
#[derive(Clone, Debug, Default)]
pub struct DistinctOptions {
    pub max_time_ms: Option<i64>,
    pub read_preference: Option<ReadPreference>,
}

impl DistinctOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

/// Options for collection queries.
#[derive(Clone, Debug, Default)]
pub struct FindOptions {
    pub allow_partial_results: bool,
    pub no_cursor_timeout: bool,
    pub oplog_replay: bool,
    pub skip: Option<i64>,
    pub limit: Option<i64>,
    pub cursor_type: CursorType,
    pub batch_size: Option<i32>,
    pub comment: Option<String>,
    pub max_time_ms: Option<i64>,
    pub modifiers: Option<bson::Document>,
    pub projection: Option<bson::Document>,
    pub sort: Option<bson::Document>,
    pub read_preference: Option<ReadPreference>,
}

impl FindOptions {
    /// Creates a new FindOptions struct with default parameters.
    pub fn new() -> Self {
        Default::default()
    }
}

impl From<FindOptions> for bson::Document {
    fn from(options: FindOptions) -> Self {
        let mut document = bson::Document::new();

        // `allow_partial_results`, `no_cursor_timeout`, `oplog_relay`, and `cursor_type` are used by
        // wire_protocol::OpQueryFlags.
        //
        // `max_time_ms` and `modifiers` are not currently used by the driver.
        //
        // read_preference is used directly by Collection::find_with_command_type.

        if let Some(projection) = options.projection {
            document.insert("projection", Bson::Document(projection));
        }

        if let Some(skip) = options.skip {
            document.insert("skip", Bson::I64(skip));
        }

        if let Some(limit) = options.limit {
            document.insert("limit", Bson::I64(limit));
        }

        if let Some(batch_size) = options.batch_size {
            document.insert("batchSize", Bson::I32(batch_size));
        }

        if let Some(sort) = options.sort {
            document.insert("sort", Bson::Document(sort));
        }

        document
    }
}

/// Options for `findOneAndDelete` operations.
#[derive(Clone, Debug, Default)]
pub struct FindOneAndDeleteOptions {
    pub max_time_ms: Option<i64>,
    pub projection: Option<bson::Document>,
    pub sort: Option<bson::Document>,
    pub write_concern: Option<WriteConcern>,
}

impl FindOneAndDeleteOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

impl From<FindOneAndDeleteOptions> for bson::Document {
    fn from(options: FindOneAndDeleteOptions) -> Self {
        let mut document = bson::Document::new();

        // max_time_ms is not currently used by the driver

        if let Some(projection) = options.projection {
            document.insert("fields", Bson::Document(projection));
        }

        if let Some(sort) = options.sort {
            document.insert("sort", Bson::Document(sort));
        }

        if let Some(write_concern) = options.write_concern {
            document.insert("writeConcern", Bson::Document(write_concern.to_bson()));
        }

        document
    }
}

/// Options for `findOneAndUpdate` operations.
#[derive(Clone, Debug, Default)]
pub struct FindOneAndUpdateOptions {
    pub return_document: Option<ReturnDocument>,
    pub max_time_ms: Option<i64>,
    pub projection: Option<bson::Document>,
    pub sort: Option<bson::Document>,
    pub upsert: Option<bool>,
    pub write_concern: Option<WriteConcern>,
}

impl FindOneAndUpdateOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

impl From<FindOneAndUpdateOptions> for bson::Document {
    fn from(options: FindOneAndUpdateOptions) -> Self {
        let mut document = bson::Document::new();

        if let Some(return_document) = options.return_document {
            document.insert("new", Bson::Boolean(return_document.as_bool()));
        }

        // max_time_ms is not currently used by the driver

        if let Some(projection) = options.projection {
            document.insert("fields", Bson::Document(projection));
        }

        if let Some(sort) = options.sort {
            document.insert("sort", Bson::Document(sort));
        }

        if let Some(upsert) = options.upsert {
            document.insert("upsert", upsert);
        }

        if let Some(write_concern) = options.write_concern {
            document.insert("writeConcern", Bson::Document(write_concern.to_bson()));
        }

        document
    }
}

/// Options for index operations.
#[derive(Clone, Debug, Default)]
pub struct IndexOptions {
    pub background: Option<bool>,
    pub expire_after_seconds: Option<i32>,
    pub name: Option<String>,
    pub sparse: Option<bool>,
    pub storage_engine: Option<String>,
    pub unique: Option<bool>,
    pub version: Option<i32>,
    // Options for text indexes
    pub default_language: Option<String>,
    pub language_override: Option<String>,
    pub text_version: Option<i32>,
    pub weights: Option<bson::Document>,
    // Options for 2dsphere indexes
    pub sphere_version: Option<i32>,
    // Options for 2d indexes
    pub bits: Option<i32>,
    pub max: Option<f64>,
    pub min: Option<f64>,
    // Options for geoHaystack indexes
    pub bucket_size: Option<i32>,
}

impl IndexOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

/// A single index model.
#[derive(Clone, Debug)]
pub struct IndexModel {
    pub keys: bson::Document,
    pub options: IndexOptions,
}

impl IndexModel {
    pub fn new(keys: bson::Document, options: Option<IndexOptions>) -> IndexModel {
        IndexModel {
            keys: keys,
            options: options.unwrap_or_else(IndexOptions::new),
        }
    }

    /// Returns the name of the index as specified by the options, or
    /// as automatically generated using the keys.
    pub fn name(&self) -> Result<String> {
        Ok(match self.options.name {
            Some(ref name) => name.to_owned(),
            None => try!(self.generate_index_name()),
        })
    }

    /// Generates the index name from keys.
    /// Auto-generated names have the form "key1_val1_key2_val2..."
    pub fn generate_index_name(&self) -> Result<String> {
        let mut name = String::new();
        for (key, bson) in self.keys.iter() {
            if !name.is_empty() {
                name.push_str("_");
            }

            name.push_str(key);
            name.push('_');
            match *bson {
                Bson::I32(ref i) => name.push_str(&format!("{}", i)),
                _ => return Err(ArgumentError(String::from("Index model keys must map to i32."))),
            }
        }
        Ok(name)
    }

    /// Converts the model to its BSON document representation.
    pub fn to_bson(&self) -> Result<bson::Document> {
        let mut doc = bson::Document::new();
        doc.insert("key", Bson::Document(self.keys.clone()));

        if let Some(ref val) = self.options.background {
            doc.insert("background", Bson::Boolean(*val));
        }
        if let Some(ref val) = self.options.expire_after_seconds {
            doc.insert("expireAfterSeconds", Bson::I32(*val));
        }
        if let Some(ref val) = self.options.name {
            doc.insert("name", Bson::String(val.to_owned()));
        } else {
            doc.insert("name", Bson::String(try!(self.generate_index_name())));
        }
        if let Some(ref val) = self.options.sparse {
            doc.insert("sparse", Bson::Boolean(*val));
        }
        if let Some(ref val) = self.options.storage_engine {
            doc.insert("storageEngine", Bson::String(val.to_owned()));
        }
        if let Some(ref val) = self.options.unique {
            doc.insert("unique", Bson::Boolean(*val));
        }
        if let Some(ref val) = self.options.version {
            doc.insert("v", Bson::I32(*val));
        }
        if let Some(ref val) = self.options.default_language {
            doc.insert("default_language", Bson::String(val.to_owned()));
        }
        if let Some(ref val) = self.options.language_override {
            doc.insert("language_override", Bson::String(val.to_owned()));
        }
        if let Some(ref val) = self.options.text_version {
            doc.insert("textIndexVersion", Bson::I32(*val));
        }
        if let Some(ref val) = self.options.weights {
            doc.insert("weights", Bson::Document(val.clone()));
        }
        if let Some(ref val) = self.options.sphere_version {
            doc.insert("2dsphereIndexVersion", Bson::I32(*val));
        }
        if let Some(ref val) = self.options.bits {
            doc.insert("bits", Bson::I32(*val));
        }
        if let Some(ref val) = self.options.max {
            doc.insert("max", Bson::FloatingPoint(*val));
        }
        if let Some(ref val) = self.options.min {
            doc.insert("min", Bson::FloatingPoint(*val));
        }
        if let Some(ref val) = self.options.bucket_size {
            doc.insert("bucketSize", Bson::I32(*val));
        }

        Ok(doc)
    }
}

/// Options for insertMany operations.
#[derive(Clone, Debug, Default)]
pub struct InsertManyOptions {
    pub ordered: Option<bool>,
    pub write_concern: Option<WriteConcern>,
}

impl InsertManyOptions {
    pub fn new() -> Self {
        Default::default()
    }
}

impl From<InsertManyOptions> for bson::Document {
    fn from(options: InsertManyOptions) -> Self {
        let mut document = bson::Document::new();

        if let Some(ordered) = options.ordered {
            document.insert("ordered", Bson::Boolean(ordered));
        }

        if let Some(write_concern) = options.write_concern {
            document.insert("writeConcern", Bson::Document(write_concern.to_bson()));
        }

        document
    }
}

/// Options for update operations.
#[derive(Clone, Debug, Default)]
pub struct UpdateOptions {
    pub upsert: Option<bool>,
    pub write_concern: Option<WriteConcern>,
}

impl UpdateOptions {
    pub fn new() -> UpdateOptions {
        Default::default()
    }
}

pub type ReplaceOptions = UpdateOptions;
