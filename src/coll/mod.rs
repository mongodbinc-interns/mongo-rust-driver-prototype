//! Interface for collection-level operations.
mod batch;
pub mod error;
pub mod options;
pub mod results;

use bson::{self, Bson, oid};
use command_type::CommandType;

use self::batch::{Batch, DeleteModel, UpdateModel};
use self::error::{BulkWriteException, WriteException};
use self::options::*;
use self::results::*;

use ThreadedClient;
use common::{merge_options, ReadPreference, WriteConcern};
use cursor::Cursor;
use db::{Database, ThreadedDatabase};

use Result;
use Error::{ArgumentError, ResponseError, OperationError, BulkWriteError};

use wire_protocol::flags::OpQueryFlags;
use std::collections::{BTreeMap, VecDeque};
use std::iter::FromIterator;

/// Interfaces with a MongoDB collection.
pub struct Collection {
    /// A reference to the database that spawned this collection.
    pub db: Database,
    /// The namespace of this collection, formatted as db_name.coll_name.
    pub namespace: String,
    read_preference: ReadPreference,
    write_concern: WriteConcern,
}

impl Collection {
    /// Creates a collection representation with optional read and write controls.
    ///
    /// If `create` is specified, the collection will be explicitly created in the database.
    pub fn new(db: Database,
               name: &str,
               create: bool,
               read_preference: Option<ReadPreference>,
               write_concern: Option<WriteConcern>)
               -> Collection {

        let rp = read_preference.unwrap_or_else(|| db.read_preference.to_owned());
        let wc = write_concern.unwrap_or_else(|| db.write_concern.to_owned());

        if create {
            // Attempt to create the collection explicitly, or fail silently.
            let _ = db.create_collection(name, None);
        }

        Collection {
            db: db.clone(),
            namespace: format!("{}.{}", db.name, name),
            read_preference: rp,
            write_concern: wc,
        }
    }

    /// Returns a unique operational request id.
    pub fn get_req_id(&self) -> i32 {
        self.db.client.get_req_id()
    }

    /// Extracts the collection name from the namespace.
    /// If the namespace is invalid, this method will panic.
    pub fn name(&self) -> String {
        match self.namespace.find('.') {
            Some(idx) => {
                let string = &self.namespace[self.namespace
                    .char_indices()
                    .nth(idx + 1)
                    .unwrap()
                    .0..];
                String::from(string)
            }
            None => {
                // '.' is inserted in Collection::new, so this should only panic due to user error.
                let msg = format!("Invalid namespace specified: '{}'.", self.namespace);
                panic!(msg);
            }
        }
    }

    /// Permanently deletes the collection from the database.
    pub fn drop(&self) -> Result<()> {
        self.db.drop_collection(&self.name()[..])
    }

    /// Runs an aggregation framework pipeline.
    pub fn aggregate(&self,
                     pipeline: Vec<bson::Document>,
                     options: Option<AggregateOptions>)
                     -> Result<Cursor> {
        let pipeline_map: Vec<_> = pipeline.into_iter()
            .map(Bson::Document)
            .collect();

        let mut spec = doc! {
            "aggregate" => (self.name()),
            "pipeline" => pipeline_map
        };

        let mut read_preference = self.read_preference.clone();

        match options {
            Some(aggregate_options) => {
                if let Some(ref read_preference_option) = aggregate_options.read_preference {
                    read_preference = read_preference_option.clone();
                }

                spec = merge_options(spec, aggregate_options);
            }
            None => {
                let cursor = doc! { "batchSize" => (::cursor::DEFAULT_BATCH_SIZE) };
                spec.insert("cursor", cursor);
            }
        };

        self.db.command_cursor(spec, CommandType::Aggregate, read_preference)
    }

    /// Gets the number of documents matching the filter.
    pub fn count(&self,
                 filter: Option<bson::Document>,
                 options: Option<CountOptions>)
                 -> Result<i64> {
        let mut spec = bson::Document::new();
        spec.insert("count", Bson::String(self.name()));

        if let Some(filter_doc) = filter {
            spec.insert("query", Bson::Document(filter_doc));
        }

        let mut read_preference = self.read_preference.clone();

        if let Some(count_options) = options {
            if let Some(ref read_preference_option) = count_options.read_preference {
                read_preference = read_preference_option.clone();
            }

            spec = merge_options(spec, count_options);
        }

        let result = try!(self.db.command(spec, CommandType::Count, Some(read_preference)));
        match result.get("n") {
            Some(&Bson::I32(n)) => Ok(n as i64),
            Some(&Bson::I64(n)) => Ok(n),
            _ => Err(ResponseError(String::from("No count received from server."))),
        }
    }

    /// Finds the distinct values for a specified field across a single collection.
    pub fn distinct(&self,
                    field_name: &str,
                    filter: Option<bson::Document>,
                    options: Option<DistinctOptions>)
                    -> Result<Vec<Bson>> {
        let mut spec = bson::Document::new();
        spec.insert("distinct", Bson::String(self.name()));
        spec.insert("key", Bson::String(String::from(field_name)));

        if let Some(filter_doc) = filter {
            spec.insert("query", Bson::Document(filter_doc));
        }

        let read_preference = options.and_then(|o| o.read_preference)
            .unwrap_or_else(|| self.read_preference.clone());

        let result = try!(self.db.command(spec, CommandType::Distinct, Some(read_preference)));
        match result.get("values") {
            Some(&Bson::Array(ref vals)) => Ok(vals.to_owned()),
            _ => Err(ResponseError(String::from("No values received from server."))),
        }
    }

    /// Returns a list of documents within the collection that match the filter.
    pub fn find(&self,
                filter: Option<bson::Document>,
                options: Option<FindOptions>)
                -> Result<Cursor> {
        self.find_with_command_type(filter, options, CommandType::Find)
    }

    fn find_with_command_type(&self,
                              filter: Option<bson::Document>,
                              options: Option<FindOptions>,
                              cmd_type: CommandType)
                              -> Result<Cursor> {
        let find_options = options.unwrap_or_else(FindOptions::new);
        let flags = OpQueryFlags::with_find_options(&find_options);

        let doc = match find_options.sort {
            Some(ref sort_opt) => {
                doc! {
                    "$query" => (filter.unwrap_or_else(bson::Document::new)),
                    "$orderby" => (Bson::Document(sort_opt.clone()))
                }
            }
            None => filter.unwrap_or_else(bson::Document::new),
        };

        let read_preference = match find_options.read_preference {
            Some(ref read_preference_option) => read_preference_option.clone(),
            None => self.read_preference.clone(), 
        };

        Cursor::query(self.db.client.clone(),
                      self.namespace.to_owned(),
                      flags,
                      doc,
                      find_options,
                      cmd_type,
                      false,
                      read_preference)
    }

    /// Returns the first document within the collection that matches the filter, or None.
    pub fn find_one(&self,
                    filter: Option<bson::Document>,
                    options: Option<FindOptions>)
                    -> Result<Option<bson::Document>> {
        self.find_one_with_command_type(filter, options, CommandType::Find)
    }

    pub fn find_one_with_command_type(&self,
                                      filter: Option<bson::Document>,
                                      options: Option<FindOptions>,
                                      cmd_type: CommandType)
                                      -> Result<Option<bson::Document>> {
        let mut find_one_options = options.unwrap_or_default();
        find_one_options.limit = Some(1);

        let mut cursor =
            try!(self.find_with_command_type(filter, Some(find_one_options), cmd_type));

        match cursor.next() {
            Some(Ok(bson)) => Ok(Some(bson)),
            Some(Err(err)) => Err(err),
            None => Ok(None),
        }
    }

    // Helper method for all findAndModify commands.
    fn find_and_modify(&self,
                       filter: bson::Document,
                       options: bson::Document,
                       _max_time_ms: Option<i64>,
                       write_concern: Option<WriteConcern>,
                       cmd_type: CommandType)
                       -> Result<Option<bson::Document>> {
        let mut cmd = doc! {
            "findAndModify" => (self.name()),
            "query" => filter
        };

        cmd = merge_options(cmd, options);

        let res = try!(self.db.command(cmd, cmd_type, None));
        let wc = write_concern.unwrap_or_else(|| self.write_concern.clone());
        try!(WriteException::validate_write_result(res.clone(), wc));

        let doc = match res.get("value") {
            Some(&Bson::Document(ref nested_doc)) => Some(nested_doc.to_owned()),
            _ => None,
        };

        Ok(doc)
    }

    /// Finds a single document and deletes it, returning the original.
    pub fn find_one_and_delete(&self,
                               filter: bson::Document,
                               options: Option<FindOneAndDeleteOptions>)
                               -> Result<Option<bson::Document>> {
        let (max_time_ms, write_concern) = match options {
            Some(ref opts) => (opts.max_time_ms, opts.write_concern.clone()),
            None => (None, None),
        };

        let mut options_doc = doc! { "remove" => true };

        if let Some(find_one_and_delete_options) = options {
            options_doc = merge_options(options_doc, find_one_and_delete_options);
        }

        self.find_and_modify(filter,
                             options_doc,
                             max_time_ms,
                             write_concern,
                             CommandType::FindOneAndDelete)
    }

    /// Finds a single document and replaces it, returning either the original
    /// or replaced document.
    pub fn find_one_and_replace(&self,
                                filter: bson::Document,
                                replacement: bson::Document,
                                options: Option<FindOneAndUpdateOptions>)
                                -> Result<Option<bson::Document>> {
        try!(Collection::validate_replace(&replacement));

        let (max_time_ms, write_concern) = match options {
            Some(ref opts) => (opts.max_time_ms, opts.write_concern.clone()),
            None => (None, None),
        };

        let mut options_doc = doc! { "update" => replacement };

        if let Some(find_one_and_replace_options) = options {
            options_doc = merge_options(options_doc, find_one_and_replace_options);
        }

        self.find_and_modify(filter,
                             options_doc,
                             max_time_ms,
                             write_concern,
                             CommandType::FindOneAndReplace)
    }

    /// Finds a single document and updates it, returning either the original
    /// or updated document.
    pub fn find_one_and_update(&self,
                               filter: bson::Document,
                               update: bson::Document,
                               options: Option<FindOneAndUpdateOptions>)
                               -> Result<Option<bson::Document>> {
        try!(Collection::validate_update(&update));

        let (max_time_ms, write_concern) = match options {
            Some(ref opts) => (opts.max_time_ms, opts.write_concern.clone()),
            None => (None, None),
        };

        let mut options_doc = doc! { "update" => update };

        if let Some(find_one_and_update_options) = options {
            options_doc = merge_options(options_doc, find_one_and_update_options);
        }

        self.find_and_modify(filter,
                             options_doc,
                             max_time_ms,
                             write_concern,
                             CommandType::FindOneAndUpdate)
    }

    fn get_unordered_batches(requests: Vec<WriteModel>) -> Vec<Batch> {
        let mut inserts = vec![];
        let mut deletes = vec![];
        let mut updates = vec![];

        for req in requests {
            match req {
                WriteModel::InsertOne { document } => inserts.push(document),
                WriteModel::DeleteOne { filter } => {
                    deletes.push(DeleteModel {
                        filter: filter,
                        multi: false,
                    })
                }
                WriteModel::DeleteMany { filter } => {
                    deletes.push(DeleteModel {
                        filter: filter,
                        multi: true,
                    })
                }
                WriteModel::ReplaceOne { filter, replacement, upsert } => {
                    updates.push(UpdateModel {
                        filter: filter,
                        update: replacement,
                        upsert: upsert,
                        multi: false,
                    })
                }
                WriteModel::UpdateOne { filter, update, upsert } => {
                    updates.push(UpdateModel {
                        filter: filter,
                        update: update,
                        upsert: upsert,
                        multi: false,
                    })
                }
                WriteModel::UpdateMany { filter, update, upsert } => {
                    updates.push(UpdateModel {
                        filter: filter,
                        update: update,
                        upsert: upsert,
                        multi: true,
                    })
                }
            }
        }

        vec![Batch::Insert(inserts), Batch::Delete(deletes), Batch::Update(updates)]
    }

    fn get_ordered_batches(mut requests: VecDeque<WriteModel>) -> Vec<Batch> {
        let first_model = match requests.pop_front() {
            Some(model) => model,
            None => return vec![],
        };

        let mut batches = vec![Batch::from(first_model)];

        for model in requests {
            let last_index = batches.len() - 1;

            if let Some(model) = batches[last_index].merge_model(model) {
                batches.push(Batch::from(model));
            }
        }

        batches
    }

    fn execute_insert_batch(&self,
                            documents: Vec<bson::Document>,
                            start_index: i64,
                            ordered: bool,
                            result: &mut BulkWriteResult,
                            exception: &mut BulkWriteException)
                            -> bool {
        let models = documents.iter()
            .map(|doc| WriteModel::InsertOne { document: doc.clone() })
            .collect();

        let options = Some(InsertManyOptions { ordered: Some(ordered), ..Default::default() });

        match self.insert_many(documents, options) {
            Ok(insert_result) => {
                result.process_insert_many_result(insert_result, models, start_index, exception)
            }
            Err(_) => {
                exception.add_unproccessed_models(models);
                false
            }
        }
    }

    fn execute_delete_batch(&self,
                            models: Vec<DeleteModel>,
                            ordered: bool,
                            result: &mut BulkWriteResult,
                            exception: &mut BulkWriteException)
                            -> bool {
        let original_models = models.iter()
            .map(|model| if model.multi {
                WriteModel::DeleteMany { filter: model.filter.clone() }
            } else {
                WriteModel::DeleteOne { filter: model.filter.clone() }
            })
            .collect();

        match self.bulk_delete(models, ordered, None, CommandType::DeleteMany) {
            Ok(bulk_delete_result) => {
                result.process_bulk_delete_result(bulk_delete_result, original_models, exception)
            }
            Err(_) => {
                exception.add_unproccessed_models(original_models);
                false
            }
        }
    }

    fn execute_update_batch(&self,
                            models: Vec<UpdateModel>,
                            start_index: i64,
                            ordered: bool,
                            result: &mut BulkWriteResult,
                            exception: &mut BulkWriteException)
                            -> bool {
        let original_models = models.iter()
            .map(|model| if model.multi {
                WriteModel::DeleteMany { filter: model.filter.clone() }
            } else {
                WriteModel::DeleteOne { filter: model.filter.clone() }
            })
            .collect();

        match self.bulk_update(models, ordered, None, CommandType::UpdateMany) {
            Ok(bulk_update_result) => {
                result.process_bulk_update_result(bulk_update_result,
                                                  original_models,
                                                  start_index,
                                                  exception)
            }
            Err(_) => {
                exception.add_unproccessed_models(original_models);
                false
            }
        }
    }

    fn execute_batch(&self,
                     batch: Batch,
                     start_index: i64,
                     ordered: bool,
                     result: &mut BulkWriteResult,
                     exception: &mut BulkWriteException)
                     -> bool {
        match batch {
            Batch::Insert(docs) => {
                self.execute_insert_batch(docs, start_index, ordered, result, exception)
            }
            Batch::Delete(models) => self.execute_delete_batch(models, ordered, result, exception),
            Batch::Update(models) => {
                self.execute_update_batch(models, start_index, ordered, result, exception)
            }
        }
    }

    /// Sends a batch of writes to the server at the same time.
    pub fn bulk_write(&self, requests: Vec<WriteModel>, ordered: bool) -> BulkWriteResult {
        let batches = if ordered {
            Collection::get_ordered_batches(VecDeque::from_iter(requests.into_iter()))
        } else {
            Collection::get_unordered_batches(requests)
        };

        let mut result = BulkWriteResult::new();
        let mut exception = BulkWriteException::new(vec![], vec![], vec![], None);

        let mut start_index = 0;

        for batch in batches {
            let length = batch.len();
            let success =
                self.execute_batch(batch, start_index, ordered, &mut result, &mut exception);

            if !success && ordered {
                break;
            }

            start_index += length;
        }

        if exception.unprocessed_requests.is_empty() {
            result.bulk_write_exception = Some(exception);
        }

        result
    }

    // Internal insertion helper function. Returns a vec of collected ids and a possible exception.
    fn insert(&self,
              docs: Vec<bson::Document>,
              options: Option<InsertManyOptions>,
              write_concern: Option<WriteConcern>,
              cmd_type: CommandType)
              -> Result<(Vec<Bson>, Option<BulkWriteException>)> {

        let wc = write_concern.unwrap_or_else(|| self.write_concern.clone());

        let mut converted_docs = Vec::new();
        let mut ids = Vec::new();

        for doc in &docs {
            let mut cdoc = doc.to_owned();
            match doc.get("_id") {
                Some(id) => ids.push(id.clone()),
                None => {
                    let id = Bson::ObjectId(try!(oid::ObjectId::new()));
                    cdoc.insert("_id", id.clone());
                    ids.push(id);
                }
            }
            converted_docs.push(Bson::Document(cdoc));
        }

        let mut cmd = doc! {
            "insert" => (self.name()),
            "documents" => converted_docs
        };

        if let Some(insert_options) = options {
            cmd = merge_options(cmd, insert_options);
        }

        let result = try!(self.db.command(cmd, cmd_type, None));

        // Intercept bulk write exceptions and insert into the result
        let exception_res = BulkWriteException::validate_bulk_write_result(result.clone(), wc);
        let exception = match exception_res {
            Ok(()) => None,
            Err(BulkWriteError(err)) => Some(err),
            Err(e) => return Err(e),
        };

        Ok((ids, exception))
    }

    /// Inserts the provided document. If the document is missing an identifier,
    /// the driver should generate one.
    pub fn insert_one(&self,
                      doc: bson::Document,
                      write_concern: Option<WriteConcern>)
                      -> Result<InsertOneResult> {
        let options =
            InsertManyOptions { write_concern: write_concern.clone(), ..Default::default() };

        let (ids, bulk_exception) = try!(self.insert(vec![doc],
                                                     Some(options),
                                                     write_concern,
                                                     CommandType::InsertOne));

        if ids.is_empty() {
            return Err(OperationError(String::from("No ids returned for insert_one.")));
        }

        // Downgrade bulk exception, if it exists.
        let exception = match bulk_exception {
            Some(e) => Some(WriteException::with_bulk_exception(e)),
            None => None,
        };

        let id = match exception {
            Some(ref exc) => {
                match exc.write_error {
                    Some(_) => None,
                    None => Some(ids[0].to_owned()),
                }
            }
            None => Some(ids[0].to_owned()),
        };

        Ok(InsertOneResult::new(id, exception))
    }

    /// Inserts the provided documents. If any documents are missing an identifier,
    /// the driver should generate them.
    pub fn insert_many(&self,
                       docs: Vec<bson::Document>,
                       options: Option<InsertManyOptions>)
                       -> Result<InsertManyResult> {
        let write_concern = options.as_ref().map_or(None, |opts| opts.write_concern.clone());

        let (ids, exception) =
            try!(self.insert(docs, options, write_concern, CommandType::InsertMany));

        let mut map = BTreeMap::new();
        for (i, item) in ids.iter().enumerate() {
            map.insert(i as i64, item.to_owned());
        }

        if let Some(ref exc) = exception {
            for error in &exc.write_errors {
                map.remove(&(error.index as i64));
            }
        }

        Ok(InsertManyResult::new(Some(map), exception))
    }

    // Sends a batch of delete ops to the server at once.
    fn bulk_delete(&self,
                   models: Vec<DeleteModel>,
                   ordered: bool,
                   write_concern: Option<WriteConcern>,
                   cmd_type: CommandType)
                   -> Result<BulkDeleteResult> {

        let wc = write_concern.unwrap_or_else(|| self.write_concern.clone());

        let mut deletes = Vec::new();
        for model in models {
            let mut delete = bson::Document::new();
            delete.insert("q", Bson::Document(model.filter));
            let limit = if model.multi { 0 } else { 1 };
            delete.insert("limit", Bson::I64(limit));
            deletes.push(Bson::Document(delete));
        }

        let mut cmd = bson::Document::new();
        cmd.insert("delete", Bson::String(self.name()));
        cmd.insert("deletes", Bson::Array(deletes));
        if !ordered {
            cmd.insert("ordered", Bson::Boolean(ordered));
        }
        cmd.insert("writeConcern", Bson::Document(wc.to_bson()));

        let result = try!(self.db.command(cmd, cmd_type, None));

        // Intercept write exceptions and insert into the result
        let exception_res = BulkWriteException::validate_bulk_write_result(result.clone(), wc);
        let exception = match exception_res {
            Ok(()) => None,
            Err(BulkWriteError(err)) => Some(err),
            Err(e) => return Err(e),
        };

        Ok(BulkDeleteResult::new(result, exception))
    }

    // Internal deletion helper function.
    fn delete(&self,
              filter: bson::Document,
              multi: bool,
              write_concern: Option<WriteConcern>)
              -> Result<DeleteResult> {
        let cmd_type = if multi {
            CommandType::DeleteMany
        } else {
            CommandType::DeleteOne
        };

        let result = try!(self.bulk_delete(vec![DeleteModel::new(filter, multi)],
                                           true,
                                           write_concern,
                                           cmd_type));

        Ok(DeleteResult::with_bulk_result(result))
    }

    /// Deletes a single document.
    pub fn delete_one(&self,
                      filter: bson::Document,
                      write_concern: Option<WriteConcern>)
                      -> Result<DeleteResult> {
        self.delete(filter, false, write_concern)
    }

    /// Deletes multiple documents.
    pub fn delete_many(&self,
                       filter: bson::Document,
                       write_concern: Option<WriteConcern>)
                       -> Result<DeleteResult> {
        self.delete(filter, true, write_concern)
    }

    // Sends a batch of replace and update ops to the server at once.
    fn bulk_update(&self,
                   models: Vec<UpdateModel>,
                   ordered: bool,
                   write_concern: Option<WriteConcern>,
                   cmd_type: CommandType)
                   -> Result<BulkUpdateResult> {
        let wc = write_concern.unwrap_or_else(|| self.write_concern.clone());

        let mut updates = Vec::new();
        for model in models {
            let mut update = bson::Document::from(model);

            if !ordered {
                update.insert("ordered", Bson::Boolean(ordered));
            }

            updates.push(Bson::Document(update));
        }

        let cmd = doc! {
            "update" => (self.name()),
            "updates" => updates,
            "writeConcern" => (wc.to_bson())
        };

        let result = try!(self.db.command(cmd, cmd_type, None));

        // Intercept write exceptions and insert into the result
        let exception_res = BulkWriteException::validate_bulk_write_result(result.clone(), wc);
        let exception = match exception_res {
            Ok(()) => None,
            Err(BulkWriteError(err)) => Some(err),
            Err(e) => return Err(e),
        };

        Ok(BulkUpdateResult::new(result, exception))
    }

    // Internal update helper function.
    fn update(&self,
              filter: bson::Document,
              update: bson::Document,
              upsert: Option<bool>,
              multi: bool,
              write_concern: Option<WriteConcern>)
              -> Result<UpdateResult> {

        let cmd_type = if multi {
            CommandType::UpdateMany
        } else {
            CommandType::UpdateOne
        };

        let result = try!(self.bulk_update(vec![UpdateModel::new(filter, update, upsert, multi)],
                                           true,
                                           write_concern,
                                           cmd_type));

        Ok(UpdateResult::with_bulk_result(result))
    }

    /// Replaces a single document.
    pub fn replace_one(&self,
                       filter: bson::Document,
                       replacement: bson::Document,
                       options: Option<ReplaceOptions>)
                       -> Result<UpdateResult> {
        let options = options.unwrap_or_else(ReplaceOptions::new);
        try!(Collection::validate_replace(&replacement));
        self.update(filter,
                    replacement,
                    options.upsert,
                    false,
                    options.write_concern)
    }

    /// Updates a single document.
    pub fn update_one(&self,
                      filter: bson::Document,
                      update: bson::Document,
                      options: Option<UpdateOptions>)
                      -> Result<UpdateResult> {
        let options = options.unwrap_or_else(UpdateOptions::new);
        try!(Collection::validate_update(&update));
        self.update(filter, update, options.upsert, false, options.write_concern)
    }

    /// Updates multiple documents.
    pub fn update_many(&self,
                       filter: bson::Document,
                       update: bson::Document,
                       options: Option<UpdateOptions>)
                       -> Result<UpdateResult> {
        let options = options.unwrap_or_else(UpdateOptions::new);
        try!(Collection::validate_update(&update));
        self.update(filter, update, options.upsert, true, options.write_concern)
    }

    fn validate_replace(replacement: &bson::Document) -> Result<()> {
        for key in replacement.keys() {
            if key.starts_with('$') {
                return Err(ArgumentError(String::from("Replacement cannot include $ operators.")));
            }
        }
        Ok(())
    }

    fn validate_update(update: &bson::Document) -> Result<()> {
        for key in update.keys() {
            if !key.starts_with('$') {
                return Err(ArgumentError(String::from("Update only works with $ operators.")));
            }
        }
        Ok(())
    }

    /// Create a single index.
    pub fn create_index(&self,
                        keys: bson::Document,
                        options: Option<IndexOptions>)
                        -> Result<String> {
        let model = IndexModel::new(keys, options);
        self.create_index_model(model)
    }

    /// Create a single index with an IndexModel.
    pub fn create_index_model(&self, model: IndexModel) -> Result<String> {
        let result = try!(self.create_indexes(vec![model]));
        Ok(result[0].to_owned())
    }

    /// Create multiple indexes.
    pub fn create_indexes(&self, models: Vec<IndexModel>) -> Result<Vec<String>> {
        let mut names = Vec::with_capacity(models.len());
        let mut indexes = Vec::with_capacity(models.len());

        for model in models {
            names.push(try!(model.name()));
            indexes.push(Bson::Document(try!(model.to_bson())));
        }

        let mut cmd = bson::Document::new();
        cmd.insert("createIndexes", Bson::String(self.name()));
        cmd.insert("indexes", Bson::Array(indexes));
        let result = try!(self.db.command(cmd, CommandType::CreateIndexes, None));

        match result.get("errmsg") {
            Some(&Bson::String(ref msg)) => Err(OperationError(msg.to_owned())),
            _ => Ok(names),
        }
    }

    /// Drop an index.
    pub fn drop_index(&self, keys: bson::Document, options: Option<IndexOptions>) -> Result<()> {
        let model = IndexModel::new(keys, options);
        self.drop_index_model(model)
    }

    /// Drop an index by name.
    pub fn drop_index_string(&self, name: String) -> Result<()> {
        let mut opts = IndexOptions::new();
        opts.name = Some(String::from(name));

        let model = IndexModel::new(bson::Document::new(), Some(opts));
        self.drop_index_model(model)
    }

    /// Drop an index by IndexModel.
    pub fn drop_index_model(&self, model: IndexModel) -> Result<()> {
        let mut cmd = bson::Document::new();
        cmd.insert("dropIndexes", Bson::String(self.name()));
        cmd.insert("index", Bson::String(try!(model.name())));

        let result = try!(self.db.command(cmd, CommandType::DropIndexes, None));
        match result.get("errmsg") {
            Some(&Bson::String(ref msg)) => Err(OperationError(msg.to_owned())),
            _ => Ok(()),
        }
    }

    /// Drop all indexes in the collection.
    pub fn drop_indexes(&self) -> Result<()> {
        let mut opts = IndexOptions::new();
        opts.name = Some(String::from("*"));

        let model = IndexModel::new(bson::Document::new(), Some(opts));
        self.drop_index_model(model)
    }

    /// List all indexes in the collection.
    pub fn list_indexes(&self) -> Result<Cursor> {
        let mut cmd = bson::Document::new();
        cmd.insert("listIndexes", Bson::String(self.name()));
        self.db.command_cursor(cmd,
                               CommandType::ListIndexes,
                               self.read_preference.to_owned())
    }
}
