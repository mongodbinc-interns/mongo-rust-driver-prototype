//! Results for collection-level operations.
use bson;
use bson::Bson;
use std::collections::BTreeMap;
use super::error::{BulkWriteException, WriteException};
use super::options::WriteModel;

/// Results for a bulk write operation.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BulkWriteResult {
    pub acknowledged: bool,
    pub inserted_count: i32,
    pub inserted_ids: BTreeMap<i64, Bson>,
    pub matched_count: i32,
    pub modified_count: i32,
    pub deleted_count: i32,
    pub upserted_count: i32,
    pub upserted_ids: BTreeMap<i64, Bson>,
    pub bulk_write_exception: Option<BulkWriteException>,
}

/// Results for a bulk delete operation.
#[derive(Debug, Clone, PartialEq)]
pub struct BulkDeleteResult {
    pub acknowledged: bool,
    pub deleted_count: i32,
    pub write_exception: Option<BulkWriteException>,
}

/// Results for a bulk update operation.
#[derive(Debug, Clone, PartialEq)]
pub struct BulkUpdateResult {
    pub acknowledged: bool,
    pub matched_count: i32,
    pub modified_count: i32,
    pub upserted_ids: Option<Bson>,
    pub write_exception: Option<BulkWriteException>,
}

/// Results for an insertOne operation.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertOneResult {
    pub acknowledged: bool,
    pub inserted_id: Option<Bson>,
    pub write_exception: Option<WriteException>,
}

/// Results for an insertMany operation.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertManyResult {
    pub acknowledged: bool,
    pub inserted_ids: Option<BTreeMap<i64, Bson>>,
    pub bulk_write_exception: Option<BulkWriteException>,
}

/// Results for a deletion operation.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteResult {
    pub acknowledged: bool,
    pub deleted_count: i32,
    pub write_exception: Option<WriteException>,
}

/// Results for an update operation.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateResult {
    pub acknowledged: bool,
    pub matched_count: i32,
    pub modified_count: i32,
    pub upserted_id: Option<Bson>,
    pub write_exception: Option<WriteException>,
}

impl BulkWriteResult {
    /// Extracts server reply information into a result.
    pub fn new() -> BulkWriteResult {
        BulkWriteResult {
            acknowledged: true,
            inserted_count: 0,
            inserted_ids: BTreeMap::new(),
            matched_count: 0,
            modified_count: 0,
            deleted_count: 0,
            upserted_count: 0,
            upserted_ids: BTreeMap::new(),
            bulk_write_exception: None,
        }
    }

    /// Adds the data in a BulkDeleteResult to this result.
    pub fn process_bulk_delete_result(
        &mut self,
        result: BulkDeleteResult,
        models: Vec<WriteModel>,
        exception: &mut BulkWriteException,
    ) -> bool {
        let ok = exception.add_bulk_write_exception(result.write_exception, models);
        self.deleted_count += result.deleted_count;

        ok
    }

    /// Adds the data in an InsertManyResult to this result.
    pub fn process_insert_many_result(
        &mut self,
        result: InsertManyResult,
        models: Vec<WriteModel>,
        start_index: i64,
        exception: &mut BulkWriteException,
    ) -> bool {
        let ok = exception.add_bulk_write_exception(result.bulk_write_exception, models);

        if let Some(ids) = result.inserted_ids {
            for (i, id) in ids {
                self.inserted_ids.insert(start_index + i, id);
                self.inserted_count += 1;
            }
        }

        ok
    }

    // Parses an index and id from a single BSON document and adds it to
    // the tree of upserted ids.
    fn parse_upserted_id(
        mut document: bson::Document,
        start_index: i64,
        upserted_ids: &mut BTreeMap<i64, Bson>,
    ) -> i32 {
        let (index, id) = (document.remove("index"), document.remove("_id"));

        match (index, id) {
            (Some(Bson::I32(i)), Some(bson_id)) => {
                let _ = upserted_ids.insert(start_index + i as i64, bson_id);
                1
            }
            (Some(Bson::I64(i)), Some(bson_id)) => {
                let _ = upserted_ids.insert(start_index + i, bson_id.clone());
                1
            }
            _ => 0,
        }
    }

    // Parses multiple indexes and ids from a single BSON document and adds
    // them to the tree of upserted ids.
    fn parse_upserted_ids(
        bson: Bson,
        start_index: i64,
        upserted_ids: &mut BTreeMap<i64, Bson>,
    ) -> i32 {
        match bson {
            Bson::Document(doc) => {
                BulkWriteResult::parse_upserted_id(doc, start_index, upserted_ids)
            }
            Bson::Array(vec) => {
                let mut count = 0;

                for bson in vec {
                    if let Bson::Document(doc) = bson {
                        count += BulkWriteResult::parse_upserted_id(doc, start_index, upserted_ids)
                    }
                }

                count
            }
            _ => 0,
        }
    }

    /// Adds the data in a BulkUpdateResult to this result.
    pub fn process_bulk_update_result(
        &mut self,
        result: BulkUpdateResult,
        models: Vec<WriteModel>,
        start_index: i64,
        exception: &mut BulkWriteException,
    ) -> bool {
        let ok = exception.add_bulk_write_exception(result.write_exception, models);

        self.matched_count += result.matched_count;
        self.modified_count += result.modified_count;

        if let Some(upserted_ids) = result.upserted_ids {
            self.upserted_count += BulkWriteResult::parse_upserted_ids(
                upserted_ids,
                start_index,
                &mut self.upserted_ids,
            );
        }

        ok
    }
}

impl BulkDeleteResult {
    /// Extracts server reply information into a result.
    pub fn new(doc: bson::Document, exception: Option<BulkWriteException>) -> BulkDeleteResult {
        let n = match doc.get("n") {
            Some(&Bson::I32(n)) => n,
            _ => 0,
        };

        BulkDeleteResult {
            acknowledged: true,
            deleted_count: n,
            write_exception: exception,
        }
    }
}

impl BulkUpdateResult {
    /// Extracts server reply information into a result.
    pub fn new(doc: bson::Document, exception: Option<BulkWriteException>) -> BulkUpdateResult {
        let n = match doc.get("n") {
            Some(&Bson::I32(n)) => n,
            _ => 0,
        };

        let (n_upserted, id) = match doc.get("upserted") {
            Some(&Bson::Array(ref arr)) => (arr.len() as i32, Some(arr[0].clone())),
            _ => (0, None),
        };

        let n_matched = n - n_upserted;

        let n_modified = match doc.get("nModified") {
            Some(&Bson::I32(n)) => n,
            _ => 0,
        };

        BulkUpdateResult {
            acknowledged: true,
            matched_count: n_matched,
            modified_count: n_modified,
            upserted_ids: id,
            write_exception: exception,
        }
    }
}

impl InsertOneResult {
    /// Extracts server reply information into a result.
    pub fn new(inserted_id: Option<Bson>, exception: Option<WriteException>) -> InsertOneResult {
        InsertOneResult {
            acknowledged: true,
            inserted_id: inserted_id,
            write_exception: exception,
        }
    }
}

impl InsertManyResult {
    /// Extracts server reply information into a result.
    pub fn new(
        inserted_ids: Option<BTreeMap<i64, Bson>>,
        exception: Option<BulkWriteException>,
    ) -> InsertManyResult {
        InsertManyResult {
            acknowledged: true,
            inserted_ids: inserted_ids,
            bulk_write_exception: exception,
        }
    }
}

impl DeleteResult {
    /// Extracts server reply information into a result.
    pub fn new(doc: bson::Document, exception: Option<WriteException>) -> DeleteResult {
        let n = match doc.get("n") {
            Some(&Bson::I32(n)) => n,
            _ => 0,
        };

        DeleteResult {
            acknowledged: true,
            deleted_count: n,
            write_exception: exception,
        }
    }

    pub fn with_bulk_result(result: BulkDeleteResult) -> DeleteResult {
        let exception = match result.write_exception {
            Some(bulk_exception) => Some(WriteException::with_bulk_exception(bulk_exception)),
            None => None,
        };

        DeleteResult {
            acknowledged: result.acknowledged,
            deleted_count: result.deleted_count,
            write_exception: exception,
        }
    }
}

impl UpdateResult {
    /// Extracts server reply information into a result.
    pub fn new(doc: bson::Document, exception: Option<WriteException>) -> UpdateResult {
        let n = match doc.get("n") {
            Some(&Bson::I32(n)) => n,
            _ => 0,
        };

        let (n_upserted, id) = match doc.get("upserted") {
            Some(&Bson::Array(ref arr)) => (arr.len() as i32, Some(arr[0].clone())),
            _ => (0, None),
        };

        let n_matched = n - n_upserted;

        let n_modified = match doc.get("nModified") {
            Some(&Bson::I32(n)) => n,
            _ => 0,
        };

        UpdateResult {
            acknowledged: true,
            matched_count: n_matched,
            modified_count: n_modified,
            upserted_id: id,
            write_exception: exception,
        }
    }

    pub fn with_bulk_result(result: BulkUpdateResult) -> UpdateResult {
        let exception = match result.write_exception {
            Some(bulk_exception) => Some(WriteException::with_bulk_exception(bulk_exception)),
            None => None,
        };

        UpdateResult {
            acknowledged: result.acknowledged,
            matched_count: result.matched_count,
            modified_count: result.modified_count,
            upserted_id: result.upserted_ids,
            write_exception: exception,
        }
    }
}
