mod pipelines;

use bson::{
    bson, doc,
    Bson, Document, TimeStamp,
};

use ::{
    Client, ThreadedClient,
    command_type::CommandType,
    common::ReadPreference,
    cursor::Cursor,
    db::{Database, ThreadedDatabase},
    error::{Error, ErrorCode, Result},
};
use self::pipelines::PipelineBuilder;

/// An error message for when a resume token has been filtered out of a change stream.
const MISSING_RESUME_TOKEN_ERR: &str = "Cannot provide resume functionality when the resume token is missing.";

//////////////////////////////////////////////////////////////////////////////////////////////////
// ChangeStream //////////////////////////////////////////////////////////////////////////////////

/// Observe real-time data changes in your MongoDB deployment without having to tail the oplog.
pub struct ChangeStream {
    /// A representation of how this change stream was built.
    cstype: CSType,

    /// The underlying cursor of this change stream.
    ///
    /// May be replaced or swapped out as needed to resume from a recoverable error.
    cursor: Cursor,

    /// A buffer of change stream docs from the most recent cursor batch.
    buffer: Vec<ChangeStreamDocument>,

    /// The resume token (_id) of the document the iterator last returned.
    document_resume_token: Option<Document>,

    /// The most recent postBatchResumeToken returned in an aggregate or
    /// getMore response. For pre-4.2 versions of MongoDB, this remains unset.
    post_batch_resume_token: Option<Document>,

    /// The pipeline of stages to append to an initial `$changeStream` stage.
    pipeline: Vec<Document>,

    /// The options provided to the initial `$changeStream` stage.
    options: ChangeStreamOptions,

    /// The read preference for the initial change stream aggregation, used
    /// for server selection during an automatic resume.
    read_preference: ReadPreference,

    /// The last operation time observed from the underlying cursor.
    last_optime: Option<TimeStamp>,

    /// A boolean indicating if the change stream has moved past the initial aggregation.
    ///
    /// This will be updated after the first successful call to `self.update_buffer`.
    is_initial_agg: bool,
}

/// A representation of the change stream itself. Used for rebuilding the cursor.
enum CSType {
    Coll(String, Database),
    Db(Database),
    Deployment(Client),
}

impl ChangeStream {
    /// Get a resume token that should be used to resume after the most recently returned change.
    pub fn get_resume_token(&self) -> Option<Document> {
        // NOTE TO CONTRIBUTORS: this must adhere to the specification outlined here:
        // https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst#option-1-changestreamgetresumetoken
        if self.post_batch_resume_token.is_some() && self.buffer.len() == 0 {
            self.post_batch_resume_token.clone()
        } else {
            self.document_resume_token.clone()
        }
    }

    //////////////////////////////////////////////////////////////////////////
    // Public to Crate ///////////////////////////////////////////////////////

    /// Create a change stream instance watching the target collection.
    pub(crate) fn watch_coll(
        coll: String,
        pipeline: Option<Vec<Document>>,
        options: Option<ChangeStreamOptions>,
        read_preference: ReadPreference,
        db: Database,
    ) -> Result<Self> {
        let options = options.unwrap_or_else(|| ChangeStreamOptions::builder().build());
        let pipeline = pipeline.unwrap_or_else(|| Vec::with_capacity(0)); // Will never be mutated, so avoid allocation.

        // Build a pipeline & cursor for watching a collection.
        let formatted_pipeline = PipelineBuilder::new(&pipeline, &options, 0).build()?.into_iter().map(Bson::from).collect();
        let cmd = doc!{"aggregate": coll.clone(), "pipeline": Bson::Array(formatted_pipeline), "cursor": doc!{}};
        let cursor = db.clone().command_cursor(cmd, CommandType::Aggregate, read_preference.clone())?;

        // Build and return the change stream instance.
        Ok(ChangeStream{
            cstype: CSType::Coll(coll, db),
            buffer: Vec::with_capacity(0),
            document_resume_token: None,
            post_batch_resume_token: None,
            last_optime: None,
            is_initial_agg: true,
            pipeline, cursor, options, read_preference,
        })
    }

    /// Create a change stream instance watching the target database.
    pub(crate) fn watch_db(
        pipeline: Option<Vec<Document>>,
        options: Option<ChangeStreamOptions>,
        read_preference: ReadPreference,
        db: Database,
    ) -> Result<Self> {
        let options = options.unwrap_or_else(|| ChangeStreamOptions::builder().build());
        let pipeline = pipeline.unwrap_or_else(|| Vec::with_capacity(0)); // Will never be mutated, so avoid allocation.

        // Build a pipeline & cursor for watching a collection.
        let formatted_pipeline = PipelineBuilder::new(&pipeline, &options, 0).build()?.into_iter().map(Bson::from).collect();
        let cmd = doc!{"aggregate": 1, "pipeline": Bson::Array(formatted_pipeline), "cursor": doc!{}};
        let cursor = db.clone().command_cursor(cmd, CommandType::Aggregate, read_preference.clone())?;

        // Build and return the change stream instance.
        Ok(ChangeStream{
            cstype: CSType::Db(db),
            buffer: Vec::with_capacity(0),
            document_resume_token: None,
            post_batch_resume_token: None,
            last_optime: None,
            is_initial_agg: true,
            pipeline, cursor, options, read_preference,
        })
    }

    /// Create a change stream instance watching the entire deployment of the given client.
    pub(crate) fn watch_deployment(
        pipeline: Option<Vec<Document>>,
        options: Option<ChangeStreamOptions>,
        read_preference: ReadPreference,
        client: Client,
    ) -> Result<Self> {
        let options = options.unwrap_or_else(|| ChangeStreamOptions::builder().build());
        let pipeline = pipeline.unwrap_or_else(|| Vec::with_capacity(0)); // Will never be mutated, so avoid allocation.

        // Build a pipeline & cursor for watching a collection.
        let formatted_pipeline = PipelineBuilder::new(&pipeline, &options, 0).for_cluster().build()?
            .into_iter().map(Bson::from).collect();
        let cmd = doc!{"aggregate": 1, "pipeline": Bson::Array(formatted_pipeline), "cursor": doc!{}};
        let cursor = client.clone().db("admin").command_cursor(cmd, CommandType::Aggregate, read_preference.clone())?;

        // Build and return the change stream instance.
        Ok(ChangeStream{
            cstype: CSType::Deployment(client),
            buffer: Vec::with_capacity(0),
            document_resume_token: None,
            post_batch_resume_token: None,
            last_optime: None,
            is_initial_agg: true,
            pipeline, cursor, options, read_preference,
        })
    }

    //////////////////////////////////////////////////////////////////////////
    // Private ///////////////////////////////////////////////////////////////

    /// Check the given error to see if recoverable in respect to change streams.
    fn is_error_recoverable(&self, err: &Error) -> bool {
        if self.is_initial_agg {
            return false;
        }
        match err {
            Error::CodedError(ecode) => {
                // We should retry network errors.
                if ecode.is_network_error() {
                    return true;
                }
                match ecode {
                    // The change stream spec blacklists these errors for retry.
                    ErrorCode::Interrupted | ErrorCode::CappedPositionLost | ErrorCode::CursorKilled => false,

                    // Any other coded server error can be retried.
                    _ => true,
                }
            }
            // For any other type of error, we can retry.
            _ => true,
        }
    }

    /// Build a new cursor based on the state of the current change stream.
    fn new_cursor(&self) -> Result<Cursor> {
        // Start building new pipeline. Depending on change stream type, modifications may be needed.
        let pipe = PipelineBuilder::new(&self.pipeline, &self.options, self.buffer.len())
            .post_batch_resume_token(self.post_batch_resume_token.as_ref())
            .document_resume_token(self.document_resume_token.as_ref())
            .last_optime(self.last_optime.as_ref());

        match &self.cstype {
            CSType::Coll(coll, db) => {
                let formatted_pipeline = pipe.build()?.into_iter().map(Bson::from).collect();
                let cmd = doc!{"aggregate": coll.clone(), "pipeline": Bson::Array(formatted_pipeline), "cursor": doc!{}};
                db.clone().command_cursor(cmd, CommandType::Aggregate, self.read_preference.clone())
            }
            CSType::Db(db) => {
                let formatted_pipeline = pipe.build()?.into_iter().map(Bson::from).collect();
                let cmd = doc!{"aggregate": 1, "pipeline": Bson::Array(formatted_pipeline), "cursor": doc!{}};
                db.clone().command_cursor(cmd, CommandType::Aggregate, self.read_preference.clone())
            }
            CSType::Deployment(client) => {
                let db = client.clone().db("admin");
                let formatted_pipeline = pipe.for_cluster().build()?.into_iter().map(Bson::from).collect();
                let cmd = doc!{"aggregate": 1, "pipeline": Bson::Array(formatted_pipeline), "cursor": doc!{}};
                db.command_cursor(cmd, CommandType::Aggregate, self.read_preference.clone())
            }
        }
    }

    /// Update the change stream's internal buffer.
    ///
    /// This is where we encapsulate the logic which may produce errors, namely dealing with the
    /// underlying cursor.
    fn update_buffer(&mut self) -> Result<()> {
        // We must not proceed with an update if buffer is populated, as that will throw off
        // the resume algorithm. Batches must be processed fully before fetching more from cursor.
        if self.buffer.len() > 0 {
            return Ok(());
        }

        // Buffer is empty, so we need to fetch a new payload and refresh the buffer.
        let doc = match self.cursor.next() {
            Some(Ok(d)) => d,
            Some(Err(err)) => {
                // If we can't recover from this error, propagate.
                if !self.is_error_recoverable(&err) {
                    return Err(err);
                }

                // Build a new cursor from our last logical resume point and continue.
                // NB: we only attempt to recover once.
                // let _ = self.cursor.kill(); // TODO: need to look into how to do this.
                self.cursor = self.new_cursor()?;
                match self.cursor.next() {
                    Some(Ok(d)) => d,
                    Some(Err(err)) => return Err(err),
                    None => return Ok(()),
                }
            }
            None => return Ok(()),
        };

        // We have a new cursor payload, so deserialize it.
        let change_doc: CSPayload = bson::from_bson(Bson::Document(doc)).map_err(|err| {
            Error::DefaultError(format!("{} If you need to change the shape of the change stream document, please use raw aggregations. {}", MISSING_RESUME_TOKEN_ERR, err))
        })?;

        // Update various internal state elements.
        self.post_batch_resume_token = change_doc.cursor.post_batch_resume_token;
        self.buffer = change_doc.cursor.batch;
        self.buffer.reverse(); // Reversed so we can use `buffer.pop()` in `.next()`.
        self.last_optime = Some(change_doc.operation_time);
        self.is_initial_agg = false;
        Ok(())
    }
}

impl Iterator for ChangeStream {
    type Item = Result<ChangeStreamDocument>;

    /// Attempt to get the next document of the change stream.
    ///
    /// As with all iterators, `None` will be returned when the iterator is empty. However, this
    /// simply indicates that the server has no more changes at this point in time. You can keep
    /// the change stream object around and attempt to iterate on it again. More elements may
    /// become available in the future.
    ///
    /// An error variant will be present only if a non-recoverable error was encountered. Any
    /// recoverable errors will not be visible to the consumer of the iterator.
    fn next(&mut self) -> Option<Self::Item> {
        // If our buffer is empty, update the buffer. Recoverable error handling logic is
        // encapsulated in `update_buffer`.
        if self.buffer.len() == 0 {
            match self.update_buffer() {
                Ok(_) => (),
                Err(err) => return Some(Err(err)), // Non-recoverable error was encountered.
            }
        }

        match self.buffer.pop() {
            Some(change_doc) => {
                self.document_resume_token = Some(change_doc.id.clone());
                Some(Ok(change_doc))
            },
            None => None,
        }
    }
}

/// Response to a successful change stream aggregate or getMore command.
#[derive(Clone, Debug, Deserialize)]
struct CSPayload {
    pub cursor: CSPayloadCursor,
    #[serde(rename="operationTime")]
    pub operation_time: TimeStamp,
    #[serde(rename="$clusterTime")]
    pub cluster_time: Document,
}

/// The cursor doc of a change stream aggregate or getMore payload.
#[derive(Clone, Debug, Deserialize)]
struct CSPayloadCursor {
    ns: String,
    id: i64,
    #[serde(alias="firstBatch", alias="nextBatch")]
    batch: Vec<ChangeStreamDocument>,
    #[serde(rename="postBatchResumeToken")]
    post_batch_resume_token: Option<Document>,
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// ChangeStreamDocument //////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Deserialize)]
pub struct ChangeStreamDocument {
    /// The id functions as an opaque token for use when resuming an interrupted change stream.
    ///
    /// NB: if this field is filtered out, the various `.watch()` convenience methods will err.
    #[serde(rename="_id")]
    pub id: Document,

    /// Describes the type of operation represented in this change notification.
    #[serde(rename="operationType")]
    pub operation_type: Option<ChangeStreamOperationType>,

    /// The database and collection name in which the change happened.
    pub ns: Option<ChangeStreamNSDocument>,

    /// Only present for ops of type `Insert`, `Update`, `Replace`, and `Delete`.
    ///
    /// For unsharded collections this contains a single field, _id, with the value of the _id of
    /// the document updated. For sharded collections, this will contain all the components of the
    /// shard key in order, followed by the _id if the _id isn’t part of the shard key.
    #[serde(rename="documentKey")]
    pub document_key: Option<Document>,

    /// Contains a description of updated and removed fields in this operation.
    ///
    /// Only present for ops of type `Update`.
    #[serde(rename="updateDescription")]
    pub update_description: Option<ChangeStreamUpdateDescription>,

    /// Always present for operations of type `Insert` and `Replace`. Also present for operations
    /// of type `Update` if the user has specified `updateLookup` in the `fullDocument` arguments
    /// to the `$changeStream` stage.
    ///
    /// For operations of type `Insert` and `Replace`, this key will contain the document being
    /// inserted, or the new version of the document that is replacing the existing document,
    /// respectively.
    ///
    /// For operations of type `Update`, this key will contain a copy of the full version of the
    /// document from some point after the update occurred. If the document was deleted since the
    /// updated happened, it will be null.
    #[serde(rename="fullDocument")]
    full_document: Option<Document>,
}

/// A document showing the database and collection name in which a change stream change happened.
#[derive(Clone, Debug, Deserialize)]
pub struct ChangeStreamNSDocument {
    pub db: String,
    pub coll: String,
}

/// Change stream operation types which can appear in a change stream document.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all="camelCase")]
pub enum ChangeStreamOperationType {
    Insert,
    Update,
    Replace,
    Delete,
    Invalidate,
    Drop,
    DropDatabase,
    Rename,

    /// Used to make this enum future-proof. All variants are accounted for as of 4.0.
    Other(String),
}

/// Contains a description of updated and removed fields for a change stream event.
///
/// Only present for ops of type `Update`.
#[derive(Clone, Debug, Deserialize)]
pub struct ChangeStreamUpdateDescription {
    /// A document containing key:value pairs of names of the fields that were
    /// changed, and the new value for those fields.
    #[serde(rename="updatedFields")]
    pub updated_fields: Document,

    /// An array of field names that were removed from the document.
    #[serde(rename="removedFields")]
    pub removed_fields: Vec<String>,
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// ChangeStreamOptions ///////////////////////////////////////////////////////////////////////////

/// The set of options available when creating a change stream.
#[derive(Clone, Debug, Serialize, TypedBuilder)]
pub struct ChangeStreamOptions {
    /// How this change stream should handle partial updates.
    ///
    /// Defaults to `Default`. When set to `UpdateLookup`, the change notification for partial
    /// updates will include both a delta describing the changes to the document, as well as a
    /// copy of the entire document that was changed from some time after the change occurred.
    #[serde(rename="fullDocument")]
    #[default="ChangeStreamOptionsFullDocument::Default"]
    pub full_document: ChangeStreamOptionsFullDocument,

    /// Specifies the logical starting point for the new change stream.
    #[serde(rename="resumeAfter")]
    #[default]
    pub resume_after: Option<Document>,

    /// The maximum amount of time for the server to wait on new documents to satisfy
    /// a change stream query.
    #[serde(rename="maxAwaitTimeMS")]
    #[default]
    pub max_await: Option<i64>,

    /// The number of documents to return per batch.
    #[serde(rename="batchSize")]
    #[default]
    pub batch_size: Option<i32>,

    /// Specifies a collation.
    #[default]
    pub collation: Option<Document>,

    /// The change stream will only provide changes that occurred at or after the specified
    /// timestamp. Any command run against the server will return an operation time that
    /// can be used here.
    #[serde(rename="startAtOperationTime")]
    #[default]
    pub start_at_operation_time: Option<TimeStamp>,

    /// Similar to `resume_after`, this option takes a resume token and starts a new change stream
    /// returning the first notification after the token. This will allow users to watch
    /// collections that have been dropped and recreated or newly renamed collections without
    /// missing any notifications.
    ///
    /// The server will report an error if `startAfter` and `resumeAfter` are both specified.
    #[serde(rename="startAfter")]
    #[default]
    pub start_after: Option<Document>,
}

/// The allowed variants for how to handle partial updates.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all="camelCase")]
pub enum ChangeStreamOptionsFullDocument {
    /// Include only the delta of changes to a document.
    Default,

    /// Include the delta of changes to a document as well as a full copy of the document.
    UpdateLookup,

    /// Used to make this enum future-proof. All variants are accounted for as of 4.0.
    Other(String),
}
