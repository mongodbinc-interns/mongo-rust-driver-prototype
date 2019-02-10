use bson::{
    Document,
    TimeStamp,
};

//////////////////////////////////////////////////////////////////////////////////////////////////
// ChangeStreamDocument //////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Deserialize)]
pub struct ChangeStreamDocument {
    /// The id functions as an opaque token for use when resuming an interrupted change stream.
    #[serde(rename="_id")]
    pub id: Document,

    /// Describes the type of operation represented in this change notification.
    #[serde(rename="operationType")]
    pub operation_type: ChangeStreamOperationType,

    /// The database and collection name in which the change happened.
    pub ns: ChangeStreamNSDocument,

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
    ///
    /// @note this is an option of the `$changeStream` pipeline stage.
    #[serde(rename="resumeAfter")]
    #[default]
    pub resume_after: Option<Document>,

    /// The maximum amount of time for the server to wait on new documents to satisfy
    /// a change stream query.
    ///
    /// This is the same field described in FindOptions in the CRUD spec.
    /// @see https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst#read
    /// @note this option is an alias for `maxTimeMS`, used on `getMore` commands
    /// @note this option is not set on the `aggregate` command nor `$changeStream` pipeline stage
    #[serde(rename="maxAwaitTimeMS")]
    #[default]
    pub max_await: Option<i64>,

    /// The number of documents to return per batch.
    ///
    /// This option is sent only if the caller explicitly provides a value. The
    /// default is to not send a value.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate
    /// @note this is an aggregation command option
    #[serde(rename="batchSize")]
    #[default]
    pub batch_size: Option<i32>,

    /// Specifies a collation.
    ///
    /// This option is sent only if the caller explicitly provides a value. The
    /// default is to not send a value.
    ///
    /// @see https://docs.mongodb.com/manual/reference/command/aggregate
    /// @note this is an aggregation command option
    #[default]
    pub collation: Option<Document>,

    /// The change stream will only provide changes that occurred at or after the
    /// specified timestamp. Any command run against the server will return
    /// an operation time that can be used here.
    /// @since 4.0
    /// @see https://docs.mongodb.com/manual/reference/method/db.runCommand/
    /// @note this is an option of the `$changeStream` pipeline stage.
    #[serde(rename="startAtOperationTime")]
    #[default]
    pub start_at_operation_time: Option<TimeStamp>,

    /// Similar to `resumeAfter`, this option takes a resume token and starts a
    /// new change stream returning the first notification after the token.
    /// This will allow users to watch collections that have been dropped and recreated
    /// or newly renamed collections without missing any notifications.
    ///
    /// The server will report an error if `startAfter` and `resumeAfter` are both specified.
    ///
    /// @since 4.2
    /// @see https://docs.mongodb.com/master/changeStreams/#change-stream-start-after
    #[serde(rename="startAfter")]
    #[default]
    pub start_after: Option<Document>,
}

/// The allowed variants for how to handle partial updates.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all="camelCase")]
pub enum ChangeStreamOptionsFullDocument {
    Default,
    UpdateLookup,

    /// Used to make this enum future-proof. All variants are accounted for as of 4.0.
    Other(String),
}
