use bson::{
    bson, doc, to_bson,
    Bson, Document, TimeStamp,
};

use ::{
    change_stream::ChangeStreamOptions,
    error::{Result},
};

pub struct PipelineBuilder<'a> {
    pipeline: &'a [Document],
    options: &'a ChangeStreamOptions,
    for_cluster: bool,
    document_resume_token: Option<&'a Document>,
    post_batch_resume_token: Option<&'a Document>,
    buffer_len: usize,
    last_optime: Option<&'a TimeStamp>,
    // max_wire_version: NotSure<()>, // TODO: need to figure out how to get this info.
}

impl<'a> PipelineBuilder<'a> {
    /// Create a new base pipeline builder.
    ///
    /// This constructor should be called to create the initial pipeline builder. This is all
    /// that is needed for creating an initial coll or db watcher. For a cluster watcher, you will
    /// need to call `for_cluster()` to ensure that cluster watching will be setup properly.
    ///
    /// The `pipeline` & `options` parameters should be the original un-modified values used
    /// when the change stream was first built. This is to guarantee algorithmic consistency.
    ///
    /// After all needed fields have been set, call `.build()` to get the pipeline.
    pub fn new(pipeline: &'a [Document], options: &'a ChangeStreamOptions, buffer_len: usize) -> Self {
        Self{
            pipeline, options, buffer_len, for_cluster: false, last_optime: None,
            document_resume_token: None, post_batch_resume_token: None,
        }
    }

    pub fn build(self) -> Result<Vec<Document>> {
        // Build options document.
        let mut opts = to_bson(self.options)?;
        { // NOTE: we can remove this scope once we update to edition 2018.
            let opts_doc = match opts {
                Bson::Document(ref mut doc) => doc,
                _ => unreachable!(),
            };
            if self.for_cluster {
                opts_doc.insert("allChangesForCluster", true);
            }

            // Handle existence of post batch resume token and empty buffer.
            if self.post_batch_resume_token.is_some() && self.buffer_len == 0 {
                let doc = self.post_batch_resume_token.unwrap().clone();
                opts_doc.insert("resumeAfter", Bson::Document(doc));
                opts_doc.remove("startAfter");
                opts_doc.remove("startAtOperationTime");
            }

            // Handle existence of document resume token.
            else if self.document_resume_token.is_some() {
                let doc = self.document_resume_token.unwrap().clone();
                opts_doc.insert("resumeAfter", Bson::Document(doc));
                opts_doc.remove("startAfter");
                opts_doc.remove("startAtOperationTime");
            }

            // Handle case where a startAfter was originally provided by user.
            else if self.options.start_after.is_some() {
                let doc = self.options.start_after.clone().unwrap();
                opts_doc.insert("resumeAfter", doc);
                opts_doc.remove("startAfter");
            }

            // Handle case where a resumeAfter was originally provided by user.
            else if self.options.resume_after.is_some() {
                let doc = self.options.resume_after.clone().unwrap();
                opts_doc.insert("resumeAfter", doc);
            }

            // TODO: this branch needs to ensure that the max wire version is >= 7. Need to figure out how to get this.
            else if self.last_optime.is_some() || self.options.start_at_operation_time.is_some() {
                let optime = match (self.last_optime, self.options.start_at_operation_time.as_ref()) {
                    (Some(optime), None) => optime.clone(),
                    (None, Some(optime)) => optime.clone(),
                    _ => unreachable!(),
                };
                opts_doc.insert("startAtOperationTime", to_bson(&optime)?);
            }

            // Note, the final `else` branch according to the spec is to just use the original change
            // stream options for the agg.
        };

        // Build full change stream pipeline.
        let mut pipeline = Vec::from(self.pipeline);
        let csdoc = doc!{"$changeStream": opts};
        pipeline.insert(0, csdoc);
        Ok(pipeline)
    }

    /// Configure the output pipeline to watch the entire cluster.
    pub fn for_cluster(mut self) -> Self {
        self.for_cluster = true;
        self
    }

    /// Update the builder with a potential value for the document resume token.
    ///
    /// The caller shouldn't be concerned about whether or not there is a value, this builder will
    /// handle the logic appropriately itself. The only time you shouldn't call this method is
    /// for the initial aggregation.
    pub fn document_resume_token(mut self, opt: Option<&'a Document>) -> Self {
        self.document_resume_token = opt;
        self
    }

    /// Update the builder with a potential value for the post batch resume token.
    ///
    /// The caller shouldn't be concerned about whether or not there is a value, this builder will
    /// handle the logic appropriately itself. The only time you shouldn't call this method is
    /// for the initial aggregation.
    pub fn post_batch_resume_token(mut self, opt: Option<&'a Document>) -> Self {
        self.post_batch_resume_token = opt;
        self
    }

    /// Update the builder with a potential value for the last optime.
    ///
    /// The caller shouldn't be concerned about whether or not there is a value, this builder will
    /// handle the logic appropriately itself. The only time you shouldn't call this method is
    /// for the initial aggregation.
    pub fn last_optime(mut self, opt: Option<&'a TimeStamp>) -> Self {
        self.last_optime = opt;
        self
    }
}
