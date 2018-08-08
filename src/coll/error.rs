//! Write errors for collection-level operations.
use bson::{self, Bson};
use super::options::WriteModel;
use common::WriteConcern;
use {Error, Result};
use std::{error, fmt};

/// The error type for Write-related MongoDB operations.
#[derive(Debug, Clone, PartialEq)]
pub struct WriteException {
    pub write_concern_error: Option<WriteConcernError>,
    pub write_error: Option<WriteError>,
    pub message: String,
}

/// The error struct for a write-concern related error.
#[derive(Debug, Clone, PartialEq)]
pub struct WriteConcernError {
    pub code: i32,
    pub details: WriteConcern,
    pub message: String,
}

/// The error struct for a write-related error.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WriteError {
    pub code: i32,
    pub message: String,
}

/// The error struct for Bulk-Write related MongoDB operations.
#[derive(Debug, Clone, PartialEq)]
pub struct BulkWriteException {
    pub processed_requests: Vec<WriteModel>,
    pub unprocessed_requests: Vec<WriteModel>,
    pub write_errors: Vec<BulkWriteError>,
    pub write_concern_error: Option<WriteConcernError>,
    pub message: String,
}

/// The error struct for a single bulk-write step, indicating the request
/// and its index in the original bulk-write request.
#[derive(Debug, Clone, PartialEq)]
pub struct BulkWriteError {
    pub index: i32,
    pub code: i32,
    pub message: String,
    pub request: Option<WriteModel>,
}

impl error::Error for WriteException {
    fn description(&self) -> &str {
        &self.message
    }
}

impl error::Error for BulkWriteException {
    fn description(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for WriteException {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str("WriteException:\n")?;

        if let Some(ref wc_err) = self.write_concern_error {
            write!(fmt, "{:?}", wc_err)?;
        }

        if let Some(ref w_err) = self.write_error {
            write!(fmt, "{:?}", w_err)?;
        }

        Ok(())
    }
}

impl fmt::Display for BulkWriteException {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str("BulkWriteException:\n")?;

        fmt.write_str("Processed Requests:\n")?;
        for v in &self.processed_requests {
            write!(fmt, "{:?}", v)?;
        }

        fmt.write_str("Unprocessed Requests:\n")?;
        for v in &self.unprocessed_requests {
            write!(fmt, "{:?}", v)?;
        }

        if let Some(ref error) = self.write_concern_error {
            write!(fmt, "{:?}", error)?;
        }

        for v in &self.write_errors {
            write!(fmt, "{:?}", v)?;
        }

        Ok(())
    }
}

impl fmt::Display for BulkWriteError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "BulkWriteError at index {} (code {}): {}",
            self.index,
            self.code,
            self.message
        )?;

        match self.request {
            Some(ref request) => write!(fmt, "Failed to execute request {:?}.", request),
            None => fmt.write_str("No additional error information was received.")
        }
    }
}

impl WriteException {
    /// Returns a new WriteException containing the given errors.
    pub fn new(wc_err: Option<WriteConcernError>, w_err: Option<WriteError>) -> WriteException {
        use std::fmt::Write;

        let mut s = wc_err.as_ref().map(|error| format!("{:?}", error)).unwrap_or_default();

        if let Some(ref error) = w_err {
            write!(s, "{:?}", error).expect("can't format error");
        }

        WriteException {
            write_concern_error: wc_err,
            write_error: w_err,
            message: s,
        }
    }

    /// Downgrades a BulkWriteException into a WriteException, retrieving the
    /// last write error to emulate the behavior of continue_on_error.
    pub fn with_bulk_exception(bulk_exception: BulkWriteException) -> WriteException {
        let mut write_errors = bulk_exception.write_errors;
        let write_error = write_errors.pop().map(|e| WriteError::new(e.code, e.message));

        WriteException::new(bulk_exception.write_concern_error, write_error)
    }

    /// Validates a single-write result.
    pub fn validate_write_result(
        result: bson::Document,
        write_concern: WriteConcern,
    ) -> Result<()> {
        let bulk_err_result = BulkWriteException::validate_bulk_write_result(result, write_concern);

        // Convert a bulk-write error into a write error, if it exists,
        // or propagate any other results.
        match bulk_err_result {
            Err(Error::BulkWriteError(bulk_exception)) => {
                Err(Error::WriteError(
                    WriteException::with_bulk_exception(bulk_exception),
                ))
            }
            Err(err) => Err(err),
            Ok(()) => Ok(()),
        }
    }
}

impl WriteConcernError {
    /// Returns a new WriteConcernError containing the provided error information.
    pub fn new<T: ToString>(code: i32, details: WriteConcern, message: T) -> WriteConcernError {
        WriteConcernError {
            code: code,
            details: details,
            message: message.to_string(),
        }
    }

    /// Parses a Bson document into a WriteConcernError with the provided write concern.
    pub fn parse(error: bson::Document, write_concern: WriteConcern) -> Result<WriteConcernError> {
        match (error.get("code"), error.get("errmsg")) {
            (Some(&Bson::I32(code)), Some(&Bson::String(ref message))) => {
                Ok(WriteConcernError::new(code, write_concern, message))
            }
            _ => Err(Error::ResponseError(format!(
                "WriteConcernError document is invalid: {:?}",
                error
            )))
        }
    }
}

impl WriteError {
    /// Returns a new WriteError containing the provided error information.
    pub fn new<T: ToString>(code: i32, message: T) -> WriteError {
        WriteError {
            code: code,
            message: message.to_string(),
        }
    }

    /// Parses a Bson document into a WriteError.
    pub fn parse(error: bson::Document) -> Result<WriteError> {
        if let Some(&Bson::I32(code)) = error.get("code") {
            if let Some(&Bson::String(ref message)) = error.get("errmsg") {
                return Ok(WriteError::new(code, message));
            }
        }
        Err(Error::ResponseError(
            format!("WriteError document is invalid: {:?}", error),
        ))
    }
}

impl BulkWriteError {
    /// Returns a new BulkWriteError containing the provided error information.
    pub fn new<T: ToString>(
        index: i32,
        code: i32,
        message: T,
        request: Option<WriteModel>,
    ) -> BulkWriteError {
        BulkWriteError {
            index: index,
            code: code,
            message: message.to_string(),
            request: request,
        }
    }

    /// Parses a Bson document into a BulkWriteError.
    pub fn parse(error: bson::Document) -> Result<BulkWriteError> {
        match (error.get("index"), error.get("code"), error.get("errmsg")) {
            (Some(&Bson::I32(index)),
             Some(&Bson::I32(code)),
             Some(&Bson::String(ref message))) => {
                Ok(BulkWriteError::new(index, code, message, None))
            }
            _ => Err(Error::ResponseError(
                format!("WriteError document is invalid: {:?}", error),
            ))
        }
    }
}

impl BulkWriteException {
    /// Returns a new BulkWriteException containing the provided error information.
    pub fn new(
        processed: Vec<WriteModel>,
        unprocessed: Vec<WriteModel>,
        write_errors: Vec<BulkWriteError>,
        write_concern_error: Option<WriteConcernError>,
    ) -> BulkWriteException {
        use std::fmt::Write;

        let mut s = write_concern_error.as_ref()
            .map(|e| format!("{:?}", e))
            .unwrap_or_default();

        for v in &write_errors {
            write!(s, "{:?}", v).expect("can't format error");
        }

        BulkWriteException {
            processed_requests: processed,
            unprocessed_requests: unprocessed,
            write_concern_error: write_concern_error,
            write_errors: write_errors,
            message: s,
        }
    }

    /// Adds a model to the vector of unprocessed models
    pub fn add_unproccessed_model(&mut self, model: WriteModel) {
        self.unprocessed_requests.push(model);
    }

    /// Adds a vector of models to the vector of unprocessed models.
    pub fn add_unproccessed_models(&mut self, models: Vec<WriteModel>) {
        self.unprocessed_requests.extend(models.into_iter());
    }

    /// Adds the data contined by another BulkWriteException to this one.
    pub fn add_bulk_write_exception(
        &mut self,
        exception_opt: Option<BulkWriteException>,
        models: Vec<WriteModel>,
    ) -> bool {
        let exception = match exception_opt {
            Some(exception) => exception,
            None => {
                self.processed_requests.extend(models.into_iter());
                return true;
            }
        };

        for req in &exception.processed_requests {
            self.processed_requests.push(req.clone());
        }

        for req in &exception.unprocessed_requests {
            self.unprocessed_requests.push(req.clone());
        }

        for err in &exception.write_errors {
            self.write_errors.push(err.clone());
        }

        if exception.write_concern_error.is_some() {
            self.write_concern_error = exception.write_concern_error;
        };

        self.message.push_str(&exception.message);

        false
    }

    /// Validates a bulk write result.
    pub fn validate_bulk_write_result(
        result: bson::Document,
        write_concern: WriteConcern,
    ) -> Result<()> {

        // Parse out any write concern errors.
        let wc_err = if let Some(&Bson::Document(ref error)) = result.get("writeConcernError") {
            Some(WriteConcernError::parse(error.clone(), write_concern)?)
        } else {
            None
        };

        // Parse out any write errors.
        let w_errs = if let Some(&Bson::Array(ref errors)) = result.get("writeErrors") {
            if errors.is_empty() {
                return Err(Error::ResponseError(String::from(
                    "Server indicates a write error, but none were found.",
                )));
            }

            let mut vec = Vec::new();
            for err in errors {
                if let Bson::Document(ref doc) = *err {
                    vec.push(BulkWriteError::parse(doc.clone())?);
                } else {
                    return Err(Error::ResponseError(
                        String::from("WriteError provided was not a bson document."),
                    ));
                }
            }
            vec
        } else {
            Vec::new()
        };

        // Return a bulk-write error if any errors were found.
        if wc_err.is_none() && w_errs.is_empty() {
            Ok(())
        } else {
            Err(Error::BulkWriteError(BulkWriteException::new(
                Vec::new(),
                Vec::new(),
                w_errs,
                wc_err,
            )))
        }
    }
}
