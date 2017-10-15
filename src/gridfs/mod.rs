//! Specification for storing and retrieving files that exceed 16MB within MongoDB.
//!
//! Instead of storing a file in a single document, GridFS divides a file into parts, or chunks,
//! and stores each of those chunks as a separate document. By default GridFS limits chunk size to
//! 255k. GridFS uses two collections to store files. One collection stores the file chunks, and
//! the other stores file metadata.

//! When you query a GridFS store for a file, the driver or client will reassemble the chunks as
//! needed. You can perform range queries on files stored through GridFS. You also can access
//! information from arbitrary sections of files, which allows you to “skip” into the middle of
//! a video or audio file.

//! GridFS is useful not only for storing files that exceed 16MB but also for storing any files for
//! which you want access without having to load the entire file into memory.
//!
//! ```no_run
//! # use mongodb::{Client, ThreadedClient};
//! # use mongodb::gridfs::{Store, ThreadedStore};
//! #
//! let client = Client::connect("localhost", 27017).unwrap();
//! let db = client.db("grid");
//! let fs = Store::with_db(db.clone());
//!
//! fs.put(String::from("/path/to/local_file.mp4")).unwrap();
//! let mut file = fs.open(String::from("/path/to/local_file.mp4")).unwrap();
//!
//! let id = file.doc.id.clone();
//! let chunk_bytes = file.find_chunk(id, 5).unwrap();
//! file.close().unwrap();
//! ```
pub mod file;

use bson::{self, oid};

use db::{Database, ThreadedDatabase};
use coll::Collection;
use coll::options::FindOptions;
use cursor::Cursor;
use Error::{self, ArgumentError};
use Result;

use self::file::{File, Mode};

use std::{io, fs};
use std::sync::Arc;

/// A default cursor wrapper that maps bson documents into GridFS file representations.
pub struct FileCursor {
    store: Store,
    cursor: Cursor,
    err: Option<Error>,
}

impl Iterator for FileCursor {
    type Item = File;

    fn next(&mut self) -> Option<File> {
        match self.cursor.next() {
            Some(Ok(bdoc)) => Some(File::with_doc(self.store.clone(), bdoc)),
            Some(Err(err)) => {
                self.err = Some(err);
                None
            }
            None => None,
        }
    }
}

impl FileCursor {
    /// Returns the next n files.
    pub fn next_n(&mut self, n: i32) -> Result<Vec<File>> {
        let docs = try!(self.cursor.next_n(n));
        Ok(
            docs.into_iter()
                .map(|doc| File::with_doc(self.store.clone(), doc.clone()))
                .collect(),
        )
    }

    /// Returns the next batch of files.
    pub fn drain_current_batch(&mut self) -> Result<Vec<File>> {
        let docs = try!(self.cursor.drain_current_batch());
        Ok(
            docs.into_iter()
                .map(|doc| File::with_doc(self.store.clone(), doc))
                .collect(),
        )
    }
}

/// Alias for a thread-safe GridFS instance.
pub type Store = Arc<StoreInner>;

/// Interfaces with a GridFS instance.
pub struct StoreInner {
    files: Collection,
    chunks: Collection,
}

pub trait ThreadedStore {
    /// A new GridFS store within the database with prefix 'fs'.
    fn with_db(db: Database) -> Store;
    /// A new GridFS store within the database with a specified prefix.
    fn with_prefix(db: Database, prefix: String) -> Store;
    /// Creates a new file.
    fn create(&self, name: String) -> Result<File>;
    /// Opens a file by filename.
    fn open(&self, name: String) -> Result<File>;
    /// Opens a file by object ID.
    fn open_id(&self, id: oid::ObjectId) -> Result<File>;
    /// Returns a cursor to all file documents matching the provided filter.
    fn find(
        &self,
        filter: Option<bson::Document>,
        options: Option<FindOptions>,
    ) -> Result<FileCursor>;
    /// Removes a file from GridFS by filename.
    fn remove(&self, name: String) -> Result<()>;
    /// Removes a file from GridFS by object ID.
    fn remove_id(&self, id: oid::ObjectId) -> Result<()>;
    /// Inserts a new file from local into GridFS.
    fn put(&self, name: String) -> Result<()>;
    /// Retrieves a file from GridFS into local storage.
    fn get(&self, name: String) -> Result<()>;
}

impl ThreadedStore for Store {
    fn with_db(db: Database) -> Store {
        Store::with_prefix(db, String::from("fs"))
    }

    fn with_prefix(db: Database, prefix: String) -> Store {
        Arc::new(StoreInner {
            files: db.collection(&format!("{}.files", prefix)[..]),
            chunks: db.collection(&format!("{}.chunks", prefix)[..]),
        })
    }

    fn create(&self, name: String) -> Result<File> {
        Ok(File::with_name(
            self.clone(),
            name,
            try!(oid::ObjectId::new()),
            Mode::Write,
        ))
    }

    fn open(&self, name: String) -> Result<File> {
        let mut options = FindOptions::new();
        options.sort = Some(doc!{ "uploadDate": 1 });

        match try!(self.files.find_one(
            Some(doc!{ "filename": name }),
            Some(options),
        )) {
            Some(bdoc) => Ok(File::with_doc(self.clone(), bdoc)),
            None => Err(ArgumentError(String::from("File does not exist."))),
        }
    }

    fn open_id(&self, id: oid::ObjectId) -> Result<File> {
        match try!(self.files.find_one(Some(doc!{ "_id": id }), None)) {
            Some(bdoc) => Ok(File::with_doc(self.clone(), bdoc)),
            None => Err(ArgumentError(String::from("File does not exist."))),
        }
    }

    fn find(
        &self,
        filter: Option<bson::Document>,
        options: Option<FindOptions>,
    ) -> Result<FileCursor> {
        Ok(FileCursor {
            store: self.clone(),
            cursor: try!(self.files.find(filter, options)),
            err: None,
        })
    }

    fn remove(&self, name: String) -> Result<()> {
        let mut options = FindOptions::new();
        options.projection = Some(doc!{ "_id": 1 });

        let cursor = try!(self.find(Some(doc!{ "filename": name }), Some(options)));
        for doc in cursor {
            try!(self.remove_id(doc.id.clone()));
        }

        Ok(())
    }

    fn remove_id(&self, id: oid::ObjectId) -> Result<()> {
        try!(self.files.delete_many(doc!{ "_id": id.clone() }, None));
        try!(self.chunks.delete_many(
            doc!{ "files_id": id.clone() },
            None,
        ));
        Ok(())
    }

    fn put(&self, name: String) -> Result<()> {
        let mut file = try!(self.create(name.clone()));
        let mut f = try!(fs::File::open(name));
        try!(io::copy(&mut f, &mut file));
        try!(file.close());
        Ok(())
    }

    fn get(&self, name: String) -> Result<()> {
        let mut f = try!(fs::File::create(name.clone()));
        let mut file = try!(self.open(name));
        try!(io::copy(&mut file, &mut f));
        try!(file.close());
        Ok(())
    }
}
