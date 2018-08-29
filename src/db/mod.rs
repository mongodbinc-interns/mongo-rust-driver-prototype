//! Interface for database-level operations.
//!
//! # Usage
//!
//! The database API provides methods for opening, creating, deleting, and listing collections.
//! It also handles user-level authentication over SCRAM-SHA-1.
//!
//! ## Collection Operations
//!
//! ```no_run
//! # use mongodb::{Client, ThreadedClient};
//! # use mongodb::db::ThreadedDatabase;
//! # let client = Client::connect("localhost", 27017).unwrap();
//! #
//! let db = client.db("movies");
//! db.create_collection("action", None).unwrap();
//! let collection_names = db.collection_names(None).unwrap();
//! assert!(!collection_names.is_empty());
//! ```
//!
//! ## Authentication
//!
//! ```no_run
//! # use mongodb::{Client, ThreadedClient};
//! # use mongodb::db::ThreadedDatabase;
//! # let client = Client::connect("localhost", 27017).unwrap();
//! #
//! let db = client.db("redacted");
//! db.create_user("saghm", "1234", None).unwrap();
//! db.auth("saghm", "1234").unwrap();
//!
//! let success = db.list_collections(None).unwrap();
//! ```
//!
//! ## Arbitrary Database Commands
//!
//! Any valid MongoDB database command can be sent to the server with the `command` and
//! `command_cursor` functions.
//!
//! ```no_run
//! # #[macro_use] extern crate bson;
//! # extern crate mongodb;
//! #
//! # use mongodb::{Client, CommandType, ThreadedClient};
//! # use mongodb::db::ThreadedDatabase;
//! # use bson::Bson;
//! # fn main() {
//! # let client = Client::connect("localhost", 27017).unwrap();
//! #
//! let db = client.db("movies");
//! let cmd = doc! { "connectionStatus": 1 };
//! let result = db.command(cmd, CommandType::Suppressed, None).unwrap();
//! if let Some(&Bson::Document(ref doc)) = result.get("authInfo") {
//!     // Read authentication info.
//! }
//! # }
//! ```
pub mod options;
pub mod roles;

use auth::Authenticator;
use bson;
use bson::Bson;
use {Client, CommandType, ThreadedClient, Result};
use Error::{CursorNotFoundError, OperationError, ResponseError};
use coll::Collection;
use coll::options::FindOptions;
use common::{ReadPreference, merge_options, WriteConcern};
use cursor::{Cursor, DEFAULT_BATCH_SIZE};
use self::options::{CreateCollectionOptions, CreateUserOptions, UserInfoOptions};
use semver::Version;
use std::error::Error;
use std::sync::Arc;

/// Interfaces with a MongoDB database.
#[derive(Debug)]
pub struct DatabaseInner {
    /// The database name.
    pub name: String,
    /// A reference to the client that spawned this database.
    pub client: Client,
    /// Indicates how a server should be selected for read operations.
    pub read_preference: ReadPreference,
    /// Describes the guarantees provided by MongoDB when reporting the success of a write
    /// operation.
    pub write_concern: WriteConcern,
}

pub type Database = Arc<DatabaseInner>;

pub trait ThreadedDatabase {
    /// Creates a database representation with optional read and write controls.
    fn open(
        client: Client,
        name: &str,
        read_preference: Option<ReadPreference>,
        write_concern: Option<WriteConcern>,
    ) -> Database;
    // Returns the version of the MongoDB instance.
    fn version(&self) -> Result<Version>;
    /// Logs in a user using the SCRAM-SHA-1 mechanism.
    fn auth(&self, user: &str, password: &str) -> Result<()>;
    /// Creates a collection representation with inherited read and write controls.
    fn collection(&self, coll_name: &str) -> Collection;
    /// Creates a collection representation with custom read and write controls.
    fn collection_with_prefs(
        &self,
        coll_name: &str,
        create: bool,
        read_preference: Option<ReadPreference>,
        write_concern: Option<WriteConcern>,
    ) -> Collection;
    /// Return a unique operational request id.
    fn get_req_id(&self) -> i32;
    /// Generates a cursor for a relevant operational command.
    fn command_cursor(
        &self,
        spec: bson::Document,
        cmd_type: CommandType,
        read_pref: ReadPreference,
    ) -> Result<Cursor>;
    /// Sends an administrative command over find_one.
    fn command(
        &self,
        spec: bson::Document,
        cmd_type: CommandType,
        read_preference: Option<ReadPreference>,
    ) -> Result<bson::Document>;
    /// Returns a list of collections within the database.
    fn list_collections(&self, filter: Option<bson::Document>) -> Result<Cursor>;
    /// Returns a list of collections within the database with a custom batch size.
    fn list_collections_with_batch_size(
        &self,
        filter: Option<bson::Document>,
        batch_size: i32,
    ) -> Result<Cursor>;
    /// Returns a list of collection names within the database.
    fn collection_names(&self, filter: Option<bson::Document>) -> Result<Vec<String>>;
    /// Creates a new collection.
    ///
    /// Note that due to the implicit creation of collections during insertion, this
    /// method should only be used to instantiate capped collections.
    fn create_collection(&self, name: &str, options: Option<CreateCollectionOptions>)
        -> Result<()>;
    /// Creates a new user.
    fn create_user(
        &self,
        name: &str,
        password: &str,
        options: Option<CreateUserOptions>,
    ) -> Result<()>;
    /// Permanently deletes all users from the database.
    fn drop_all_users(&self, write_concern: Option<WriteConcern>) -> Result<(i32)>;
    /// Permanently deletes the collection from the database.
    fn drop_collection(&self, name: &str) -> Result<()>;
    /// Permanently deletes the database from the server.
    fn drop_database(&self) -> Result<()>;
    /// Permanently deletes the user from the database.
    fn drop_user(&self, name: &str, Option<WriteConcern>) -> Result<()>;
    /// Retrieves information about all users in the database.
    fn get_all_users(&self, show_credentials: bool) -> Result<Vec<bson::Document>>;
    /// Retrieves information about a given user from the database.
    fn get_user(&self, user: &str, options: Option<UserInfoOptions>) -> Result<bson::Document>;
    /// Retrieves information about a given set of users from the database.
    fn get_users(
        &self,
        users: Vec<&str>,
        options: Option<UserInfoOptions>,
    ) -> Result<Vec<bson::Document>>;
}

impl ThreadedDatabase for Database {
    fn open(
        client: Client,
        name: &str,
        read_preference: Option<ReadPreference>,
        write_concern: Option<WriteConcern>,
    ) -> Database {
        let rp = read_preference.unwrap_or_else(|| client.read_preference.to_owned());
        let wc = write_concern.unwrap_or_else(|| client.write_concern.to_owned());

        Arc::new(DatabaseInner {
            name: String::from(name),
            client: client,
            read_preference: rp,
            write_concern: wc,
        })
    }

    fn auth(&self, user: &str, password: &str) -> Result<()> {
        let authenticator = Authenticator::new(self.clone());
        authenticator.auth(user, password)
    }

    fn collection(&self, coll_name: &str) -> Collection {
        Collection::new(
            self.clone(),
            coll_name,
            false,
            Some(self.read_preference.to_owned()),
            Some(self.write_concern.to_owned()),
        )
    }

    fn collection_with_prefs(
        &self,
        coll_name: &str,
        create: bool,
        read_preference: Option<ReadPreference>,
        write_concern: Option<WriteConcern>,
    ) -> Collection {
        Collection::new(
            self.clone(),
            coll_name,
            create,
            read_preference,
            write_concern,
        )
    }

    fn get_req_id(&self) -> i32 {
        self.client.get_req_id()
    }

    fn command_cursor(
        &self,
        spec: bson::Document,
        cmd_type: CommandType,
        read_pref: ReadPreference,
    ) -> Result<Cursor> {
        Cursor::command_cursor(
            self.client.clone(),
            &self.name[..],
            spec,
            cmd_type,
            read_pref,
        )
    }

    fn command(
        &self,
        spec: bson::Document,
        cmd_type: CommandType,
        read_preference: Option<ReadPreference>,
    ) -> Result<bson::Document> {

        let coll = self.collection("$cmd");
        let options = FindOptions {
            batch_size: Some(1),
            read_preference: read_preference,
            ..FindOptions::new()
        };
        let res = coll.find_one_with_command_type(
            Some(spec.clone()),
            Some(options),
            cmd_type,
        )?;
        res.ok_or_else(|| {
            OperationError(format!("Failed to execute command with spec {:?}.", spec))
        })
    }

    fn list_collections(&self, filter: Option<bson::Document>) -> Result<Cursor> {
        self.list_collections_with_batch_size(filter, DEFAULT_BATCH_SIZE)
    }

    fn list_collections_with_batch_size(
        &self,
        filter: Option<bson::Document>,
        batch_size: i32,
    ) -> Result<Cursor> {

        let mut spec = doc!{
            "listCollections": 1,
            "cursor": {
                "batchSize": batch_size,
            },
        };
        if let Some(f) = filter {
            spec.insert("filter", f);
        }

        self.command_cursor(
            spec,
            CommandType::ListCollections,
            self.read_preference.to_owned(),
        )
    }

    fn collection_names(&self, filter: Option<bson::Document>) -> Result<Vec<String>> {
        self.list_collections(filter)?
            .filter_map(|result| match result {
                Err(err) => Some(Err(err)),
                Ok(mut doc) => match doc.remove("name") {
                    Some(Bson::String(name)) => Some(Ok(name)),
                    _ => None,
                }
            })
            .collect()
    }

    fn version(&self) -> Result<Version> {
        let doc = doc! { "buildinfo": 1 };
        let out = self.command(doc, CommandType::BuildInfo, None)?;

        match out.get("version") {
            Some(&Bson::String(ref s)) => {
                match Version::parse(s) {
                    Ok(v) => Ok(v),
                    Err(e) => Err(ResponseError(String::from(e.description()))),
                }
            }
            _ => Err(ResponseError(
                String::from("No version received from server"),
            )),
        }
    }

    fn create_collection(
        &self,
        name: &str,
        options: Option<CreateCollectionOptions>,
    ) -> Result<()> {
        let mut doc = doc! { "create": name };

        if let Some(create_collection_options) = options {
            doc = merge_options(doc, create_collection_options);
        }

        self.command(doc, CommandType::CreateCollection, None)?;

        Ok(())
    }

    fn create_user(
        &self,
        name: &str,
        password: &str,
        options: Option<CreateUserOptions>,
    ) -> Result<()> {
        let mut doc = doc! {
            "createUser": name,
            "pwd": password
        };

        match options {
            Some(user_options) => {
                doc = merge_options(doc, user_options);
            }
            None => {
                doc.insert("roles", Vec::new());
            }
        };

        self.command(doc, CommandType::CreateUser, None).map(drop)
    }

    fn drop_all_users(&self, write_concern: Option<WriteConcern>) -> Result<(i32)> {
        let mut doc = doc! { "dropAllUsersFromDatabase": 1 };

        if let Some(concern) = write_concern {
            doc.insert("writeConcern", concern.to_bson());
        }

        let response = self.command(doc, CommandType::DropAllUsers, None)?;

        match response.get("n") {
            Some(&Bson::I32(i)) => Ok(i),
            Some(&Bson::I64(i)) => Ok(i as i32),
            _ => Err(CursorNotFoundError),
        }
    }

    fn drop_collection(&self, name: &str) -> Result<()> {
        let spec = doc!{ "drop": name };
        self.command(spec, CommandType::DropCollection, None).map(drop)
    }

    fn drop_database(&self) -> Result<()> {
        let spec = doc!{ "dropDatabase": 1 };
        self.command(spec, CommandType::DropDatabase, None).map(drop)
    }

    fn drop_user(&self, name: &str, write_concern: Option<WriteConcern>) -> Result<()> {
        let mut doc = doc! { "dropUser": name };

        if let Some(concern) = write_concern {
            doc.insert("writeConcern", concern.to_bson());
        }

        self.command(doc, CommandType::DropUser, None).map(drop)
    }

    fn get_all_users(&self, show_credentials: bool) -> Result<Vec<bson::Document>> {
        let doc = doc! {
            "usersInfo": 1,
            "showCredentials": show_credentials
        };

        let out = self.command(doc, CommandType::GetUsers, None)?;

        let vec = match out.get("users") {
            Some(&Bson::Array(ref vec)) => vec.clone(),
            _ => return Err(CursorNotFoundError),
        };

        vec.into_iter()
            .map(|bson| match bson {
                Bson::Document(doc) => Ok(doc),
                _ => Err(CursorNotFoundError),
            })
            .collect()
    }

    fn get_user(&self, user: &str, options: Option<UserInfoOptions>) -> Result<bson::Document> {
        let mut doc = doc! {
            "usersInfo": {
                "user": user,
                "db": &self.name,
            },
        };

        if let Some(user_info_options) = options {
            doc = merge_options(doc, user_info_options);
        }

        let out = self.command(doc, CommandType::GetUser, None)?;
        let users = match out.get("users") {
            Some(&Bson::Array(ref v)) => v.clone(),
            _ => return Err(CursorNotFoundError),
        };

        match users.first() {
            Some(&Bson::Document(ref doc)) => Ok(doc.clone()),
            _ => Err(CursorNotFoundError),
        }
    }

    fn get_users(
        &self,
        users: Vec<&str>,
        options: Option<UserInfoOptions>,
    ) -> Result<Vec<bson::Document>> {
        let vec: Vec<_> = users
            .into_iter()
            .map(|user| bson!({
                "user": user,
                "db": &self.name,
            }))
            .collect();

        let mut doc = doc! { "usersInfo": vec };

        if let Some(user_info_options) = options {
            doc = merge_options(doc, user_info_options);
        }

        let out = self.command(doc, CommandType::GetUsers, None)?;
        let vec = match out.get("users") {
            Some(&Bson::Array(ref vec)) => vec.clone(),
            _ => return Err(CursorNotFoundError),
        };

        vec.into_iter()
            .map(|bson| match bson {
                Bson::Document(doc) => Ok(doc),
                _ => Err(CursorNotFoundError),
            })
            .collect()
    }
}
