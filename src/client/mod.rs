pub mod db;
pub mod coll;
pub mod command;
pub mod common;
pub mod connstring;
pub mod cursor;
pub mod wire_protocol;

use std::cell::RefCell;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicIsize, Ordering, ATOMIC_ISIZE_INIT};

use client::db::Database;
use client::common::{ReadPreference, WriteConcern};
use client::connstring::ConnectionString;

/// Interfaces with a MongoDB server or replica set.
pub struct MongoClient {
    req_id: Arc<AtomicIsize>,
    socket: Arc<Mutex<RefCell<TcpStream>>>,
    config: ConnectionString,
    pub read_preference: ReadPreference,
    pub write_concern: WriteConcern,
}

impl MongoClient {
    /// Creates a new MongoClient connected to a single MongoDB server.
    pub fn new(host: &str, port: u16) -> Result<MongoClient, String> {
        MongoClient::with_prefs(host, port, None, None)
    }

    /// `new` with custom read and write controls.
    pub fn with_prefs(host: &str, port: u16, read_pref: Option<ReadPreference>,
                      write_concern: Option<WriteConcern>) -> Result<MongoClient, String> {
        let config = ConnectionString::new(host, port);
        MongoClient::with_config(config, read_pref, write_concern)
    }

    /// Creates a new MongoClient connected to a server or replica set using
    /// a MongoDB connection string URI as defined by
    /// [the manual](http://docs.mongodb.org/manual/reference/connection-string/).
    pub fn with_uri(uri: &str) -> Result<MongoClient, String> {
        MongoClient::with_uri_and_prefs(uri, None, None)
    }

    /// `with_uri` with custom read and write controls.
    pub fn with_uri_and_prefs(uri: &str, read_pref: Option<ReadPreference>,
                              write_concern: Option<WriteConcern>) -> Result<MongoClient, String> {
        let config = try!(connstring::parse(uri));
        MongoClient::with_config(config, read_pref, write_concern)
    }

    fn with_config(config: ConnectionString, read_pref: Option<ReadPreference>,
                   write_concern: Option<WriteConcern>) -> Result<MongoClient, String> {

        let socket = try!(MongoClient::connect(&config));

        let rp = match read_pref {
            Some(rp) => rp,
            None => ReadPreference::Primary,
        };

        let wc = match write_concern {
            Some(wc) => wc,
            None => WriteConcern::new(),
        };

        Ok(MongoClient {
            req_id: Arc::new(ATOMIC_ISIZE_INIT),
            socket: Arc::new(Mutex::new(RefCell::new(socket))),
            config: config,
            read_preference: rp,
            write_concern: wc,
        })
    }

    /// Creates a database representation with default read and write controls.
    pub fn db<'a>(&'a self, db_name: &str) -> Database<'a> {
        Database::new(self, db_name, None, None)
    }

    /// Creates a database representation with custom read and write controls.
    pub fn db_with_prefs<'a>(&'a self, db_name: &str, read_preference: Option<ReadPreference>,
                             write_concern: Option<WriteConcern>) -> Database<'a> {
        Database::new(self, db_name, read_preference, write_concern)
    }

    /// Returns a unique operational request id.
    pub fn get_req_id(&self) -> i32 {
        self.req_id.fetch_add(1, Ordering::SeqCst) as i32
    }

    // Connects to a MongoDB server as defined by `config`.
    fn connect(config: &ConnectionString) -> Result<TcpStream, String> {
        let host_name = config.hosts[0].host_name.to_owned();
        let port = config.hosts[0].port;

        match TcpStream::connect((&host_name[..], port)) {
            Ok(sock) => Ok(sock),
            Err(_) => return Err(format!("Failed to connect to host '{}:{}'", host_name, port)),
        }
    }

    /// Drops the database defined by `db_name`.
    pub fn drop_database(&self, db_name: &str) -> Result<(), String> {
        let db = Database::new(self, db_name, None, None);
        try!(db.drop_database());
        Ok(())
    }
}
