//! Iterable network cursor for MongoDB queries.
//!
//! ```no_run
//! # #[macro_use] extern crate bson;
//! # extern crate mongodb;
//! #
//! # use mongodb::{Client, ThreadedClient};
//! # use mongodb::db::ThreadedDatabase;
//! # use bson::Bson;
//! #
//! # fn main() {
//! # let client = Client::connect("localhost", 27017).unwrap();
//! # let coll = client.db("test").collection("info");
//! #
//! coll.insert_one(doc!{ "spirit_animal": "ferret" }, None).unwrap();
//!
//! let mut cursor = coll.find(None, None).unwrap();
//! for result in cursor {
//!     let doc = result.expect("Received network error during cursor operations.");
//!     if let Some(&Bson::String(ref value)) = doc.get("spirit_animal") {
//!         println!("My spirit animal is {}", value);
//!     }
//! }
//! # }
//! ```
use {Client, CommandType, Error, ErrorCode, Result, ThreadedClient};
use apm::{CommandStarted, CommandResult, EventRunner};

use bson::{self, Bson};
use common::{merge_options, ReadMode, ReadPreference};
use coll::options::FindOptions;
use pool::PooledStream;
use time;
use wire_protocol::flags::OpQueryFlags;
use wire_protocol::operations::Message;

use std::{ i32, usize };
use std::mem::size_of;
use std::collections::vec_deque::VecDeque;

// Allows the server to decide the batch size.
pub const DEFAULT_BATCH_SIZE: i32 = 0;

/// Maintains a connection to the server and lazily returns documents from a
/// query.
#[derive(Debug)]
pub struct Cursor {
    // The client to read from.
    client: Client,
    // The namespace to read and write from.
    namespace: String,
    // How many documents to fetch at a given time from the server.
    batch_size: i32,
    // Uniquely identifies the cursor being returned by the reply.
    cursor_id: i64,
    // An upper bound on the total number of documents this cursor should return.
    limit: i32,
    // How many documents have been returned so far.
    count: i32,
    // A cache for documents received from the query that have not yet been returned.
    buffer: VecDeque<bson::Document>,
    read_preference: ReadPreference,
    cmd_type: CommandType,
}

macro_rules! try_or_emit {
    ($cmd_type:expr, $cmd_name:expr, $req_id:expr, $connstring:expr, $result:expr, $client:expr) =>
    {
        match $result {
            Ok(val) => val,
            Err(e) => {
                if $cmd_type != CommandType::Suppressed {
                    let hook_result = $client.run_completion_hooks(&CommandResult::Failure {
                        duration: 0,
                        command_name: String::from($cmd_name),
                        failure: &e,
                        request_id: $req_id as i64,
                        connection_string: $connstring,
                    });

                    if hook_result.is_err() {
                        return Err(Error::EventListenerError(Some(Box::new(e))));
                    }
                }

                return Err(e)
            }
        }
    };
}

impl Cursor {
    /// Construcs a new Cursor for a database command.
    ///
    /// # Arguments
    ///
    /// `client` - Client making the request.
    /// `db` - Which database the command is being sent to.
    /// `doc` - Specifies the command that is being run.
    /// `cmd_type` - The type of command, which will be used for monitoring events.
    /// `read_pref` - The read preference for the query.
    ///
    /// # Return value
    ///
    /// Returns the newly created Cursor on success, or an Error on failure.
    pub fn command_cursor(
        client: Client,
        db: &str,
        doc: bson::Document,
        cmd_type: CommandType,
        read_pref: ReadPreference,
    ) -> Result<Cursor> {
        let mut options = FindOptions::new();
        options.batch_size = Some(1);

        Cursor::query(
            client.clone(),
            format!("{}.$cmd", db),
            OpQueryFlags::empty(),
            doc,
            options,
            cmd_type,
            true,
            read_pref,
        )
    }

    fn get_bson_and_cid_from_message(
        message: Message,
    ) -> Result<(bson::Document, VecDeque<bson::Document>, i64)> {
        match message {
            Message::OpReply {
                cursor_id: cid,
                documents: docs,
                ..
            } => {
                let out_doc = if let Some(out_doc) = docs.get(0) {
                    if let Some(&Bson::I32(code)) = out_doc.get("code") {
                        // If command doesn't exist or namespace not found, return
                        // an empty array instead of throwing an error.
                        if code != ErrorCode::CommandNotFound as i32 &&
                            code != ErrorCode::NamespaceNotFound as i32
                        {
                            if let Some(&Bson::String(ref msg)) = out_doc.get("errmsg") {
                                return Err(Error::OperationError(msg.to_owned()));
                            }
                        }
                    }
                    out_doc.clone()
                } else {
                    bson::Document::new()
                };

                Ok((out_doc, docs.into_iter().collect(), cid))
            }
            _ => Err(Error::CursorNotFoundError),
        }
    }

    fn get_bson_and_cursor_info_from_command_message(
        message: Message,
    ) -> Result<(bson::Document, VecDeque<bson::Document>, i64, String)> {

        let (first, mut v, _) = Cursor::get_bson_and_cid_from_message(message)?;

        // Extract cursor information
        let mut cursor = match v.remove(0).and_then(|mut doc| doc.remove("cursor")) {
            Some(Bson::Document(cursor)) => cursor,
            _ => return Err(Error::CursorNotFoundError),
        };

        match (cursor.remove("id"), cursor.remove("ns"), cursor.remove("firstBatch")) {
            (Some(Bson::I64(id)),
             Some(Bson::String(ns)),
             Some(Bson::Array(batch))) => {
                // Extract first batch documents
                let map = batch
                    .into_iter()
                    .filter_map(|bdoc| if let Bson::Document(doc) = bdoc {
                        Some(doc)
                    } else {
                        None
                    })
                    .collect();

                Ok((first, map, id, ns))
            }
            _ => Err(Error::CursorNotFoundError)
        }
    }

    /// Executes a query where the batch size of the returned cursor is
    /// specified.
    ///
    /// # Arguments
    ///
    /// `client` - The client to read from.
    /// `namespace` - The namespace to read and write from.
    /// `flags` - Bit vector of query options.
    /// `query` - Document describing the query to make.
    /// `options` - Options for the query.
    /// `cmd_type` - The type of command, which will be used for monitoring events.
    /// `is_cmd_cursor` - Whether or not the Cursor is for a database command.
    /// `read_pref` - The read preference for the query.
    ///
    /// # Return value
    ///
    /// Returns the cursor for the query results on success, or an Error on
    /// failure.
    pub fn query(
        client: Client,
        namespace: String,
        flags: OpQueryFlags,
        query: bson::Document,
        options: FindOptions,
        cmd_type: CommandType,
        is_cmd_cursor: bool,
        read_pref: ReadPreference,
    ) -> Result<Cursor> {

        // Select a server stream from the topology.
        let (mut stream, slave_ok, send_read_pref) = if cmd_type.is_write_command() {
            (client.acquire_write_stream()?, false, false)
        } else {
            client.acquire_stream(read_pref.to_owned())?
        };

        // Set slave_ok flag based on the result from server selection.
        let new_flags = if slave_ok {
            flags | OpQueryFlags::SLAVE_OK
        } else {
            flags
        };

        // Send read_preference to the server based on the result from server selection.
        let new_query = if !send_read_pref {
            query
        } else if query.contains_key("$query") {
            // Query is already formatted as a $query document; add onto it.
            let mut query = query;
            query.insert("read_preference", read_pref.to_document());
            query
        } else {
            // Convert the query to a $query document.
            doc! {
                "$query": query,
                "read_preference": read_pref.to_document(),
            }
        };

        Cursor::query_with_stream(
            &mut stream,
            client,
            namespace,
            new_flags,
            new_query,
            options,
            cmd_type,
            is_cmd_cursor,
            Some(read_pref),
        )
    }

    pub fn query_with_stream(
        stream: &mut PooledStream,
        client: Client,
        namespace: String,
        flags: OpQueryFlags,
        query: bson::Document,
        options: FindOptions,
        cmd_type: CommandType,
        is_cmd_cursor: bool,
        read_pref: Option<ReadPreference>,
    ) -> Result<Cursor> {

        let socket = stream.get_socket();
        let req_id = client.get_req_id();

        let index = namespace.find('.').unwrap_or_else(|| namespace.len());
        let db_name = String::from(&namespace[..index]);
        let coll_name = String::from(&namespace[index + 1..]);
        let cmd_name = cmd_type.to_str();
        let connstring = socket.get_ref().peer_addr()?.to_string();

        let filter = match query.get("$query") {
            Some(&Bson::Document(ref doc)) => doc.clone(),
            _ => query.clone(),
        };

        let command = match cmd_type {
            CommandType::Find => {
                let document = doc! {
                    "find": coll_name,
                    "filter": filter
                };

                merge_options(document, options.clone())
            }
            _ => query.clone(),
        };

        let init_time = time::precise_time_ns();
        let message = Message::new_query(
            req_id,
            flags,
            namespace.clone(),
            options.skip.unwrap_or(0) as i32,
            options.batch_size.unwrap_or(DEFAULT_BATCH_SIZE),
            query,
            options.projection,
        )?;

        if cmd_type != CommandType::Suppressed {
            let hook_result = client.run_start_hooks(&CommandStarted {
                command: command,
                database_name: db_name,
                command_name: String::from(cmd_name),
                request_id: req_id as i64,
                connection_string: connstring.clone(),
            });

            if hook_result.is_err() {
                return Err(Error::EventListenerError(None));
            }
        }

        try_or_emit!(
            cmd_type,
            cmd_name,
            req_id,
            connstring,
            message.write(socket),
            client
        );
        let reply = try_or_emit!(
            cmd_type,
            cmd_name,
            req_id,
            connstring,
            Message::read(socket),
            client
        );

        let fin_time = time::precise_time_ns();

        let (doc, buf, cursor_id, namespace) = if is_cmd_cursor {
            try_or_emit!(
                cmd_type,
                cmd_name,
                req_id,
                connstring,
                Cursor::get_bson_and_cursor_info_from_command_message(reply),
                client
            )
        } else {
            let (doc, buf, id) = try_or_emit!(
                cmd_type,
                cmd_name,
                req_id,
                connstring,
                Cursor::get_bson_and_cid_from_message(reply),
                client
            );
            (doc, buf, id, namespace)
        };

        let reply = match cmd_type {
            CommandType::Find => doc! {
                "cursor": {
                    "id": cursor_id,
                    "ns": &namespace,
                    "firstBatch": buf.iter().cloned().map(Bson::from).collect::<Vec<_>>(),
                },
                "ok": 1
            },
            _ => doc,
        };

        if cmd_type != CommandType::Suppressed {
            let _hook_result = client.run_completion_hooks(&CommandResult::Success {
                duration: fin_time - init_time,
                reply: reply,
                command_name: String::from(cmd_name),
                request_id: req_id as i64,
                connection_string: connstring,
            });
        }

        let read_preference =
            read_pref.unwrap_or_else(|| ReadPreference::new(ReadMode::Primary, None));

        // Check if actual batch size fits into an `i32`.
        if size_of::<i32>() <= size_of::<usize>() && buf.len() > i32::MAX as usize {
            return Err(Error::DefaultError(
                format!("Batch buffer size {} overflows i32", buf.len())
            ));
        }

        Ok(Cursor {
            client: client,
            namespace: namespace,
            batch_size: buf.len() as i32,
            cursor_id: cursor_id,
            limit: options.limit.unwrap_or(0) as i32,
            count: 0,
            buffer: buf,
            read_preference: read_preference,
            cmd_type: cmd_type.clone(),
        })
    }

    fn get_from_stream(&mut self) -> Result<()> {
        let (mut stream, _, _) = self.client.acquire_stream(self.read_preference.to_owned())?;
        let socket = stream.get_socket();

        let req_id = self.client.get_req_id();
        let get_more = Message::new_get_more(
            req_id,
            self.namespace.to_owned(),
            self.batch_size,
            self.cursor_id,
        );

        let index = self.namespace.rfind('.').unwrap_or_else(
            || self.namespace.len(),
        );
        let db_name = String::from(&self.namespace[..index]);
        let cmd_name = String::from("get_more");
        let connstring = socket.get_ref().peer_addr()?.to_string();

        if self.cmd_type != CommandType::Suppressed {
            let hook_result = self.client.run_start_hooks(&CommandStarted {
                command: doc! { "cursor_id": self.cursor_id },
                database_name: db_name,
                command_name: cmd_name.clone(),
                request_id: req_id as i64,
                connection_string: connstring.clone(),
            });

            if hook_result.is_err() {
                return Err(Error::EventListenerError(None));
            }
        }

        try_or_emit!(
            self.cmd_type,
            cmd_name,
            req_id,
            connstring,
            get_more.write(socket.get_mut()),
            self.client
        );
        let reply = Message::read(socket.get_mut())?;

        let (_, v, _) = Cursor::get_bson_and_cid_from_message(reply)?;
        self.buffer.extend(v);
        Ok(())
    }

    /// Attempts to read a specified number of BSON documents from the cursor.
    ///
    /// # Arguments
    ///
    /// `n` - The number of documents to read.
    ///
    /// # Return value
    ///
    /// Returns a vector containing the BSON documents that were read.
    pub fn next_n(&mut self, n: usize) -> Result<Vec<bson::Document>> {
        self.take(n).collect()
    }

    /// # Return value
    ///
    /// Returns a vector containing the BSON documents that were read.
    #[deprecated(since = "0.2.8",
                 note = "this method uses 20 as the default batch size instead of letting the server decide; using `drain_current_batch` is recommended instead")]
    pub fn next_batch(&mut self) -> Result<Vec<bson::Document>> {
        let batch_size = if self.batch_size == 0 {
            20
        } else {
            self.batch_size
        };

        // If `usize` is at least as wide as `i32`, then all non-negative values
        // of `i32` are guaranteed to fit into a `usize`. Otherwise `usize::MAX`
        // fits into an `i32`.
        if
            batch_size < 0
            ||
            (
                size_of::<usize>() < size_of::<i32>()
                &&
                batch_size > usize::MAX as i32
            )
        {
            return Err(Error::DefaultError(
                format!("batch size {} is out of range", batch_size)
            ));
        }

        self.next_n(batch_size as usize)
    }

    /// Attempts to read a batch of BSON documents from the cursor.
    ///
    /// # Return value
    ///
    /// Returns a vector containing the BSON documents that were read.
    pub fn drain_current_batch(&mut self) -> Result<Vec<bson::Document>> {
        if self.buffer.is_empty() {
            self.get_from_stream()?;
        }

        Ok(self.buffer.drain(..).collect())
    }

    /// Checks whether there are any more documents for the cursor to return.
    ///
    /// # Return value
    ///
    /// Returns `true` if the cursor is not yet exhausted, or `false` if it is.
    pub fn has_next(&mut self) -> Result<bool> {
        if self.limit > 0 && self.count >= self.limit {
            Ok(false)
        } else {
            if self.buffer.is_empty() && self.limit != 1 && self.cursor_id != 0 {
                self.get_from_stream()?;
            }
            Ok(!self.buffer.is_empty())
        }
    }
}

impl Iterator for Cursor {
    type Item = Result<bson::Document>;

    /// Attempts to read a BSON document from the cursor.
    ///
    /// # Return value
    ///
    /// Returns a BSON document if there is another one to return; `None` if
    /// there are no more documents to return; or an Error if the request for
    /// another document fails.
    fn next(&mut self) -> Option<Result<bson::Document>> {
        match self.has_next() {
            Ok(true) => {
                self.count += 1;
                self.buffer.pop_front().map(Ok)
            }
            Ok(false) => None,
            Err(err) => Some(Err(err)),
        }
    }
}
