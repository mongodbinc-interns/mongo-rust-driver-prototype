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
//! coll.insert_one(doc!{ "spirit_animal" => "ferret" }, None).unwrap();
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
use common::{ReadMode, ReadPreference};
use pool::PooledStream;
use time;
use wire_protocol::flags::{self, OpQueryFlags};
use wire_protocol::operations::Message;

use std::collections::vec_deque::VecDeque;

pub const DEFAULT_BATCH_SIZE: i32 = 20;

/// Maintains a connection to the server and lazily returns documents from a
/// query.
///
/// # Fields
///
/// `client` - The client to read from.
/// `namespace` - The namespace to read and write from.
/// `batch_size` - How many documents to fetch at a given time from the server.
/// `cursor_id` - Uniquely identifies the cursor being returned by the reply.
/// `limit` - An upper bound on the total number of documents this cursor
///           should return.
/// `count` - How many documents have been returned so far.
/// `buffer` - A cache for documents received from the query that have not
///            yet been returned.
pub struct Cursor {
    client: Client,
    namespace: String,
    batch_size: i32,
    cursor_id: i64,
    limit: i32,
    count: i32,
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

                    if let Err(_) = hook_result {
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
    ///
    /// # Return value
    ///
    /// Returns the newly created Cursor on success, or an Error on failure.
    pub fn command_cursor(client: Client,
                          db: &str,
                          doc: bson::Document,
                          cmd_type: CommandType,
                          read_pref: ReadPreference)
                          -> Result<Cursor> {
        Cursor::query(client.clone(),
                      format!("{}.$cmd", db),
                      1,
                      OpQueryFlags::empty(),
                      0,
                      0,
                      doc,
                      None,
                      cmd_type,
                      true,
                      read_pref)
    }

    fn get_bson_and_cid_from_message(message: Message)
                                     -> Result<(bson::Document, VecDeque<bson::Document>, i64)> {
        match message {
            Message::OpReply { cursor_id: cid, documents: docs, .. } => {
                let mut v = VecDeque::new();
                let mut out_doc = doc!{};

                if !docs.is_empty() {
                    out_doc = docs[0].clone();
                    if let Some(&Bson::I32(ref code)) = docs[0].get("code") {
                        // If command doesn't exist or namespace not found, return
                        // an empty array instead of throwing an error.
                        if *code != ErrorCode::CommandNotFound as i32 &&
                           *code != ErrorCode::NamespaceNotFound as i32 {
                            if let Some(&Bson::String(ref msg)) = docs[0].get("errmsg") {
                                return Err(Error::OperationError(msg.to_owned()));
                            }
                        }
                    }
                }

                for doc in docs {
                    v.push_back(doc.clone());
                }

                Ok((out_doc, v, cid))
            }
            _ => Err(Error::CursorNotFoundError),
        }
    }

    fn get_bson_and_cursor_info_from_command_message
        (message: Message)
         -> Result<(bson::Document, VecDeque<bson::Document>, i64, String)> {

        let (first, v, _) = try!(Cursor::get_bson_and_cid_from_message(message));
        if v.len() != 1 {
            return Err(Error::CursorNotFoundError);
        }

        let doc = &v[0];

        // Extract cursor information
        if let Some(&Bson::Document(ref cursor)) = doc.get("cursor") {
            if let Some(&Bson::I64(ref id)) = cursor.get("id") {
                if let Some(&Bson::String(ref ns)) = cursor.get("ns") {
                    if let Some(&Bson::Array(ref batch)) = cursor.get("firstBatch") {

                        // Extract first batch documents
                        let map = batch.iter()
                            .filter_map(|bdoc| if let Bson::Document(ref doc) = *bdoc {
                                Some(doc.clone())
                            } else {
                                None
                            })
                            .collect();

                        return Ok((first, map, *id, ns.to_owned()));
                    }
                }
            }
        }

        Err(Error::CursorNotFoundError)
    }

    /// Executes a query where the batch size of the returned cursor is
    /// specified.
    ///
    /// # Arguments
    ///
    /// `client` - The client to read from.
    /// `namespace` - The namespace to read and write from.
    /// `batch_size` - How many documents the cursor should return at a time.
    /// `flags` - Bit vector of query options.
    /// `number_to_skip` - The number of initial documents to skip over in the
    ///                    query results.
    /// `number_to_return - The total number of documents that should be
    ///                     returned by the query.
    /// `return_field_selector - An optional projection of which fields should
    ///                          be present in the documents to be returned by
    ///                          the query.
    /// `is_cmd_cursor` - Whether or not the Cursor is for a database command.
    /// `monitor_host` - The host being monitored, if this is a query from a server monitor.
    /// # Return value
    ///
    /// Returns the cursor for the query results on success, or an Error on
    /// failure.
    pub fn query(client: Client,
                 namespace: String,
                 batch_size: i32,
                 flags: OpQueryFlags,
                 number_to_skip: i32,
                 number_to_return: i32,
                 query: bson::Document,
                 return_field_selector: Option<bson::Document>,
                 cmd_type: CommandType,
                 is_cmd_cursor: bool,
                 read_pref: ReadPreference)
                 -> Result<Cursor> {

        // Select a server stream from the topology.
        let (stream, slave_ok, send_read_pref) = if cmd_type.is_write_command() {
            (try!(client.acquire_write_stream()), false, false)
        } else {
            try!(client.acquire_stream(read_pref.to_owned()))
        };

        // Set slave_ok flag based on the result from server selection.
        let new_flags = if !slave_ok {
            flags
        } else {
            flags | flags::SLAVE_OK
        };

        // Send read_preference to the server based on the result from server selection.
        let new_query = if !send_read_pref {
            query
        } else {
            match query.get("$query") {
                Some(_) => {
                    // Query is already formatted as a $query document; add onto it.
                    let mut nq = query.clone();
                    nq.insert("read_preference", Bson::Document(read_pref.to_document()));
                    nq
                }
                None => {
                    // Convert the query to a $query document.
                    let mut nq = doc! { "$query" => query };
                    nq.insert("read_preference", Bson::Document(read_pref.to_document()));
                    nq
                }
            }
        };

        Cursor::query_with_stream(stream,
                                  client,
                                  namespace,
                                  batch_size,
                                  new_flags,
                                  number_to_skip,
                                  number_to_return,
                                  new_query,
                                  return_field_selector,
                                  cmd_type,
                                  is_cmd_cursor,
                                  Some(read_pref))
    }

    pub fn query_with_stream(stream: PooledStream,
                             client: Client,
                             namespace: String,
                             batch_size: i32,
                             flags: OpQueryFlags,
                             number_to_skip: i32,
                             number_to_return: i32,
                             query: bson::Document,
                             return_field_selector: Option<bson::Document>,
                             cmd_type: CommandType,
                             is_cmd_cursor: bool,
                             read_pref: Option<ReadPreference>)
                             -> Result<Cursor> {

        let mut stream = stream;
        let mut socket = stream.get_socket();
        let req_id = client.get_req_id();

        let index = namespace.find('.').unwrap_or(namespace.len());
        let db_name = String::from(&namespace[..index]);
        let coll_name = String::from(&namespace[index + 1..]);
        let cmd_name = cmd_type.to_str();
        let connstring = format!("{}", try!(socket.get_ref().peer_addr()));

        let filter: bson::Document = match query.get("$query") {
            Some(&Bson::Document(ref doc)) => doc.clone(),
            _ => doc!{},
        };

        let projection = match return_field_selector {
            Some(ref doc) => doc.clone(),
            None => doc!{},
        };

        let sort = match query.get("$orderby") {
            Some(&Bson::Document(ref doc)) => doc.clone(),
            _ => doc!{},
        };

        let command = match cmd_type {
            CommandType::Find => {
                doc! {
                "find" => coll_name,
                "filter" => filter,
                "projection" => projection,
                "skip" => number_to_skip,
                "limit" => number_to_return,
                "batchSize" => batch_size,
                "sort" => sort
            }
            }
            _ => query.clone(),
        };


        let init_time = time::precise_time_ns();
        let result = Message::new_query(req_id,
                                        flags,
                                        namespace.to_owned(),
                                        number_to_skip,
                                        batch_size,
                                        query.clone(),
                                        return_field_selector);

        let message = try!(result);

        if cmd_type != CommandType::Suppressed {
            let hook_result = client.run_start_hooks(&CommandStarted {
                command: command,
                database_name: db_name,
                command_name: String::from(cmd_name),
                request_id: req_id as i64,
                connection_string: connstring.clone(),
            });

            if let Err(_) = hook_result {
                return Err(Error::EventListenerError(None));
            }
        }

        try_or_emit!(cmd_type,
                     cmd_name,
                     req_id,
                     connstring,
                     message.write(socket),
                     client);
        let reply = try_or_emit!(cmd_type,
                                 cmd_name,
                                 req_id,
                                 connstring,
                                 Message::read(socket),
                                 client);

        let fin_time = time::precise_time_ns();

        let (doc, buf, cursor_id, namespace) = if is_cmd_cursor {
            try_or_emit!(cmd_type,
                         cmd_name,
                         req_id,
                         connstring,
                         Cursor::get_bson_and_cursor_info_from_command_message(reply),
                         client)
        } else {
            let (doc, buf, id) = try_or_emit!(cmd_type,
                                              cmd_name,
                                              req_id,
                                              connstring,
                                              Cursor::get_bson_and_cid_from_message(reply),
                                              client);
            (doc, buf, id, namespace)
        };

        let vec: Vec<_> = buf.iter().map(|doc| Bson::Document(doc.clone())).collect();

        let reply = match cmd_type {
            CommandType::Find => {
                doc! {
                "cursor" => {
                    "id" => cursor_id,
                    "ns" => (&namespace[..]),
                    "firstBatch" => (Bson::Array(vec))
                },
                "ok" => 1
            }
            }
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

        Ok(Cursor {
            client: client,
            namespace: namespace,
            batch_size: batch_size,
            cursor_id: cursor_id,
            limit: number_to_return,
            count: 0,
            buffer: buf,
            read_preference: read_preference,
            cmd_type: cmd_type.clone(),
        })
    }

    fn get_from_stream(&mut self) -> Result<()> {
        let (mut stream, _, _) = try!(self.client.acquire_stream(self.read_preference.to_owned()));
        let mut socket = stream.get_socket();

        let req_id = self.client.get_req_id();
        let get_more = Message::new_get_more(req_id,
                                             self.namespace.to_owned(),
                                             self.batch_size,
                                             self.cursor_id);

        let index = self.namespace.rfind('.').unwrap_or(self.namespace.len());
        let db_name = String::from(&self.namespace[..index]);
        let cmd_name = String::from("get_more");
        let connstring = format!("{}", try!(socket.get_ref().peer_addr()));

        if self.cmd_type != CommandType::Suppressed {
            let hook_result = self.client.run_start_hooks(&CommandStarted {
                command: doc! { "cursor_id" => (self.cursor_id) },
                database_name: db_name,
                command_name: cmd_name.clone(),
                request_id: req_id as i64,
                connection_string: connstring.clone(),
            });

            if let Err(_) = hook_result {
                return Err(Error::EventListenerError(None));
            }
        }

        try_or_emit!(self.cmd_type,
                     cmd_name,
                     req_id,
                     connstring,
                     get_more.write(socket.get_mut()),
                     self.client);
        let reply = try!(Message::read(socket.get_mut()));

        let (_, v, _) = try!(Cursor::get_bson_and_cid_from_message(reply));
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
    pub fn next_n(&mut self, n: i32) -> Result<Vec<bson::Document>> {
        let mut vec = vec![];

        for _ in 0..n {
            let bson_option = self.next();

            match bson_option {
                Some(Ok(bson)) => vec.push(bson),
                Some(Err(err)) => return Err(err),
                None => break,
            };
        }

        Ok(vec)
    }

    /// Attempts to read a batch of BSON documents from the cursor.
    ///
    /// # Return value
    ///
    /// Returns a vector containing the BSON documents that were read.
    pub fn next_batch(&mut self) -> Result<Vec<bson::Document>> {
        let n = self.batch_size;
        self.next_n(n)
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
                try!(self.get_from_stream());
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
                match self.buffer.pop_front() {
                    Some(bson) => Some(Ok(bson)),
                    None => None,
                }
            }
            Ok(false) => None,
            Err(err) => Some(Err(err)),
        }
    }
}
