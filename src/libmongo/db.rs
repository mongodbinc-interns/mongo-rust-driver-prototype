/* Copyright 2013 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::int::range;
use std::libc::c_int;
use std::ptr::to_unsafe_ptr;
use std::to_bytes::*;

use bson::encode::*;
use bson::formattable::*;

use util::*;
use client::Client;
use coll::Collection;

static L_END: bool = true;

pub struct DB<'self> {
    name : ~str,
    priv client : &'self Client,
}

#[link_args = "-lmd5"]
extern {
    fn md5_init(pms: *MD5State);
    fn md5_append(pms: *MD5State, data: *const u8, nbytes: c_int);
    fn md5_finish(pms: *MD5State, digest: *[u8,..16]);
}

priv struct MD5State {
    count: [u32,..2],
    abcd: [u32,..4],
    buf: [u8,..64]
}

impl MD5State {
    fn new(len: u64) -> MD5State {
        let mut c: [u32,..2] = [0u32,0];
        let l = len.to_bytes(L_END);
        c[0] |= l[0] as u32;
        c[0] |= (l[1] << 8) as u32;
        c[0] |= (l[2] << 16) as u32;
        c[0] |= (l[3] << 24) as u32;
        c[1] |= l[4] as u32;
        c[1] |= (l[5] << 8) as u32;
        c[1] |= (l[6] << 16) as u32;
        c[1] |= (l[7] << 24) as u32;

        MD5State {
            count: c,
            abcd: [0u32,0,0,0],
            buf: [
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0
                ]
        }
    }
}

/**
 * Having created a `Client` and connected as desired
 * to a server or cluster, users may interact with
 * databases by creating `DB` handles to those databases.
 */
impl<'self> DB<'self> {
    /**
     * Creates a new Mongo DB with given name and associated Client.
     *
     * # Arguments
     * * `name` - name of DB
     * * `client` - Client with which this DB is associated
     *
     * # Returns
     * DB (handle to database)
     */
    pub fn new(name : ~str, client : &'self Client) -> DB<'self> {
        DB {
            name : name,
            client : client
        }
    }

    // COLLECTION INTERACTION
    /**
     * Gets names of all collections in this `DB`, returning error
     * if any fail. Names do not include `DB` name, i.e. are not
     * full namespaces.
     *
     * # Returns
     * vector of collection names on success, `MongoErr` on failure
     *
     * # Failure Types
     * * error querying `system.indexes` collection
     * * response from server not in expected form (must contain
     *      vector of `BsonDocument`s each containing "name" fields of
     *      `UString`s)
     */
    pub fn get_collection_names(&self) -> Result<~[~str], MongoErr> {
        let mut names : ~[~str] = ~[];

        // query on namespace collection
        let coll = Collection::new(copy self.name, fmt!("%s", SYSTEM_NAMESPACE), self.client);
        let mut cur = match coll.find(None, None, None) {
            Ok(cursor) => cursor,
            Err(e) => return Err(e),
        };

        // pull out all the names, returning error if any fail
        for cur.advance |doc| {
            match doc.find(~"name") {
                Some(val) => {
                    let tmp = copy *val;
                    match tmp {
                        UString(s) => {
                            // ignore special collections (with "$")
                            if !s.contains_char('$') {
                                names.push(s.slice_from(self.name.len()+1).to_owned());
                            }
                        },
                        _ => return Err(MongoErr::new(
                                    ~"db::get_collection_names",
                                    fmt!("db %s", self.name),
                                    ~"got non-string collection name")),
                    }
                },
                None => return Err(MongoErr::new(
                                ~"db::get_collection_names",
                                fmt!("db %s", self.name),
                                ~"got no name for collection")),

            }
        }

        Ok(names)
    }
    /**
     * Gets `Collection`s in this `DB`, returning error if any fail.
     *
     * # Returns
     * vector of `Collection`s on success, `MongoErr` on failure
     *
     * # Failure Types
     * * errors propagated from `get_collection_names`
     */
    pub fn get_collections(&self) -> Result<~[Collection<'self>], MongoErr> {
        let names = match self.get_collection_names() {
            Ok(n) => n,
            Err(e) => return Err(e),
        };

        let mut coll : ~[Collection<'self>] = ~[];
        for names.iter().advance |&n| {
            coll = coll + ~[Collection::new(self.name.clone(), n, self.client)];
        }

        Ok(coll)
    }
    /**
     * Creates collection with given options.
     *
     * # Arguments
     * * `coll` - name of collection to create
     * * `flag_array` - collection creation flags
     * * `option_array` - collection creation options
     *
     * # Returns
     * handle to collection on success, `MongoErr` on failure
     */
    pub fn create_collection(   &self,
                                coll : ~str,
                                flag_array : Option<~[COLLECTION_FLAG]>,
                                option_array : Option<~[COLLECTION_OPTION]>)
            -> Result<Collection<'self>, MongoErr> {
        let flags = process_flags!(flag_array);
        let cmd = fmt!( "{ \"create\":\"%s\", %s }",
                        coll,
                        self.process_create_ops(flags, option_array));
        match self.run_command(SpecNotation(cmd)) {
            Ok(_) => Ok(Collection::new(self.name.clone(), coll, self.client)),
            Err(e) => Err(e),
        }
    }
    priv fn process_create_ops(&self, flags : i32, options : Option<~[COLLECTION_OPTION]>)
            -> ~str {
        let mut opts_str = ~"";
        opts_str.push_str(fmt!( "\"autoIndexId\":%? ",
                                (flags & AUTOINDEX_ID as i32) != 0i32));

        match options {
            None => (),
            Some(opt_arr) => {
                for opt_arr.iter().advance |&opt| {
                    opts_str.push_str(match opt {
                        CAPPED(sz) => fmt!(", \"capped\":true, \"size\":%?", sz),
                        SIZE(sz) => fmt!(", \"size\":%?", sz),
                        MAX_DOCS(k) => fmt!(", \"max\":%?", k),
                    })
                }
            }
        }

        opts_str
    }
    /**
     * Gets handle to collection with given name, from this `DB`.
     *
     * # Arguments
     * * `coll` - name of `Collection` to get
     *
     * # Returns
     * handle to collection
     */
    pub fn get_collection(&self, coll : ~str) -> Collection<'self> {
        Collection::new(self.name.clone(), coll, self.client)
    }
    /**
     * Drops given collection from database associated with this `DB`.
     *
     * # Arguments
     * * `coll` - name of collection to drop
     *
     * # Returns
     * () on success, `MongoErr` on failure
     */
    pub fn drop_collection(&self, coll : ~str) -> Result<(), MongoErr> {
        match self.run_command(SpecNotation(fmt!("{ \"drop\":\"%s\" }", coll))) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    // TODO make take options? (not strictly necessary but may be good?)
    // TODO allow other query options, e.g. SLAVE_OK, with helper function
    /**
     * Runs given command (taken as `BsonDocument` or `~str`).
     *
     * # Arguments
     * * `cmd` - command to run, taken as `SpecObj(BsonDocument)` or
     *              `SpecNotation(~str)`
     *
     * # Returns
     * `~BsonDocument` response from server on success that must be parsed
     * appropriately by caller, `MongoErr` on failure
     */
    pub fn run_command(&self, cmd : QuerySpec) -> Result<~BsonDocument, MongoErr> {
        let coll = Collection::new(copy self.name, fmt!("%s", SYSTEM_COMMAND), self.client);

        //let ret_msg = match coll.find_one(Some(cmd), None, None, None) {
        let ret_msg = match coll.find_one(Some(copy cmd), None, Some(~[NO_CUR_TIMEOUT])) {
            Ok(msg) => msg,
            Err(e) => return Err(MongoErr::new(
                                    ~"db::run_command",
                                    fmt!("error getting return value from run_command %?", cmd),
                                    fmt!("-->\n%s", e.to_str()))),
        };

        // check if run_command succeeded
        let ok = match ret_msg.find(~"ok") {
            Some(x) => match *x {
                Double(v) => v,
                _ => return Err(MongoErr::new(
                                    ~"db::run_command",
                                    fmt!("error in returned value from run_command %?", cmd),
                                    fmt!("\"ok\" field contains %?", *x))),
            },
            None => return Err(MongoErr::new(
                                    ~"db::run_command",
                                    fmt!("error in returned value from run_command %?", cmd),
                                    ~"no \"ok\" field in return message!")),
        };
        match ok {
            0f64 => (),
            _ => return Ok(ret_msg)
        }

        // otherwise, extract error message
        let errmsg = match ret_msg.find(~"errmsg") {
            Some(x) => match *x {
                UString(ref s) => s,
                _ => return Err(MongoErr::new(
                                    ~"db::run_command",
                                    fmt!("error in returned value from run_command %?", cmd),
                                    fmt!("\"errmsg\" field contains %?", *x))),
            },
            None => return Err(MongoErr::new(
                                    ~"db::run_command",
                                    fmt!("error in returned value from run_comand %?", cmd),
                                    ~"run_command failed without msg!")),
        };

        Err(MongoErr::new(
                ~"db::run_command",
                fmt!("run_command %? failed", cmd),
                copy *errmsg))
    }

    /**
     * Parses write concern into bytes and sends to server.
     *
     * # Arguments
     * * `wc` - write concern, i.e. getLastError specifications
     *
     * # Returns
     * () on success, `MongoErr` on failure
     *
     * # Failure Types
     * * invalid write concern specification (should never happen)
     * * network
     * * getLastError error, e.g. duplicate ```_id```s
     */
    pub fn get_last_error(&self, wc : Option<~[WRITE_CONCERN]>) -> Result<(), MongoErr>{
        // set default write concern (to 1) if not specified
        let concern = match wc {
            None => ~[W_N(1), FSYNC(false)],
            Some(w) => w,
        };
        // parse write concern, early exiting if set to <= 0
        let mut concern_str = ~"{ \"getLastError\":1";
        for concern.iter().advance |&opt| {
            concern_str.push_str(match opt {
                JOURNAL(j) => fmt!(", \"j\":%?", j),
                W_N(w) => {
                    if w <= 0 { return Ok(()); }
                    else { fmt!(", \"w\":%d", w) }
                }
                W_STR(w) => fmt!(", \"w\":\"%s\"", w),
                WTIMEOUT(t) => fmt!(", \"wtimeout\":%d", t),
                FSYNC(s) => fmt!(", \"fsync\":%?", s),
            });
        }
        concern_str.push_str(~" }");

        // run_command and get entire doc
        let err_doc_tmp = match self.run_command(SpecNotation(concern_str)) {
            Ok(doc) => doc,
            Err(e) => return Err(MongoErr::new(
                                    ~"db::get_last_error",
                                    ~"run_command error",
                                    fmt!("-->\n%s", e.to_str()))),
        };

        // error field name possibitilies
        let err_field = ~[  err_doc_tmp.find(~"err"),
                            err_doc_tmp.find(~"$err")];

        // search for error field
        let mut err_found = false;
        let mut err_doc = Int32(1); // [invalid err_doc]
        for err_field.iter().advance |&err_result| {
            match err_result {
                None => (),
                Some(doc) => {
                    err_found = true;
                    err_doc = copy *doc;
                }
            }
        };

        if !err_found {
            return Err(MongoErr::new(
                            ~"db::get_last_error",
                            ~"getLastError unexpected format",
                            ~"no $err field in reply"));
        }

        // unwrap error message
        match err_doc {
            Null => Ok(()),
            UString(s) => Err(MongoErr::new(
                            ~"db::get_last_error",
                            ~"getLastError error",
                            copy s)),
            _ => Err(MongoErr::new(
                            ~"db::get_last_error",
                            ~"getLastError unexpected format",
                            ~"unknown last error in reply")),
        }
    }

    ///Add a new database user with the given username and password.
    ///If the system.users collection becomes unavailable, this will fail.
    pub fn add_user(&self, username: ~str, password: ~str, roles: ~[~str]) -> Result<(), MongoErr>{
        let coll = self.get_collection(~"system.users");
        let mut user = match coll.find_one(Some(SpecNotation(fmt!("{ \"user\": \"%s\" }", username))), None, None)
            {
                Ok(u) => u,
                Err(_) => {
                    let mut doc = BsonDocument::new();
                    doc.put(~"user", UString(copy username));
                    ~doc
                }
            };
        user.put(~"pwd", UString(md5(fmt!("%s:mongo:%s", username, password))));
        user.put(~"roles", roles.to_bson_t());
        coll.save(user, None)
    }

    ///Become authenticated as the given username with the given password.
    pub fn authenticate(&self, username: ~str, password: ~str) -> Result<(), MongoErr> {
        let nonce = match self.run_command(SpecNotation(~"{ \"getnonce\": 1 }")) {
            Ok(doc) => match *doc.find(~"nonce").unwrap() { //this unwrap should always succeed
                UString(ref s) => copy *s,
                _ => return Err(MongoErr::new(
                    ~"db::authenticate",
                    ~"error while getting nonce",
                    fmt!("an invalid nonce (%?) was returned by the server", *doc.find(~"nonce").unwrap())))
            },
            Err(e) => return Err(e)
        };
        match self.run_command(SpecNotation(fmt!(" {
              \"authenticate\": 1,
              \"user\": \"%s\",
              \"nonce\": \"%s\",
              \"key\": \"%s\" } ",
              username,
              nonce,
              md5(fmt!("%s%s%s", nonce, username, md5(fmt!("%s:mongo:%s",username, password))))))) {
           Ok(_) => return Ok(()),
           Err(e) => return Err(e)
        }
    }

    ///Log out of the current user.
    ///Closing a connection will also log out.
    pub fn logout(&self) -> Result<(), MongoErr> {
        match self.run_command(SpecNotation(~"{ \"logout\": 1 }")) {
            Ok(doc) => match *doc.find(~"ok").unwrap() {
                Double(1f64) => return Ok(()),
                Int32(1i32) => return Ok(()),
                Int64(1i64) => return Ok(()),
                _ => return Err(MongoErr::new(
                    ~"db::logout",
                    ~"error while logging out",
                    ~"the server returned ok: 0"))
            },
            Err(e) => return Err(e)
        };
    }

    ///Get the profiling level of the database.
    pub fn get_profiling_level(&self) -> Result<int, MongoErr> {
        match self.run_command(SpecNotation(~"{ \"profile\": -1 }")) {
            Ok(d) => match d.find(~"was") {
                Some(&Double(f)) => Ok(f as int),
                _ => return Err(MongoErr::new(
                    ~"db::get_profiling_level",
                    ~"could not get profiling level",
                    ~"an invalid profiling level was returned"))
            },
            Err(e) => return Err(e)
        }
    }

    ///Set the profiling level of the database.
    pub fn set_profiling_level(&self, level: int) -> Result<~BsonDocument, MongoErr> {
        self.run_command(SpecNotation(fmt!("{ \"profile\": %d }", level)))
    }
}

priv fn md5(msg: &str) -> ~str {
    let msg_bytes = msg.to_bytes(L_END);
    let m = MD5State::new(msg_bytes.len() as u64);
    let digest: [u8,..16] = [
        0,0,0,0,
        0,0,0,0,
        0,0,0,0,
        0,0,0,0
    ];

    unsafe {
        md5_init(to_unsafe_ptr(&m));
        md5_append(to_unsafe_ptr(&m), to_unsafe_ptr(&(msg_bytes[0])), msg_bytes.len() as i32);
        md5_finish(to_unsafe_ptr(&m), to_unsafe_ptr(&digest));
    }

    let mut result: ~str = ~"";
    for range(0, 16) |i| {
        let mut byte = fmt!("%x", digest[i] as uint);
        if byte.len() == 1 {
            byte = (~"0").append(byte);
        }
        result.push_str(byte);
    }
    result
}

#[cfg(test)]
#[test]
fn md5_test() {
    assert_eq!(md5(~"hello"), ~"5d41402abc4b2a76b9719d911017c592");
    assert_eq!(md5(~"asdfasdfasdf"), ~"a95c530a7af5f492a74499e70578d150");
}
