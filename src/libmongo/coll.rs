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

use bson::encode::*;
use bson::formattable::*;

use util::*;
use msg::*;
use client::Client;
use cursor::Cursor;
use db::DB;

pub enum MongoIndex {
    MongoIndexName(~str),
    MongoIndexFields(~[INDEX_FIELD]),
}

impl MongoIndex {
    fn process_index_opts(flags : i32, options : Option<~[INDEX_OPTION]>) -> (Option<~str>, ~[~str]) {
        let mut opts_str: ~[~str] = ~[];

        // flags
        if (flags & BACKGROUND as i32) != 0i32 { opts_str.push(~"\"background\":true"); }
        if (flags & UNIQUE as i32) != 0i32 { opts_str.push(~"\"unique\":true"); }
        if (flags & DROP_DUPS as i32) != 0i32 { opts_str.push(~"\"dropDups\":true"); }
        if (flags & SPARSE as i32) != 0i32 { opts_str.push(~"\"spare\":true"); }

        // options
        let mut name = None;
        match options {
            None => (),
            Some(opt_arr) => {
                for opt_arr.iter().advance |&opt| {
                    opts_str.push(match opt {
                        INDEX_NAME(n) => {
                            name = Some(copy n);
                            fmt!("\"name\":\"%s\"", n)
                        }
                        EXPIRE_AFTER_SEC(exp) => fmt!("\"expireAfterSeconds\":%d", exp).to_owned(),
                        VERS(v) => fmt!("\"v\":%d", v),
                        //WEIGHTS(BsonDocument),
                        //DEFAULT_LANG(~str),
                        //OVERRIDE_LANG(~str),
                    });
                }
            }
        };

        (name, opts_str)
    }
    fn process_index_fields(    index_arr : ~[INDEX_FIELD],
                                index_opts : &mut ~[~str],
                                get_name : bool)
            -> (~str, ~[~str]) {
        let mut name = ~[];
        let mut index_str = ~[];
        for index_arr.iter().advance |&field| {
            match field {
                NORMAL(arr) => {
                    for arr.iter().advance |&(key, order)| {
                        index_str.push(fmt!("\"%s\":%d", key, order as int));
                        if get_name { name.push(fmt!("%s_%d", key, order as int)); }
                    }
                }
                HASHED(key) => {
                    index_str.push(fmt!("\"%s\":\"hashed\"", key));
                    if get_name { name.push(fmt!("%s_hashed", key)); }
                }
                GEOSPATIAL(key, geotype) => {
                    let typ = match geotype {
                        SPHERICAL => ~"2dsphere",
                        FLAT => ~"2d",
                    };
                    index_str.push(fmt!("\"%s\":\"%s\"", key, typ));
                    if get_name { name.push(fmt!("%s_%s", key, typ)); }
                }
                GEOHAYSTACK(loc, snd, sz) => {
                    index_str.push(fmt!("\"%s\":\"geoHaystack\", \"%s\":1", loc, snd));
                    if get_name { name.push(fmt!("%s_geoHaystack_%s_1", loc, snd)); }
                    (*index_opts).push(fmt!("\"bucketSize\":%?", sz));
                }
            }
        }

        (name.connect("_"), index_str)
    }

    /**
     * From either `~str` or full specification of index, gets name.
     *
     * # Returns
     * name of index (string passed in if `MongoIndexName` passed),
     * default index name if `MongoIndexFields` passed)
     */
    pub fn get_name(&self) -> ~str {
        match (copy *self) {
            MongoIndexName(s) => s,
            MongoIndexFields(arr) => {
                let mut tmp = ~[];
                let (name, _) = MongoIndex::process_index_fields(arr, &mut tmp, true);
                name
            }
        }
    }
}

pub struct Collection<'self> {
    db : ~str,
    name : ~str,
    priv client : &'self Client,
}

// TODO: checking arguments for validity?

/**
 * Having created a `Client` and connected as desired
 * to a server or cluster, users may interact with
 * collections by creating `Collection` handles to those
 * collections.
 */
impl<'self> Collection<'self> {
    /**
     * Creates a new handle to the given collection.
     * Alternative to `client.get_collection(db, collection)`.
     *
     * # Arguments
     * * `db` - name of database
     * * `coll` - name of collection to get
     * * `client` - name of client associated with `Collection`
     *
     * # Returns
     * handle to given collection
     */
    pub fn new<'a>(db : ~str, name : ~str, client : &'a Client) -> Collection<'a> {
        Collection { db : db, name : name, client : client }
    }

    /**
     * Gets `DB` containing this `Collection`.
     *
     * # Returns
     * handle to database containing this `Collection`
     */
    pub fn get_db(&self) -> DB<'self> {
        DB::new(self.db.clone(), self.client)
    }

    /**
     * Converts this collection to a capped collection.
     *
     * # Arguments
     * * `options` - array of options with which to create capped
     *                  collection
     *
     * # Returns
     * () on success, `MongoErr` on failure
     */
    // XXX test
    pub fn to_capped(&self, options : ~[COLLECTION_OPTION])
                -> Result<(), MongoErr> {
        let mut cmd = ~"";

        cmd.push_str(fmt!("\"convertToCapped\":\"%s\"", self.name));
        for options.iter().advance |&opt| {
            cmd.push_str(match opt {
                SIZE(sz) => fmt!(", \"size\":%?", sz),
                _ => return Err(MongoErr::new(
                                    ~"coll::to_capped",
                                    ~"unexpected option",
                                    ~"to_capped only takes SIZE of new cappedcollection")),
            });
        }

        let db = DB::new(self.db.clone(), self.client);
        match db.run_command(SpecNotation(fmt!("{ %s }", cmd))) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /**
     * CRUD ops.
     *
     * Different methods rather than enum of arguments
     * since complexity not decreased with enum (for
     * both users and developers), and CRUD oeprations
     * assumed reasonably stable.
     *
     * Moreover, basic operations still do take enums
     * for flexibility; easy to wrap for syntactic sugar.
     */

    /// INSERT OPS
    /**
     * Inserts given document with given write concern into collection.
     *
     * # Arguments
     * * `doc`- `BsonFormattable` to input
     * * `wc` - write concern with which to insert (`None` for default of 1,
     *          `Some` for finer specification)
     *
     * # Returns
     * () on success, `MongoErr` on failure
     *
     * # Failure Types
     * * invalid document to insert
     * * network
     */
    pub fn insert<U : BsonFormattable>(&self, doc : U, wc : Option<~[WRITE_CONCERN]>)
                -> Result<(), MongoErr> {
        let bson_doc = ~[match doc.to_bson_t() {
                Embedded(bson) => *bson,
                _ => return Err(MongoErr::new(
                                    ~"coll::insert",
                                    ~"unknown BsonDocument/Document error",
                                    ~"BsonFormattable not actually BSON formattable")),
            }];
        let msg = mk_insert(
                        self.client.inc_requestId(),
                        &self.db,
                        &self.name,
                        0i32,
                        bson_doc);

        match self.client._send_msg(msg_to_bytes(msg), (&self.db, wc), false) {
            Ok(_) => Ok(()),
            Err(e) => return Err(MongoErr::new(
                                    ~"coll::insert",
                                    ~"sending insert",
                                    fmt!("-->\n%s", e.to_str()))),
        }
    }
    /**
     * Inserts given batch of documents with given write concern and options
     * into collection.
     *
     * # Arguments
     * * `docs`- array of `BsonFormattable`s to input
     * * `flag_array` - `CONT_ON_ERR`
     * * `option_array` - [none yet]
     * * `wc` - write concern with which to insert (`None` for default of 1,
     *          `Some` for finer specification)
     *
     * # Returns
     * () on success, `MongoErr` on failure
     *
     * # Failure Types
     * * invalid document to insert (e.g. not proper format or
     *      duplicate `_id`)
     * * network
     */
    pub fn insert_batch<U : BsonFormattable>(&self, docs : ~[U],
                                                    flag_array : Option<~[INSERT_FLAG]>,
                                                    option_array : Option<~[INSERT_OPTION]>,
                                                    wc : Option<~[WRITE_CONCERN]>)
                -> Result<(), MongoErr> {
        let mut bson_docs : ~[BsonDocument] = ~[];
        for docs.iter().advance |&d| {
            bson_docs.push(match d.to_bson_t() {
                    Embedded(bson) => *bson,
                    _ => return Err(MongoErr::new(
                                    ~"coll::insert_batch",
                                    ~"some BsonDocument/Document error",
                                    ~"no idea")),
                });
        }
        let flags = process_flags!(flag_array);
        let _ = option_array;
        let msg = mk_insert(
                        self.client.inc_requestId(),
                        &self.db,
                        &self.name,
                        flags,
                        bson_docs);

        match self.client._send_msg(msg_to_bytes(msg), (&self.db, wc), false) {
            Ok(_) => Ok(()),
            Err(e) => return Err(MongoErr::new(
                                    ~"coll::insert_batch",
                                    ~"sending batch insert",
                                    fmt!("-->\n%s", e.to_str()))),
        }
    }
    // TODO check
    pub fn save<U : BsonFormattable>(&self, doc : U, wc : Option<~[WRITE_CONCERN]>)
                -> Result<(), MongoErr> {
        let bson_doc = match doc.to_bson_t() {
            Embedded(bson) => *bson,
            _ => return Err(MongoErr::new(
                                ~"coll::save",
                                ~"unknown BsonDocument/Document error",
                                ~"BsonFormattable not actually BSON formattable")),
        };
        match (copy bson_doc.find(~"id")) {
            None => self.insert(doc, wc),
            Some(id) => {
                let mut query = BsonDocument::new();
                query.append(~"_id", copy *id);
                self.update(SpecObj(query), SpecObj(copy bson_doc), Some(~[UPSERT]), None, wc)
            },
        }
    }

    /// UPDATE OPS
    /**
     * Updates documents satisfying given query with given update
     * specification and write concern.
     *
     * # Arguments
     * * `query` - `SpecNotation(~str)` or `SpecObj(BsonDocument)`
     *              specifying documents to update
     * * `update_spec` - `SpecNotation(~str)` or `SpecObj(BsonDocument)`
     *              specifying update to documents
     * * `flag_array` - `UPSERT`, `MULTI`
     * * `option_array` - [nothing yet]
     * * `wc` - write concern with which to update documents
     *
     * # Returns
     * () on success, `MongoErr` on failure
     *
     * # Failure Types
     * * invalid query or update specification
     * * getLastError
     * * network
     */
    pub fn update(&self,    query : QuerySpec,
                            update_spec : QuerySpec,
                            flag_array : Option<~[UPDATE_FLAG]>,
                            option_array : Option<~[UPDATE_OPTION]>,
                            wc : Option<~[WRITE_CONCERN]>)
                -> Result<(), MongoErr> {
        let flags = process_flags!(flag_array);
        let _ = option_array;
        let q = match query {
            SpecObj(bson_doc) => bson_doc,
            SpecNotation(s) => match (copy s).to_bson_t() {
                Embedded(bson) => *bson,
                _ => return Err(MongoErr::new(
                                        ~"coll::update",
                                        ~"query specification",
                                        fmt!("expected JSON formatted string, got %s", s))),
            },
        };
        let up = match update_spec {
            SpecObj(bson_doc) => bson_doc,
            SpecNotation(s) => match (copy s).to_bson_t() {
                Embedded(bson) => *bson,
                _ => return Err(MongoErr::new(
                                        ~"coll::update",
                                        ~"update specification",
                                        fmt!("expected JSON formatted string, got %s", s))),
            },
        };
        let msg = mk_update(
                        self.client.inc_requestId(),
                        &self.db,
                        &self.name,
                        flags,
                        q,
                        up);

        match self.client._send_msg(msg_to_bytes(msg), (&self.db, wc), false) {
            Ok(_) => Ok(()),
            Err(e) => return Err(MongoErr::new(
                                    ~"coll::update",
                                    ~"sending update",
                                    fmt!("-->\n%s", e.to_str()))),
        }
    }

    /// FIND OPS
    // TODO make more general
    priv fn process_find_opts(&self, options : Option<~[QUERY_OPTION]>) -> (i32, i32) {
        let (x, y) = (0i32, 0i32);
        let mut nskip = x; let mut nret = y;
        match options {
            None => (),
            Some(opts) => {
                for opts.iter().advance |&opt| {
                    match opt {
                        NRET(n) => nret = n as i32,
                        NSKIP(n) => nskip = n as i32,
                    }
                }
            }
        }
        (nskip, nret)
    }
    /**
     * Returns Cursor over given projection from queried documents.
     *
     * # Arguments
     * * `query` - optional `SpecNotation(~str)` or `SpecObj(BsonDocument)`
     *              specifying documents to query
     * * `proj` -  optioal `SpecNotation(~str)` or `SpecObj(BsonDocument)`
     *              specifying projection from queried documents
     * * `flag_array` - optional, `CUR_TAILABLE`, `SLAVE_OK`, `OPLOG_REPLAY`,
     *                  `NO_CUR_TIMEOUT`, `AWAIT_DATA`, `EXHAUST`,
     *                  `PARTIAL`
     *
     * # Returns
     * initialized (unqueried) Cursor on success, `MongoErr` on failure
     */
    pub fn find(&self,  query : Option<QuerySpec>,
                        proj : Option<QuerySpec>,
                        flag_array : Option<~[QUERY_FLAG]>/*,
                        option_array : Option<~[QUERY_OPTION]>*/)
                -> Result<Cursor<'self>, MongoErr> {
        // construct query (wrapped as { $query : {...} }
        //      for ease of query modification)
        let q_field = match query {
            None => BsonDocument::new(),                // empty Bson
            Some(SpecObj(bson_doc)) => bson_doc,
            Some(SpecNotation(s)) => match (copy s).to_bson_t() {
                Embedded(bson) => *bson,
                _ => return Err(MongoErr::new(
                                        ~"coll::find",
                                        ~"query specification",
                                        fmt!("expected JSON formatted string, got n%s", s))),
            },
        };
        let mut q = BsonDocument::new();
        q.put(~"$query", Embedded(~q_field));

        // construct projection
        let p = match proj {
            None => None,
            Some(SpecObj(bson_doc)) => Some(bson_doc),
            Some(SpecNotation(s)) => match (copy s).to_bson_t() {
                Embedded(bson) => Some(*bson),
                _ => return Err(MongoErr::new(
                                        ~"coll::find",
                                        ~"projection specification",
                                        fmt!("expected JSON formatted string, got %s", s))),
            },
        };

        // get flags
        let flags = process_flags!(flag_array);

        // get skip and limit if applicable
//        let (nskip, nret) = self.process_find_opts(option_array);

        // construct cursor and return
//        Ok(Cursor::new(q, p, @self, flags, nskip, nret))
        Ok(Cursor::new(q, p, self, self.client, flags))
    }
    /**
     * Returns pointer to first Bson from queried documents.
     *
     * # Arguments
     * * `query` - optional `SpecNotation(~str)` or `SpecObj(BsonDocument)`
     *              specifying documents to query
     * * `proj` -  optional `SpecNotation(~str)` or `SpecObj(BsonDocument)`
     *              specifying projection from queried documents
     * * `flag_array` - optional, `CUR_TAILABLE`, `SLAVE_OK`, `OPLOG_REPLAY`,
     *                  `NO_CUR_TIMEOUT`, `AWAIT_DATA`, `EXHAUST`,
     *                  `PARTIAL`
     *
     * # Returns
     * ~BsonDocument of first result on success, MongoErr on failure
     */
    //pub fn find_one(&self, query : Option<QuerySpec>, proj : Option<QuerySpec>, flag_array : Option<~[QUERY_FLAG]>, option_array : Option<~[QUERY_OPTION]>)
    pub fn find_one(&self, query : Option<QuerySpec>, proj : Option<QuerySpec>, flag_array : Option<~[QUERY_FLAG]>)
                -> Result<~BsonDocument, MongoErr> {
        /*let options = match option_array {
            None => Some(~[NRET(1)]),
            Some(opt) => Some(opt + ~[NRET(1)]),
        };

        let mut cur = self.find(query, proj, flag_array, options); */
        let mut cur = self.find(query, proj, flag_array);
        match cur {
            Ok(ref mut cursor) => {
                cursor.cursor_limit(-1);
                match cursor.next() {
                    Some(doc) => Ok(doc),
                    None => Err(MongoErr::new(
                                        ~"coll::find_one",
                                        ~"empty collection",
                                        ~"no documents in collection")),
                }
            },
            Err(e) => return Err(MongoErr::new(
                                    ~"coll::find_one",
                                    ~"",
                                    fmt!("-->\n%s", e.to_str()))),
        }
    }

    /// DELETE OPS
    priv fn process_delete_opts(&self, options : Option<~[DELETE_OPTION]>) -> i32 {
        let _ = options;
        0i32
    }
    /**
     * Removes specified documents from collection.
     *
     * # Arguments
     * * `query` - optional `SpecNotation(~str)` or `SpecObj(BsonDocument)`
     *              specifying documents to query
     * * `flag_array` - optional, `CUR_TAILABLE`, `SLAVE_OK`, `OPLOG_REPLAY`,
     *                  `NO_CUR_TIMEOUT`, `AWAIT_DATA`, `EXHAUST`,
     *                  `PARTIAL`
     * * `option_array` - [nothing yet]
     * * `wc` - write concern with which to perform remove
     *
     * # Returns
     * () on success, `MongoErr` on failure
     */
    pub fn remove(&self, query : Option<QuerySpec>, flag_array : Option<~[DELETE_FLAG]>, option_array : Option<~[DELETE_OPTION]>, wc : Option<~[WRITE_CONCERN]>)
                -> Result<(), MongoErr> {
        let q = match query {
            None => BsonDocument::new(),
            Some(SpecObj(bson_doc)) => bson_doc,
            Some(SpecNotation(s)) => match (copy s).to_bson_t() {
                Embedded(bson) => *bson,
                _ => return Err(MongoErr::new(
                                        ~"coll::remove",
                                        ~"query specification",
                                        fmt!("expected JSON formatted string, got %s", s))),
            },
        };
        let flags = process_flags!(flag_array);
        let _ = self.process_delete_opts(option_array);
        let msg = mk_delete(self.client.inc_requestId(), &self.db, &self.name, flags, q);

        match self.client._send_msg(msg_to_bytes(msg), (&self.db, wc), false) {
            Ok(_) => Ok(()),
            Err(e) => return Err(MongoErr::new(
                                    ~"coll::remove",
                                    ~"sending remove",
                                    fmt!("-->\n%s", e.to_str()))),
        }
    }

    /// INDICES (or "Indexes")
    /**
     * Creates index by specifying a vector of the different elements
     * that can form an index (e.g. (field,order) pairs, geographical
     * options, etc.)
     *
     * # Arguments
     * * `index_arr` - vector of index elements
     *                  (`NORMAL(vector of (field, order) pairs)`,
     *                  `HASHED(field)`,
     *                  `GEOSPATIAL(field, type)`,
     *                  `GEOHAYSTACK(loc, field, bucket)')
     * * `flag_array` - optional vector of index-creating flags:
     *                  `BACKGROUND`,
     *                  `UNIQUE`,
     *                  `DROP_DUPS`,
     *                  `SPARSE`
     * * `option_array` - optional vector of index-creating options:
     *                  `INDEX_NAME(name)`,
     *                  `EXPIRE_AFTER_SEC(nsecs)`,
     *                  `VERS(version no)`
     *
     * # Returns
     * name of index as `MongoIndexName` (in enum `MongoIndex`) on success,
     * `MongoErr` on failure
     */
    pub fn create_index(&self,  index_arr : ~[INDEX_FIELD],
                                flag_array : Option<~[INDEX_FLAG]>,
                                option_array : Option<~[INDEX_OPTION]>)
                -> Result<MongoIndex, MongoErr> {
        let coll = Collection::new(copy self.db, fmt!("%s", SYSTEM_INDEX), self.client);

        let flags = process_flags!(flag_array);
        let (x, y) = MongoIndex::process_index_opts(flags, option_array);
        let mut maybe_name = x; let mut opts = y;
        let (default_name, index) = MongoIndex::process_index_fields(
                                        index_arr,
                                        &mut opts,
                                        maybe_name.is_none());
        if maybe_name.is_none() {
            opts.push(fmt!("\"name\":\"%s\"", default_name));
            maybe_name = Some(default_name);
        }

        let index_str = fmt!("{ \"key\":{ %s }, \"ns\":\"%s.%s\", %s }",
                            index.connect(", "),
                            self.db,
                            self.name,
                            opts.connect(", "));
        match coll.insert(index_str, None) {
            Ok(_) => Ok(MongoIndexName(maybe_name.unwrap())),
            Err(e) => Err(e),
        }
    }
    // TODO index cache? XXX presently just does a create_index...
    pub fn ensure_index(&self,  index_arr : ~[INDEX_FIELD],
                                flag_array : Option<~[INDEX_FLAG]>,
                                option_array : Option<~[INDEX_OPTION]>)
                -> Result<MongoIndex, MongoErr> {
        self.create_index(index_arr, flag_array, option_array)
    }
    pub fn get_indexes(&self) -> Result<~[~str], MongoErr> {
        let coll = Collection::new(copy self.db, fmt!("%s", SYSTEM_INDEX), self.client);
        let mut cursor = match coll.find(None, None, None) {
            Ok(c) => c,
            Err(e) => return Err(e),
        };
        let mut indices = ~[];
        for cursor.advance |ind| {
            indices.push(ind.to_str());
        }
        Ok(indices)
    }
    /**
     * Drops specified index.
     *
     * # Arguments
     * * `index` - `MongoIndex` to drop specified either by explicit name
     *              or fields
     *
     * # Returns
     * () on success, `MongoErr` on failure
     */
    pub fn drop_index(&self, index : MongoIndex) -> Result<(), MongoErr> {
        let db = DB::new(copy self.db, self.client);
        match db.run_command(SpecNotation(
                    fmt!("{ \"deleteIndexes\":\"%s\", \"index\":\"%s\" }",
                        self.name,
                        index.get_name()))) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    ///Validate a collection.
    //TODO: could be using options?
    pub fn validate(&self, full: bool, scandata: bool) -> Result<~BsonDocument, MongoErr> {
        let db = self.get_db();
        match db.run_command(SpecNotation(fmt!(
            "{ \"validate\": \"%s\", \"full\": \"%s\", \"scandata\": \"%s\" }",
            self.name,
            full.to_str(),
            scandata.to_str()))) {
                Ok(doc) => Ok(doc),
                Err(e) => Err(e)
        }
    }
}
