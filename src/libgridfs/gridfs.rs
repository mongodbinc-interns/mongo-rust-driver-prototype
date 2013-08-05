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

#[link(name="gridfs", vers="0.1.0", author="austin.estep@10gen.com, jaoke.chinlee@10gen.com")];
#[crate_type="lib"];
#[license="Apache 2.0"];
extern mod bson;
extern mod mongo;
extern mod std;
extern mod extra;

use bson::encode::*;

use mongo::db::*;
use mongo::coll::*;
use mongo::util::*;

use gridfile::*;

pub mod gridfile;

pub struct GridFS {
    db: @DB,
    files: Collection,
    chunks: Collection,
    last_id: Option<Document>,
}

impl GridFS {
    /**
     * Create a new GridFS handle on the given DB.
     * The GridFS handle uses the collections
     * "fs.files" and "fs.chunks".
     */
    pub fn new(db: @DB) -> GridFS {
        GridFS {
            db: db,
            files: db.get_collection(~"fs.files"),
            chunks: db.get_collection(~"fs.chunks"),
            last_id: None,
        }
    }

    pub fn file_write(&self) -> GridWriter {
        GridWriter::new(self.db)
    }

    pub fn put(&mut self, data: ~[u8]) -> Result<(), MongoErr> {
        use std::rt::io::io_error;

        let mut res = Ok(());
        let mut file = self.file_write();
        do io_error::cond.trap(|c| {
            res = Err(MongoErr::new(
                ~"grid::put",
                c.desc.to_owned(),
                if c.detail.is_some() {c.detail.unwrap()}
                else {~"method returned without error detail"}));
        }).in {
            file.write(data);
            file.close();
            self.last_id = file.file_id.clone();
        }
        res
    }

    pub fn delete(&self, id: Document) -> Result<(), MongoErr> {
        let mut file_doc = BsonDocument::new();
        let mut chunk_doc = BsonDocument::new();
        file_doc.put(~"_id", id.clone());
        chunk_doc.put(~"files_id", id);
        result_and(
            self.files.remove(Some(SpecObj(file_doc)), None, None, None),
            self.chunks.remove(Some(SpecObj(chunk_doc)), None, None, None)
        )
    }

    pub fn file_read(&self, id: Document) -> GridReader {
        GridReader::new(self.db, id)
    }

}

priv fn result_and<T,U>(r1: Result<T,U>, r2: Result<T,U>) -> Result<T,U> {
    match r1 {
        Ok(k) => match r2 {
            Ok(_) => Ok(k),
            Err(e) => return Err(e)
        },
        Err(e) => Err(e)
    }
}
