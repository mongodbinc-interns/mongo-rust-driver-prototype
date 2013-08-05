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

use rtio = std::rt::io;

pub use std::rt::io::{Reader,Writer};

use bson::encode::*;
use bson::formattable::*;

use mongo::coll::*;
use mongo::db::*;
use mongo::util::*;

pub struct GridWriter {
    chunks: Collection,
    files: Collection,
    closed: bool,
    chunk_size: uint,
    chunk_num: uint,
    file_id: Option<Document>,
    position: uint,
}

pub struct GridReader {
    chunks: Collection,
    files: Collection,
    length: uint,
    position: uint,
    file_id: Document,
    buf: ~[u8],
}

impl rtio::Writer for GridWriter {
    pub fn write(&mut self, d: &[u8]) {
        if self.closed {
            rtio::io_error::cond.raise(rtio::IoError {
                kind: rtio::Closed,
                desc: "cannot write to a closed GridWriter",
                detail: None
            })
        }
        let mut data: ~[u8] = d.to_owned();
        if self.position > 0 {
            let space = self.chunk_size - self.position;
            let to_write = data.iter().transform(|x| *x).take_(space).collect::<~[u8]>();
            match self.flush_data(to_write) {
                Ok(_) => (),
                Err(e) => rtio::io_error::cond.raise(rtio::IoError {
                    kind: rtio::OtherIoError,
                    desc: "could not flush data to buffer",
                    detail: Some(e.to_str())
                })
            }
        }
        //to_write copies out the first chunk_size elts of data
        let mut to_write: ~[u8] = data.iter().transform(|x| *x).take_(self.chunk_size).collect();

        //data consumes those elts by skipping them
        data = data.iter().skip(self.chunk_size).transform(|x| *x).collect();

        //if we filled up the chunk, flush the chunk
        while to_write.len() != 0 && to_write.len() == self.chunk_size {
            match self.flush_data(to_write) {
                Ok(_) => (),
                Err(e) => rtio::io_error::cond.raise(rtio::IoError {
                    kind: rtio::OtherIoError,
                    desc: "could not flush data to buffer",
                    detail: Some(e.to_str())
                })
            }
            to_write = data.iter().take_(self.chunk_size).transform(|x| *x).collect();
            data = data.iter().skip(self.chunk_size).transform(|x| *x).collect();
        }
        if to_write.len() == 0 {
            return;
        }
        match self.flush_data(to_write) {
            Ok(_) => (),
            Err(e) => rtio::io_error::cond.raise(rtio::IoError {
                kind: rtio::OtherIoError,
                desc: "could not flush data to buffer",
                detail: Some(e.to_str())
            })
        }
    }

    pub fn flush(&mut self) {
        let db = self.chunks.get_db();

        let mut oid = ~"";
        match self.file_id {
            Some(ObjectId(ref v)) => {
                for v.iter().advance |&b| {
                    let mut byte = fmt!("%x", b as uint);
                    if byte.len() == 1 {
                        byte = (~"0").append(byte)
                    }
                    oid.push_str(byte);
                }
            }
            _ => ()
        }

        let mut ioerr: Option<rtio::IoError> = None;

        let md5 = match db.run_command(SpecNotation(
            fmt!("{ 'filemd5': '%s', 'root': 'fs' }", oid))) {
            Ok(d) => match d.find(~"md5") {
                Some(&UString(ref s)) => s.clone(),
                _ => {
                    ioerr = Some(rtio::IoError {
                        kind: rtio::OtherIoError,
                        desc: "could not get filemd5 from server",
                        detail: None
                    });
                    ~""
                }
            },
            Err(e) => {
                ioerr = Some(rtio::IoError {
                    kind: rtio::OtherIoError,
                    desc: "could not get filemd5 from server",
                    detail: Some(e.to_str())
                });
                ~""
            }
        };

        if ioerr.is_some() {
            rtio::io_error::cond.raise(ioerr.unwrap());
        }

        let mut file = BsonDocument::new();
        file.put(~"md5", UString(md5));
        file.put(~"length", Int32(self.position as i32));
        file.put(~"_id", self.file_id.clone().unwrap());
        file.put(~"chunkSize", self.chunk_size.to_bson_t());
        file.put(~"filename", UString(~""));
        file.put(~"contentType", UString(~""));
        file.put(~"aliases", UString(~""));
        file.put(~"metadata", UString(~""));
        //TODO: needs an uploadDate field,
        //assuming there is a reasonable date library
        match self.files.insert(file, None) {
            Ok(_) => (),
            Err(e) => rtio::io_error::cond.raise(rtio::IoError {
                kind: rtio::OtherIoError,
                desc: "could not store metadata",
                detail: Some(e.to_str())
            })
        }
    }
}

impl GridWriter {
    pub fn new(db: &DB) -> GridWriter {
        let chunks = db.get_collection(~"fs.chunks");
        let files = db.get_collection(~"fs.files");
        match chunks.ensure_index(~[NORMAL(~[(~"files_id", ASC), (~"n", ASC)])], None, None) {
            Ok(_) => (),
            Err(e) => fail!(e.to_str())
        }
        GridWriter {
            chunks: chunks,
            files: files,
            closed: false,
            chunk_size: 256 * 1024,
            chunk_num: 0,
            file_id: None,
            position: 0,
        }
    }

    pub fn close(&mut self) -> Result<(), MongoErr> {
        let mut res = Ok(());
        if !self.closed {
            do rtio::io_error::cond.trap(|c| {
                res = Err(MongoErr::new(
                    ~"gridfile::close",
                    ~"unable to flush buffer",
                    c.desc.to_owned()));
            }).in {
                self.flush();
                self.closed = true;
            }
        }
        res
    }

    fn flush_data(&mut self, data: &[u8]) -> Result<(), MongoErr> {
        let mut chunk = BsonDocument::new();
        chunk.put(~"n", Int32(self.chunk_num as i32));
        chunk.put(~"data", Binary(0u8, data.clone().to_owned()));
        if self.file_id.is_none() {
            match self.chunks.find_one(Some(SpecObj(chunk.clone())), None, None) {
                Ok(d) => match d.find(~"files_id") {
                    Some(id) =>  self.file_id = Some(id.clone()),
                    _ => self.file_id = Some(ObjIdFactory::new().oid())
                },
                _ => self.file_id = Some(ObjIdFactory::new().oid())
            }
        }
        chunk.put(~"files_id", self.file_id.clone().unwrap());
        match self.chunks.insert(chunk.clone(), None) {
            Ok(_) => (),
            Err(e) => return Err(e)
        }
        self.position += data.len();
        self.chunk_num += 1;
        Ok(())
    }
}

impl rtio::Reader for GridReader {
    pub fn read(&mut self, buf: &mut [u8]) -> Option<uint> {
        let mut size = buf.len();
        if size == 0 { return None; }
        let remainder = self.length - self.position;
        if size < 0 || size > remainder {
            size = remainder;
        }

        let mut received = self.buf.len();

        while received < size {
            let mut find = BsonDocument::new();
            find.put(~"files_id", self.file_id.clone());
            let chunk = match self.chunks.find_one(Some(SpecObj(find)),
                None, None) {
                Ok(d) => d,
                Err(_) => return None
            };

            let data = match chunk.find(~"data") {
                Some(&Binary(_, ref v)) => v.clone(),
                _ => return None
            };

            received += data.len();
            for data.iter().advance |&elt| {
                buf[self.position] = elt;
                self.position += 1;
            }
        }
        Some(received)
    }

    pub fn eof(&mut self) -> bool {
        self.position >= self.length
    }
}

impl GridReader {
    pub fn new(db: &DB,
               file_id: Document)
        -> GridReader {
        let chunks = db.get_collection(~"fs.chunks");
        let files = db.get_collection(~"fs.files");
        let mut doc = BsonDocument::new();
        doc.put(~"_id", file_id.clone());
        let len = match files.find_one(Some(SpecObj(doc)), None, None) {
            Ok(d) => match d.find(~"length") {
                Some(&Int32(i)) => i as uint,
                Some(&Double(f)) => f as uint,
                Some(&Int64(i)) => i as uint,
                _ => fail!("could not create new GridReader; length was invalid")
            },
            Err(e) => fail!(e.to_str())
        };
        GridReader {
            chunks: chunks,
            files: files,
            file_id: file_id,
            length: len,
            position: 0,
            buf: ~[],
        }
    }
}
