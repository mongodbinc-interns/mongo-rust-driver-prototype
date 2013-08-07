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

use std::str::from_bytes;
use std::int::range;
use std::cast::transmute;
use encode::*;
use tools::stream::*;

static L_END: bool = true;

//Format codes
static DOUBLE: u8 = 0x01;
static STRING: u8 = 0x02;
static EMBED: u8 = 0x03;
static ARRAY: u8 = 0x04;
static BINARY: u8 = 0x05;
static OBJID: u8 = 0x07;
static BOOL: u8 = 0x08;
static UTCDATE: u8 = 0x09;
static NULL: u8 = 0x0A;
static REGEX: u8 = 0x0B;
static DBREF: u8 = 0x0C;
static JSCRIPT: u8 = 0x0D;
static JSCOPE: u8 = 0x0F;
static INT32: u8 = 0x10;
static TSTAMP: u8 = 0x11;
static INT64: u8 = 0x12;
static MINKEY: u8 = 0xFF;
static MAXKEY: u8 = 0x7F;

///Parser object for BSON. T is constrained to Stream<u8>.
pub struct BsonParser<T> {
    stream: T
}

///Collects up to 8 bytes in order as a u64.
priv fn bytesum(bytes: &[u8]) -> u64 {
    let mut i = 0;
    let mut ret: u64 = 0;
    for bytes.iter().advance |&byte| {
        ret |= (byte as u64) >> (8 * i);
        i += 1;
    }
    ret
}

impl<T:Stream<u8>> BsonParser<T> {

    ///Parse a byte stream into a BsonDocument. Returns an error string on parse failure.
    ///Initializing a BsonParser and calling document() will fully convert a ~[u8]
    ///into a BsonDocument if it was formatted correctly.
    pub fn document(&mut self) -> Result<BsonDocument,~str> {
        let size = bytesum(self.stream.aggregate(4)) as i32;
        let mut elemcode = self.stream.expect(&[
            DOUBLE,STRING,EMBED,ARRAY,BINARY,OBJID,
            BOOL,UTCDATE,NULL,REGEX,DBREF,JSCRIPT,JSCOPE,
            INT32,TSTAMP,INT64,MINKEY,MAXKEY]);
        self.stream.pass(1);
        let mut ret = BsonDocument::new();
        while elemcode != None {
            let key = self.cstring();
            let val: Document = match elemcode {
                Some(DOUBLE) => self._double(),
                Some(STRING) => self._string(),
                Some(EMBED) => {
                    let doc = self._embed();
                    match doc {
                        Ok(d) => d,
                        Err(e) => return Err(e)
                    }
                }
                Some(ARRAY) => {
                    let doc = self._array();
                    match doc {
                        Ok(d) => d,
                        Err(e) => return Err(e)
                    }
                }
                Some(BINARY) => self._binary(),
                Some(OBJID) => ObjectId(self.stream.aggregate(12)),
                Some(BOOL) => self._bool(),
                Some(UTCDATE) => UTCDate(bytesum(self.stream.aggregate(8)) as i64),
                Some(NULL) => Null,
                Some(REGEX) => self._regex(),
                Some(DBREF) => {
                    let doc = self._dbref();
                    match doc {
                        Ok(d) => d,
                        Err(e) => return Err(e)
                    }
                }
                Some(JSCRIPT) => {
                    let doc = self._jscript();
                    match doc {
                        Ok(d) => d,
                        Err(e) => return Err(e)
                    }
                }
                Some(JSCOPE) => {
                    let doc = self._jscope();
                    match doc {
                        Ok(d) => d,
                        Err(e) => return Err(e)
                    }
                }
                Some(INT32) => Int32(bytesum(self.stream.aggregate(4)) as i32),
                Some(TSTAMP) => Timestamp(bytesum(self.stream.aggregate(4)) as u32,
                    bytesum(self.stream.aggregate(4)) as u32),
                Some(INT64) => Int64(bytesum(self.stream.aggregate(8)) as i64),
                Some(MINKEY) => MinKey,
                Some(MAXKEY) => MaxKey,
                _ => return Err(~"an invalid element code was found")
            };
            ret.put(key, val);
            elemcode = self.stream.expect(&[
                DOUBLE,STRING,EMBED,ARRAY,BINARY,OBJID,
                BOOL,UTCDATE,NULL,REGEX,DBREF,JSCRIPT,JSCOPE,
                INT32,TSTAMP,INT64,MINKEY,MAXKEY]);
            if self.stream.has_next() { self.stream.pass(1); }
        }
        ret.size = size;
        Ok(ret)
    }

    ///Parse a string without denoting its length. Mainly for keys.
    fn cstring(&mut self) -> ~str {
        let is_0: &fn(&u8) -> bool = |&x| x == 0x00;
        let s = from_bytes(self.stream.until(is_0));
        self.stream.pass(1);
        s
    }

    ///Parse a double.
    fn _double(&mut self) -> Document {
        let mut u: u64 = 0;
        for range(0,8) |i| {
            //TODO: how will this hold up on big-endian architectures?
            u |= (*self.stream.first() as u64 << ((8 * i)));
            self.stream.pass(1);
        }
        let v: &f64 = unsafe { transmute(&u) };
        Double(*v)
    }

    ///Parse a string with length.
    fn _string(&mut self) -> Document {
        self.stream.pass(4); //skip length
        let v = self.cstring();
        UString(v)
    }
    ///Parse an embedded object. May fail.
    fn _embed(&mut self) -> Result<Document,~str> {
        return self.document().chain(|s| Ok(Embedded(~s)));
    }
    ///Parse an embedded array. May fail.
    fn _array(&mut self) -> Result<Document,~str> {
        return self.document().chain(|s| Ok(Array(~s)));
    }
    ///Parse generic binary data.
    fn _binary(&mut self) -> Document {
        let count = bytesum(self.stream.aggregate(4));
        let subtype = *(self.stream.first());
        self.stream.pass(1);
        let data = self.stream.aggregate(count as uint);
        Binary(subtype, data)
    }
    ///Parse a boolean.
    fn _bool(&mut self) -> Document {
        let ret = (*self.stream.first()) as bool;
        self.stream.pass(1);
        Bool(ret)
    }
    ///Parse a regex.
    fn _regex(&mut self) -> Document {
        let s1 = self.cstring();
        let s2 = self.cstring();
        Regex(s1, s2)
    }
    fn _dbref(&mut self) -> Result<Document, ~str> {
        let s = match self._string() {
            UString(rs) => rs,
            _ => return Err(~"invalid string found in dbref")
        };
        let d = self.stream.aggregate(12);
        Ok(DBRef(s, ~ObjectId(d)))
    }
    ///Parse a javascript object.
    fn _jscript(&mut self) -> Result<Document, ~str> {
        let s = self._string();
        //using this to avoid irrefutable pattern error
        match s {
            UString(s) => Ok(JScript(s)),
            _ => Err(~"invalid string found in javascript")
        }
    }

    ///Parse a scoped javascript object.
    fn _jscope(&mut self) -> Result<Document,~str> {
        self.stream.pass(4);
        let s = self.cstring();
        let doc = self.document();
        return doc.chain(|d| Ok(JScriptWithScope(s.clone(),~d)));
    }

    ///Create a new parser with a given stream.
    pub fn new(stream: T) -> BsonParser<T> { BsonParser { stream: stream } }
}

///Standalone decode binding.
///This is equivalent to initializing a parser and calling document().
pub fn decode(b: ~[u8]) -> Result<BsonDocument,~str> {
    let mut parser = BsonParser::new(b);
    parser.document()
}

#[cfg(test)]
mod tests {
    use super::*;
    use encode::*;
    use extra::test::BenchHarness;

    #[test]
    fn test_decode_size() {
        let doc = decode(~[10,0,0,0,10,100,100,100,0]);
        assert_eq!(doc.unwrap().size, 10);
    }

    #[test]
    fn test_cstring_decode() {
        let stream: ~[u8] = ~[104,101,108,108,111,0];
        let mut parser = BsonParser::new(stream);
        assert_eq!(parser.cstring(), ~"hello");
    }

    #[test]
    fn test_double_decode() {
        let stream: ~[u8] = ~[110,134,27,240,249,33,9,64];
        let mut parser = BsonParser::new(stream);
        let d = parser._double();
        match d {
            Double(d2) => {
                assert!(d2.approx_eq(&3.14159f64));
            }
            _ => fail!("failed in a test case; how did I get here?")
        }
    }

    #[test]
    fn test_document_decode() {
        let stream1: ~[u8] = ~[11,0,0,0,8,102,111,111,0,1,0];
        let mut parser1 = BsonParser::new(stream1);
        let mut doc1 = BsonDocument::new();
        doc1.put(~"foo", Bool(true));
        assert_eq!(parser1.document().unwrap(), doc1);

        let stream2: ~[u8] = ~[45,0,0,0,4,102,111,111,0,22,0,0,0,2,48,0,
            6,0,0,0,104,101,108,108,111,0,8,49,0,0,
            0,2,98,97,122,0,4,0,0,0,113,117,120,0,0];
        let mut inside = BsonDocument::new();
        inside.put_all(~[(~"0", UString(~"hello")), (~"1", Bool(false))]);
        let mut doc2 = BsonDocument::new();
        doc2.put_all(~[(~"foo", Array(~ copy inside)), (~"baz", UString(~"qux"))]);
        assert_eq!(decode(stream2).unwrap(), doc2);
    }

    #[test]
    fn test_binary_decode() {
        let stream: ~[u8] = ~[6,0,0,0,0,1,2,3,4,5,6];
        let mut parser = BsonParser::new(stream);
        assert_eq!(parser._binary(), Binary(0, ~[1,2,3,4,5,6]));
    }

    #[test]
    fn test_dbref_encode() {
        let mut doc = BsonDocument::new();
        doc.put(~"foo", DBRef(~"bar", ~ObjectId(~[0u8,1,2,3,4,5,6,7,8,9,10,11])));
        let stream: ~[u8] = ~[30,0,0,0,12,102,111,111,0,4,0,0,0,98,97,114,0,0,1,2,3,4,5,6,7,8,9,10,11,0];
        assert_eq!(decode(stream).unwrap(), doc)
    }

    //TODO: get bson strings of torture-test objects
    #[bench]
    fn bench_basic_obj_decode(b: &mut BenchHarness) {
         do b.iter {
             let stream: ~[u8] = ~[45,0,0,0,4,102,111,
             111,0,22,0,0,0,2,48,0,6,0,0,0,104,101,108,
             108,111,0,8,49,0,0,0,2,98,97,122,0,4,0,0,0,
             113,117,120,0,0];
            decode(stream);
         }
    }

}
