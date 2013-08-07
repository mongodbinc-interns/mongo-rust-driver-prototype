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

use std::char::is_digit;
use std::str::from_chars;
use std::float::from_str;
use tools::stream::*;
use encode::*;

///This trait is for parsing non-BSON object notations such as JSON, XML, etc.
pub trait ObjParser<V> {
    pub fn from_string(s: &str) -> Result<V,~str>;
}

///JSON parsing struct. T is a Stream<char>.
pub struct ExtendedJsonParser<T> {
    stream: T
}

/// Intermediate type returned by many parsing methods
type DocResult = Result<Document, ~str>;

///Publicly exposes from_string.
impl ObjParser<Document> for ExtendedJsonParser<~[char]> {
    pub fn from_string(s: &str) -> DocResult {
        let mut stream = s.iter().collect::<~[char]>();
        stream.pass_while(&[' ', '\n', '\r', '\t']);
        if !(stream.first() == &'{') {
            return Err(~"invalid json string found!");
        }
        let mut parser = ExtendedJsonParser::new(stream);
        parser.object()
    }
}

macro_rules! match_insert {
    ($cb:ident, $key:expr) => {
        match self.$cb() {
            Ok(bl) => { ret.put($key, bl); }
            Err(e) => return Err(e)
        }
    }
}

///Main parser implementation for JSON
impl<T:Stream<char>> ExtendedJsonParser<T> {

    ///Parse an object. Returns an error string on parse failure
    pub fn object(&mut self) -> DocResult {
        self.stream.pass(1); //pass over brace
        let mut ret = BsonDocument::new();
        while !(self.stream.first() == &'}') {
            self.stream.pass_while(&[' ', '\n', '\r', '\t']);
            if self.stream.expect(&['\"', '\'']).is_none() { return Err(~"keys must begin with quote marks"); }
            let keych = *self.stream.first();
            let key = match self._string(keych) {
                UString(s) => s,
                _ => return Err(~"invalid key found")
            };
            self.stream.pass_while(&[' ', '\n', '\r', '\t']);
            if self.stream.expect(&[':']).is_none() {
                return Err(~"keys and values should be separated by :");
            }
            self.stream.pass(1); //pass over :
            self.stream.pass_while(&[' ', '\n', '\r', '\t']);
            let c = self.stream.expect(&['\"', '\'', 't', 'f', '[', '{']);
            match c {
                Some('\"') => { ret.put(key, self._string('\"')); }
                Some('\'') => { ret.put(key, self._string('\'')); }
                Some('t') => {
                    match_insert!(_bool,key);
                }
                Some('f') => {
                    match_insert!(_bool,key);
                }
                Some('[') => {
                    match_insert!(_list,key)
                }
                Some('{') => {
                    let o = self.object();
                    if o.is_err() { return o; }
                    let obj = o.unwrap();
                    let id = ExtendedJsonParser::_keyobj::<T>(&obj);
                    if !id.is_none() { ret.put(key, id.unwrap()); }
                    else { ret.put(key, obj); }
                }
                _ => if is_digit(*self.stream.first()) { ret.put(key, self._number()); }
                     else if (*self.stream.first()) == '-' {
                        self.stream.pass(1);
                        match self._number() {
                           Double(f) => ret.put(key, Double(-1f64 * f)),
                           _ => return Err(~"error while expecting a negative value")
                        }
                     }
                     else { return Err(fmt!("invalid value found: %?", self.stream.first())); }
            }
            self.stream.pass_while(&[' ', '\n', '\r', '\t']);
            let comma = self.stream.expect(&[',', '}']);
            match comma {
                Some(',') => {
                    self.stream.pass(1);
                    self.stream.pass_while(&[' ', '\n', '\r', '\t'])
                }
                Some('}') => {
                    self.stream.pass(1);
                    self.stream.pass_while(&[' ', '\n', '\r', '\t']);
                    return Ok(Embedded(~ret));
                }
                _ => return Err(fmt!("invalid end to object: expecting , or }, found %?", self.stream.first()))
            }
            if !self.stream.has_next() { break; }
        }
        self.stream.pass_while(&[' ', '\n', '\r', '\t']);
        Ok(Embedded(~ret))
    }

    ///Parse a string.
    fn _string(&mut self, delim: char) -> Document {
        self.stream.pass(1); //pass over begin quote
        let ret: ~[char] = self.stream.until(|c| *c == delim);
        self.stream.pass(1); //pass over end quote
        self.stream.pass_while(&[' ', '\n', '\r', '\t']); //pass over trailing whitespace
        UString(from_chars(ret))
    }

    ///Parse a number; converts it to float.
    fn _number(&mut self) -> Document {
        let ret = self.stream.until(|c| (*c == ',') ||
            [' ', '\n', '\r', '\t', ']', '}'].contains(c));
        Double(from_str(from_chars(ret)).unwrap() as f64)
    }

    ///Parse a boolean. Errors for values other than 'true' or 'false'.
    fn _bool(&mut self) -> DocResult {
        let c1 = self.stream.expect(&['t', 'f']);
        match c1 {
            Some('t') => { self.stream.pass(1);
                    let next = ~['r', 'u', 'e'];
                    let mut i = 0;
                    while i < 3 {
                        let c = self.stream.expect(&[next[i]]);
                        if c.is_none() {
                            return Err(~"invalid boolean value while expecting true!");
                        }
                        i += 1;
                        self.stream.pass(1);
                    }
                    self.stream.pass_while(&[' ', '\n', '\r', '\t']);
                    Ok(Bool(true))
                     }
            Some('f') => { self.stream.pass(1);
                    let next = ~['a', 'l', 's', 'e'];
                    let mut i = 0;
                    while i < 4 {
                        let c = self.stream.expect(&[next[i]]);
                        if c.is_none() {
                            return Err(~"invalid boolean value while expecting false!");
                        }
                        i += 1;
                        self.stream.pass(1);
                    }
                    self.stream.pass_while(&[' ', '\n', '\r', '\t']);
                    Ok(Bool(false))
                     }
            _ => return Err(~"invalid boolean value!")
        }
    }

    ///Parse null. Errors for values other than 'null'.
    fn _null(&mut self) -> DocResult {
        let c1 = self.stream.expect(&['n']);
        match c1 {
            Some('n') => { self.stream.pass(1);
                let next = ~['u', 'l', 'l'];
                let mut i = 0;
                while i < 3 {
                    let c = self.stream.expect(&[next[i]]);
                    if c.is_none() { return Err(~"invalid null value!"); }
                    i += 1;
                    self.stream.pass(1);
                }
                self.stream.pass_while(&[' ', '\n', '\r', '\t']);
                Ok(Null)
            }
            _ => return Err(~"invalid null value!")
        }
    }

    ///Parse a list.
    fn _list(&mut self) -> DocResult {
        self.stream.pass(1); //pass over [
        self.stream.pass_while(&[' ', '\n', '\r', '\t']);
        let mut ret = BsonDocument::new();
        let mut i: uint = 0;
        while !(self.stream.first() == &']') {
            let c = self.stream.expect(&['\"', '\'', 't', 'f', '[', '{']);
            match c {
                Some('\"') => ret.put(i.to_str(), self._string('\"')),
                Some('\'') => ret.put(i.to_str(), self._string('\'')),
                Some('t') => {
                    match_insert!(_bool,i.to_str());
                }
                Some('f') => {
                    match_insert!(_bool,i.to_str());
                }
                Some('[') => {
                    match_insert!(_list,i.to_str());
                }
                Some('{') => {
                    let o = self.object();
                    if o.is_err() { return o; }
                    let obj = o.unwrap();
                    let id = ExtendedJsonParser::_keyobj::<T>(&obj);
                    if !id.is_none() { ret.put(i.to_str(), id.unwrap()); }
                    else { ret.put(i.to_str(), obj); }
                }
                _ => if is_digit(*self.stream.first()) {
                        ret.put(i.to_str(), self._number())
                     }
                     else {
                         return Err(fmt!("invalid value found: %?", self.stream.first()));
                     }
            }
            i += 1;
            self.stream.pass_while(&[' ', '\n', '\r', '\t']);
            let comma = self.stream.expect(&[',', ']']);
            match comma {
                Some(',') => {
                    self.stream.pass(1);
                    self.stream.pass_while(&[' ', '\n', '\r', '\t']);
                }
                Some(']') => {
                    self.stream.pass(1);
                    self.stream.pass_while(&[' ', '\n', '\r', '\t']);
                    return Ok(Array(~ret));
                }
                _ => return Err(fmt!("invalid value found: %?", self.stream.first()))
            }
            if !self.stream.has_next() { break; } //this should only happen during tests
        }
        self.stream.pass_while(&[' ', '\n', '\r', '\t']);
        Ok(Array(~ret))
    }

    ///If this object was an $oid, return an ObjID.
    fn _keyobj(json: &Document) -> Option<Document> {
        match *json {
            Embedded(ref m) => {
                if m.fields.len() == 1 && m.contains_key(~"$oid") { //objectid
                    match (m.find(~"$oid")) {
                        Some(&UString(ref st)) => return Some(
                            ObjectId(st.bytes_iter().collect::<~[u8]>())
                        ),
                        _ => return None //fail more silently here
                    }
                }
                else if m.fields.len() == 1 && m.contains_key(~"$date") { //utcdate
                    match (m.find(~"$date")) {
                        Some(&Double(f)) => return Some(UTCDate(f as i64)),
                        _ => return None
                    }
                }
                else if m.fields.len() == 1 && m.contains_key(~"$minKey") { //minkey
                    match (m.find(~"$minKey")) {
                        Some(&Double(1f64)) => return Some(MinKey),
                        _ => return None
                    }
                }
                else if m.fields.len() == 1 && m.contains_key(~"$maxKey") { //maxkey
                    match (m.find(~"$maxKey")) {
                        Some(&Double(1f64)) => return Some(MaxKey),
                        _ => return None
                    }
                }
                else if m.fields.len() == 1 && m.contains_key(~"$timestamp") { //timestamp
                    match (m.find(~"$timestamp")) {
                        Some(&Embedded(ref doc)) => if doc.fields.len() == 2
                            && doc.contains_key(~"t")
                            && doc.contains_key(~"i") {
                                match (doc.find(~"t"), doc.find(~"i")) {
                                    (Some(&Double(a)), Some(&Double(b))) => {
                                        return Some(Timestamp(a as u32, b as u32)); //TODO is this right??
                                    }
                                    _ => return None
                                }
                            },
                        _ => return None
                    }
                }
                else if m.fields.len() == 2 //binary data
                    && m.contains_key(~"$binary")
                    && m.contains_key(~"$type") {
                    match (m.find(~"$binary"), m.find(~"$type")) {
                        (Some(&UString(ref s1)), Some(&UString(ref s2))) =>
                            return Some(Binary(s2.bytes_iter().collect::<~[u8]>()[0],
                                s1.bytes_iter().collect::<~[u8]>())),
                        _ => return None
                    }
                }
                else if m.fields.len() == 2 //regex
                    && m.contains_key(~"$regex")
                    && m.contains_key(~"$options") {
                    match (m.find(~"$regex"), m.find(~"$options")) {
                        (Some(&UString(ref s1)), Some(&UString(ref s2))) =>
                            return Some(Regex(s1.clone(), s2.clone())),
                        _ => return None
                    }
                }
                else if m.fields.len() == 2 //dbref
                    && m.contains_key(~"$ref")
                    && m.contains_key(~"$id") {
                    match (m.find(~"$ref"), m.find(~"$id")) {
                        (Some(&UString(ref s)), Some(&ObjectId(ref d))) =>
                            return Some(DBRef(s.clone(), ~ObjectId(d.clone()))),
                        _ => return None
                    }
                }
            }
            _ => return None
        }
        None
    }

    ///Return a new JSON parser with a given stream.
    pub fn new(stream: T) -> ExtendedJsonParser<T> { ExtendedJsonParser {stream: stream} }
}

#[cfg(test)]
mod tests {

    use super::*;
    use encode::*;
    use extra::test::BenchHarness;

    #[test]
    fn test_string_fmt() {
        let stream = "\"hello\"".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream);
        let val = parser._string('\"');
        assert_eq!(UString(~"hello"), val);
    }

    #[test]
    fn test_number_fmt() {
        let stream = "2".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream);
        let val = parser._number();
        assert_eq!(Double(2f64), val);
    }

    #[test]
    fn test_bool_fmt() {
        let stream_true = "true".iter().collect::<~[char]>();
        let stream_false = "false".iter().collect::<~[char]>();
        let mut parse_true = ExtendedJsonParser::new(stream_true);
        let mut parse_false = ExtendedJsonParser::new(stream_false);
        let val_t = parse_true._bool().unwrap();
        let val_f = parse_false._bool().unwrap();

        assert_eq!(Bool(true), val_t);
        assert_eq!(Bool(false), val_f);
    }

    #[test]
    #[should_fail]
    fn test_invalid_true_fmt() {
        let stream = "tasdf".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream);
        if parser._bool().is_err() { fail!("invalid_true_fmt") }
    }

    #[test]
    #[should_fail]
    fn test_invalid_false_fmt() {
        let stream = "fasdf".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream);
        if parser._bool().is_err() { fail!("invalid_false_fmt") }
    }

    #[test]
    #[should_fail]
    fn test_invalid_bool_fmt() {
        let stream = "asdf".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream);
        if parser._bool().is_err() { fail!("invalid_bool_fmt") }
    }
    #[test]
    fn test_list_fmt() {
        let stream = "[5.01, true, \'hello\']".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream);
        let val = parser._list().unwrap();
        let mut l = BsonDocument::new();
        l.put(~"0", Double(5.01f64));
        l.put(~"1", Bool(true));
        l.put(~"2", UString(~"hello"));
        assert_eq!(Array(~l), val);
    }

    #[test]
    fn test_object_fmt() {
        let stream = "{\"foo\": true, \'bar\': 2, 'baz': [\"qux-dux\"]}".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream);
        let mut d = BsonDocument::new();
        let mut doc = BsonDocument::new();
        doc.put(~"0", UString(~"qux-dux"));
        d.put(~"foo", Bool(true));
        d.put(~"bar", Double(2f64));
        d.put(~"baz", Array(~doc));

        assert_eq!(Embedded(~d), parser.object().unwrap());
    }

    #[test]
    fn test_nested_obj_fmt() {
        let stream = "{\"qux\": {\"foo\": 5.0, 'bar': 'baz'}, \"fizzbuzz\": false}".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream);
        let mut m1 = BsonDocument::new();
        m1.put(~"foo", Double(5f64));
        m1.put(~"bar", UString(~"baz"));

        let mut m2 = BsonDocument::new();
        m2.put(~"qux", Embedded(~m1));
        m2.put(~"fizzbuzz", Bool(false));

        let v = parser.object();
        match v {
            Ok(_) => { }
            Err(e) => { fail!(fmt!("Object with internal object failed: %s", e)) }
        }

        assert_eq!(Embedded(~m2), v.unwrap());
    }

    #[test]
    fn test_nested_list_fmt() {
        //list with object inside
        let stream1 = "[ 5.0, {\"foo\": true}, 'bar' ]".iter().collect::<~[char]>();
        let mut parser1 = ExtendedJsonParser::new(stream1);
        let mut m = BsonDocument::new();
        m.put(~"foo", Bool(true));
        let v1 = parser1._list();
        match v1 {
            Ok(_) => { }
            Err(e) => { fail!(fmt!("List with internal object failed: %s", e)) }
        }

        let mut l = BsonDocument::new();
        l.put(~"0", Double(5f64));
        l.put(~"1", Embedded(~m));
        l.put(~"2", UString(~"bar"));
        assert_eq!(Array(~l), v1.unwrap());

        //list with list inside
        let stream2 = "[5.0, [ true, false ], \"foo\"]".iter().collect::<~[char]>();
        let mut parser2 = ExtendedJsonParser::new(stream2);
        let v2 = parser2._list();
        let mut l1 = BsonDocument::new();
        l1.put(~"0", Bool(true));
        l1.put(~"1", Bool(false));
        let mut l2 = BsonDocument::new();
        l2.put(~"0", Double(5f64));
        l2.put(~"1", Array(~l1));
        l2.put(~"2", UString(~"foo"));
        match v2 {
            Ok(_) => { }
            Err(e) => { fail!(fmt!("List with internal list failed: %s", e)) }
        }

        assert_eq!(Array(~l2), v2.unwrap());
    }

    #[test]
    fn test_null_fmt() {
        let stream = "null".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream);
        assert_eq!(Null, parser._null().unwrap());

    }

    #[test]
    #[should_fail]
    fn test_invalid_null_fmt() {
        let stream = "nulf".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream);
        if parser._null().is_err() { fail!("invalid null value") }
    }

    #[test]
    fn test_objid_fmt() {
        let stream = "{\"$oid\": \"abcdefg\"}".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream);
        let v = parser.object();
        let val = ExtendedJsonParser::_keyobj::<~[char]>(&(v.unwrap())).unwrap();

        assert_eq!(ObjectId("abcdefg".bytes_iter().collect::<~[u8]>()), val);
    }

    #[test]
    fn test_date_fmt() {
        let stream = "{\'$date\': 12345}".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream);
        let v = parser.object();
        let val = ExtendedJsonParser::_keyobj::<~[char]>(&(v.unwrap())).unwrap();

        assert_eq!(UTCDate(12345i64), val);
    }

    #[test]
    fn test_minkey_1_fmt() {
        let stream = "{\"$minKey\": 1}".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream);
        let v = parser.object();
        let val = ExtendedJsonParser::_keyobj::<~[char]>(&(v.unwrap())).unwrap();

        assert_eq!(MinKey, val);
    }

    #[test]
    #[should_fail]
    fn test_minkey_otherkey_fmt() {
        let stream = "{\'$minKey\': 3}".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream);
        let v = parser.object();
        ExtendedJsonParser::_keyobj::<~[char]>(&(v.unwrap())).unwrap();
    }

    #[test]
    fn test_regex_fmt() {
        let stream = "{\"$regex\": \"foo\", \"$options\": \"bar\"}".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream);
        let v = parser.object();
        let val = ExtendedJsonParser::_keyobj::<~[char]>(&(v.unwrap())).unwrap();

        assert_eq!(Regex(~"foo",~"bar"), val);
    }

    #[test]
    #[should_fail]
    fn test_mismatched_quotes() {
        let stream = "{\"foo': 'bar\"}".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream);
        if parser.object().is_err() { fail!("test_mismatched_quotes") }
    }

    #[bench]
    fn bench_string_parse(b: &mut BenchHarness) {
        let stream = "'asdfasdf'".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream.clone());
        do b.iter {
            parser._string('\'');
            parser = ExtendedJsonParser::new(stream.clone());
        }
    }

    #[bench]
    fn bench_num_parse(b: &mut BenchHarness) {
        let stream = "25252525".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream.clone());
        do b.iter {
            parser._number();
            parser = ExtendedJsonParser::new(stream.clone());
        }
    }

    #[bench]
    fn bench_bool_parse(b: &mut BenchHarness) {
        let stream_true = "true".iter().collect::<~[char]>();
        let stream_false = "false".iter().collect::<~[char]>();
        let mut parse_true = ExtendedJsonParser::new(stream_true.clone());
        let mut parse_false = ExtendedJsonParser::new(stream_false.clone());
        do b.iter {
            parse_true._bool();
            parse_true = ExtendedJsonParser::new(stream_true.clone());
            parse_false._bool();
            parse_true = ExtendedJsonParser::new(stream_false.clone());
        }
    }

    #[bench]
    fn bench_list_parse(b: &mut BenchHarness) {
        let stream = "[5.01, true, \'hello\']".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream.clone());
        do b.iter {
            parser._list();
            parser = ExtendedJsonParser::new(stream.clone());
        }
    }

    #[bench]
    fn bench_basic_obj_parse(b: &mut BenchHarness) {
        let stream = "{'foo': true, 'bar': 2}".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream.clone());
        do b.iter {
            parser.object();
            parser = ExtendedJsonParser::new(stream.clone());
        }
    }

    #[bench]
    fn bench_object_parse(b: &mut BenchHarness) {
        let stream = "{\"foo\": true, \'bar\': 2, 'baz': [\"qux-dux\"]}".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream.clone());
        do b.iter {
            parser.object();
            parser = ExtendedJsonParser::new(stream.clone());
        }
    }

    #[bench]
    fn bench_nested_obj_parse(b: &mut BenchHarness) {
        let stream = "{\"qux\": {\"foo\": 5.0, 'bar': 'baz'}, \"fizzbuzz\": false}".iter().collect::<~[char]>();
        let mut parser = ExtendedJsonParser::new(stream.clone());
        do b.iter {
            parser.object();
            parser = ExtendedJsonParser::new(stream.clone());
        }
    }

    #[bench]
    fn bench_advanced_object(b: &mut BenchHarness) {
        let stream = "{
            'fullName' : 'John Doe',
            'age' : 42,
            'state' : 'Massachusetts',
            'city' : 'Boston',
            'zip' : 02201,
            'married' : false,
            'dozen' : 12,
            'topThreeFavoriteColors' : [ 'red', 'magenta', 'cyan' ],
            'favoriteSingleDigitWholeNumbers' : [ 7 ],
            'favoriteFiveLetterWord' : 'fadsy',
            'strings' :
            [
            'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ',
            '01234567890',
            'mixed-1234-in-{+^}',
            '\"quoted\"',
            '\"\\e\\s\\c\\a\\p\\e\\d\"',
            '\"quoted-at-sign@sld.org\"',
            '\"escaped\\\"quote\"',
            '\"back\\slash\"',
            'email@address.com'
            ],
            'ipAddresses' : [ '127.0.0.1', '24.48.64.2', '192.168.1.1', '209.68.44.3', '2.2.2.2' ]
        }".iter().collect::<~[char]>();

        let mut parser = ExtendedJsonParser::new(stream.clone());
        do b.iter {
            match parser.object() {
                Ok(_) => (),
                Err(e) => fail!(e.to_str())
            }
            parser = ExtendedJsonParser::new(stream.clone());
        }
    }

    #[bench]
    fn bench_extended_object(b: &mut BenchHarness) {
        let stream = "{
            'name': 'foo',
            'baz': 'qux',
            'binary': { '$binary': 012345432, '$type': 0 },
            'dates': [ { '$date': 987654 }, {'$date': 123456}, {'$date': 748392} ],
            'timestamp': { 'timestamp': { 'timestamp': { '$timestamp': { 't': 1234, 'i': 5678 } } } },
            'regex': { '$regex': '^.*/012345/.*(foo|bar)+.*$', '$options': '-j -g -i' },
            'oid': { '$oid': 43214321 },
            'minkey': { 'maxkey': { 'that-was-a-fakeout': { '$minKey': 1 } } },
            'maxkey': { 'minkey': { 'haha-that-too': { '$maxKey': 1 } } }
        }".iter().collect::<~[char]>();

        let mut parser = ExtendedJsonParser::new(stream.clone());
        do b.iter {
            match parser.object() {
                Ok(_) => (),
                Err(e) => fail!(e.to_str())
            }
            parser = ExtendedJsonParser::new(stream.clone());
        }
    }
}
