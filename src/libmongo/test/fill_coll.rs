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
use mongo::client::*;
use mongo::coll::*;

use bson::formattable::*;
use bson::encode::*;

/**
 * Helper fn for tests; fills a collection with a given number of docs.
 */
pub fn fill_coll<'a>(db : ~str, coll : ~str, client : &'a Client, n : uint)
            -> (Collection<'a>, ~[~str], ~[BsonDocument]) {
    let coll = Collection::new(db, coll, client);

    // clear out collection to start from scratch
    coll.remove(None, None, None, None);

    // create and insert batch
    let mut ins_strs = ~[];
    let mut ins_docs = ~[];
    let mut i = 0;
    for n.times {
        let ins_str = fmt!("{
                                \"_id\":%d,
                                \"a\":%d,
                                \"b\":\"ins %d\",
                                \"loc\":{ \"x\":%d, \"y\":%d },
                                \"insert no\":%d
                            }", i, i/2, i, -i, i+4, i);
        let ins_doc = match (copy ins_str).to_bson_t() {
                Embedded(bson) => *bson,
                _ => fail!("what happened"),
            };
        ins_strs.push(ins_str);
        ins_docs.push(ins_doc);
        i += 1;
    }
    coll.insert_batch(ins_strs.clone(), None, None, None);

    (coll, ins_strs, ins_docs)
}
