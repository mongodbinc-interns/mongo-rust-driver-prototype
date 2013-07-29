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
use mongo::db::*;
use mongo::util::*;
use mongo::shard::*;

use fill_coll::*;

#[test]
fn test_sharding() {
    // get collections
    let m = @Client::new();
    match m.connect(~"127.0.0.1", 57017) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    let mongos = ShardController::new(m);

    let mongod = @Client::new();
    match mongod.connect(~"127.0.0.1", 27017) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    let db_str = ~"rust_shards";
    let n = 15;
    let colls = [~"coll0", ~"coll1", ~"coll2"];
    for colls.iter().advance |&name| {
        fill_coll(db_str.clone(), name, mongod, n);
    }

    match mongos.add_shard(~"127.0.0.1:27017") {
        Ok(_) => (),
        Err(e) => debug!("%s", e.to_str())
    }
    match mongos.add_shard(~"127.0.0.1:37017") {
        Ok(_) => (),
        Err(e) => debug!("%s", e.to_str())
    }

    match mongos.enable_sharding(db_str) {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str())
    }

    info!("pre-enabling status");
    match mongos.status() {
        Ok(s) => info!(fmt!("Sharding status: %s", s)),
        Err(e) => fail!("%s", e.to_str())
    }

    match mongos.add_shard(~"localhost:27017") {
        Ok(_) => (),
        Err(e) => debug!("%s", e.to_str())
    }

    match mongos.add_shard(~"localhost:37017") {
        Ok(_) => (),
        Err(e) => debug!("%s", e.to_str())
    }

    info!("post-enabling status");
    match mongos.status() {
        Ok(s) => info!(fmt!("Sharding status: %s", s)),
        Err(e) => fail!("%s", e.to_str())
    }

    match mongos.mongos.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }

    match mongod.disconnect() {
        Ok(_) => (),
        Err(e) => fail!("%s", e.to_str()),
    }
}
