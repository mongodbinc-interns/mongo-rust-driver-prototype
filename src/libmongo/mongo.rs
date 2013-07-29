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

#[link(name="mongo", vers="0.1.0", author="jaoke.chinlee@10gen.com, austin.estep@10gen.com")];
#[comment="a huMONGOus crate"];
#[license="Apache 2.0"];
#[crate_type="lib"];

// TODO fix visibility issues

//#[no_core]

extern mod std;
extern mod extra;
extern mod bson;
extern mod tools;

// Misc: utility module
#[macro_escape]
pub mod util;
#[macro_escape]
mod mockable;

// Client-side components
pub mod client;     // primary point-of-entry for driver system
pub mod db;         // database-related functionality
pub mod coll;       // collection-related functionality

// Connection components
mod conn;       // medium for connecting to [a] server(s); general, should be hidden from user
pub mod conn_node;    // ...; node
mod msg;        // message header and related; should be hidden from user
//pub mod conn_replica; // ...; replica set
pub mod shard;   // ...; sharded cluster

// Cursor components
pub mod cursor;     // mode of interaction with results
//pub mod index;      // index for querying?
