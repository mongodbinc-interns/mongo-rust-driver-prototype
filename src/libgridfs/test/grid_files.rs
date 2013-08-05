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

use gridfs::grid::*;
use gridfs::gridfile::*;

use mongo::db::*;
use mongo::client::*;
use mongo::util::*;

#[test]
fn grid_files() {
    let client = @Client::new();
    match client.connect(~"127.0.0.1", MONGO_DEFAULT_PORT) {
        Ok(_) => (),
        Err(e) => fail!(e.to_str())
    }

    let db = DB::new(~"rust_gridfs", client);
    let mut grid = GridFS::new(@db);

    let data = ~[0u8,1,2,3,4,5,6,7,8,9];

    //do it manually; allow condition to escape
    let mut file = grid.file_write();
    file.chunk_size = 2; //make tiny chunks
    file.write(data.clone());
    file.close();

    //use regular chunk size
    match grid.put(data.clone()) {
        Ok(_) => (),
        Err(e) => fail!(e.to_str())
    }
    //TODO this test needs to clean up after itself

    let id = grid.last_id.clone().unwrap();
    let mut ofile = grid.file_read(id);
    let mut buf = ~[0u8,0,0,0,0,0,0,0,0,0]; //10
    ofile.read(buf);
    assert_eq!(buf,
        ~[0u8,1,2,3,4,5,6,7,8,9]);
    assert!(ofile.eof());
}
