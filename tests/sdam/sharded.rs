use super::framework::run_suite;

use std::fs;
use std::path::Path;

#[test]
fn sdam_sharded() {
    let path = "tests/json/data/specs/source/server-discovery-and-monitoring/tests/sharded/";
    let paths = fs::read_dir(&Path::new(path)).unwrap();

    for path in paths {
        let path2 = path.unwrap().path();
        let filename = path2.to_string_lossy();
        if filename.ends_with(".json") {
            run_suite(&filename, None)
        }
    }
}
