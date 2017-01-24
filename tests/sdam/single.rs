use mongodb::stream::StreamConnector;
use mongodb::topology::{TopologyDescription, TopologyType};

use std::fs;
use std::path::Path;

use super::framework::run_suite;

#[test]
fn sdam_single() {
    let dir = "tests/json/data/specs/source/server-discovery-and-monitoring/tests/single/";
    let paths = fs::read_dir(&Path::new(dir)).unwrap();

    for path in paths {
        let path2 = path.unwrap().path();
        let filename = path2.to_string_lossy();
        if filename.ends_with(".json") {
            let mut description = TopologyDescription::new(StreamConnector::default());
            description.topology_type = TopologyType::Single;
            run_suite(&filename, Some(description))
        }
    }
}
