use mongodb::{Client, ThreadedClient};
use mongodb::common::ReadMode;
use mongodb::connstring::ConnectionString;
use mongodb::stream::StreamConnector;
use mongodb::topology::{TopologyDescription, TopologyType};
use mongodb::topology::server::Server;

use json::server_selection::reader::SuiteContainer;
use serde_json::Value;
use std::sync::{Arc, RwLock};

pub fn run_suite(file: &str) {
    let json = Value::from_file(file).unwrap();
    let suite = json.get_suite().unwrap();

    let dummy_config = ConnectionString::new("i-dont-exist", 27017);
    let dummy_client = Client::with_config(dummy_config, None, None).unwrap();
    let dummy_top_arc = Arc::new(RwLock::new(
        TopologyDescription::new(StreamConnector::default()),
    ));

    let mut topology_description = TopologyDescription::new(StreamConnector::default());
    topology_description.topology_type = suite.topology_description.ttype;

    for suite_server in suite.topology_description.servers {
        let server = Server::new(
            dummy_client.clone(),
            suite_server.host.clone(),
            dummy_top_arc.clone(),
            false,
            StreamConnector::default(),
            None,
            None,
        );

        {
            let mut description = server.description.write().unwrap();
            description.round_trip_time = Some(suite_server.rtt);
            description.tags = suite_server.tags;
            description.server_type = suite_server.stype;
        }

        topology_description.servers.insert(
            suite_server.host,
            server,
        );
    }

    let (mut suitable_hosts, _) = if suite.write {
        topology_description.choose_write_hosts()
    } else {
        topology_description
            .choose_hosts(&suite.read_preference)
            .unwrap()
    };

    if suite.topology_description.ttype != TopologyType::Sharded &&
        suite.topology_description.ttype != TopologyType::Single
    {
        topology_description.filter_hosts(&mut suitable_hosts, &suite.read_preference);
    }

    if suitable_hosts.is_empty() && !suite.write &&
        suite.read_preference.mode == ReadMode::SecondaryPreferred
    {
        let mut read_pref = suite.read_preference.clone();
        read_pref.mode = ReadMode::PrimaryPreferred;
        let (mut hosts, _) = topology_description.choose_hosts(&read_pref).unwrap();
        if suite.topology_description.ttype != TopologyType::Sharded &&
            suite.topology_description.ttype != TopologyType::Single
        {
            topology_description.filter_hosts(&mut hosts, &read_pref);
        }
        suitable_hosts.extend(hosts);
    }

    assert_eq!(suite.suitable_servers.len(), suitable_hosts.len());
    for server in &suite.suitable_servers {
        assert!(suitable_hosts.contains(&server.host));
    }

    topology_description.filter_latency_hosts(&mut suitable_hosts);
    assert_eq!(suite.in_latency_window.len(), suitable_hosts.len());
    for server in &suite.in_latency_window {
        assert!(suitable_hosts.contains(&server.host));
    }
}
