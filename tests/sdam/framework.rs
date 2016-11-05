use mongodb::{Client, ThreadedClient};
use mongodb::Error::OperationError;
use mongodb::connstring::{self, ConnectionString};
use mongodb::topology::{Topology, TopologyDescription, TopologyType};
use mongodb::topology::monitor::IsMasterResult;
use mongodb::topology::server::Server;

use json::sdam::reader::SuiteContainer;
use rustc_serialize::json::Json;

use std::collections::HashMap;

pub fn run_suite(file: &str, description: Option<TopologyDescription>) {
    let json = Json::from_file(file).unwrap();
    let suite = json.get_suite().unwrap();

    let dummy_config = ConnectionString::new("i-dont-exist", 27017);
    let dummy_client = Client::with_config(dummy_config, None, None).unwrap();
    let connection_string = connstring::parse(&suite.uri).unwrap();

    // For a standalone topology with multiple startup servers, the user
    // should pass in an unknown topology. For a base standalone topology,
    // the user should note that they expect a standalone by providing TopologyType::Single.
    let should_ignore_description = if let Some(ref inner) = description {
        inner.topology_type == TopologyType::Single && connection_string.hosts.len() != 1
    } else {
        false
    };

    let topology = if should_ignore_description {
        Topology::new(connection_string.clone(), None).unwrap()
    } else {
        Topology::new(connection_string.clone(), description).unwrap()
    };

    let top_description_arc = topology.description.clone();

    let mut servers = HashMap::new();

    // Fill servers array
    for host in &connection_string.hosts {
        let mut topology_description = topology.description.write().unwrap();
        let server = Server::new(dummy_client.clone(),
                                 host.clone(),
                                 top_description_arc.clone(),
                                 false);
        topology_description.servers.insert(host.clone(), server);
    }

    for phase in suite.phases {
        for (host, response) in phase.operation.data {
            {
                // Save each seen server to replicate monitors for servers
                // that have been removed from the topology.
                let topology_description = topology.description.read().unwrap();
                for (host, server) in &topology_description.servers {
                    servers.insert(host.clone(), server.clone());
                }
            }

            let mut topology_description = topology.description.write().unwrap();

            if response.is_empty() {
                let server = servers.get(&host).expect("Host not found.");
                let mut server_description = server.description.write().unwrap();
                server_description.set_err(OperationError("Simulated network error.".to_owned()));
            } else {
                match IsMasterResult::new(response) {
                    Ok(ismaster) => {
                        let server = servers.get(&host).expect("Host not found.");
                        let mut server_description = server.description.write().unwrap();
                        server_description.update(ismaster, 0)
                    }
                    Err(err) => panic!(err),
                }
            }

            let server_description = {
                let server = servers.get(&host).expect("Host not found.");
                server.description.read().unwrap().clone()
            };

            topology_description.update_without_monitor(host.clone(),
                                                        server_description.clone(),
                                                        dummy_client.clone(),
                                                        top_description_arc.clone());
        }

        // Check server and topology descriptions.
        let topology_description = topology.description.read().unwrap();

        assert_eq!(phase.outcome.servers.len(), topology_description.servers.len());
        for (host, server) in &phase.outcome.servers {
            match topology_description.servers.get(host) {
                Some(top_server) => {
                    let top_server_description = top_server.description.read().unwrap();
                    assert_eq!(server.set_name, top_server_description.set_name);
                    assert_eq!(server.stype, top_server_description.server_type);
                }
                None => panic!("Missing host in outcome."),
            }
        }

        assert_eq!(phase.outcome.set_name, topology_description.set_name);
        assert_eq!(phase.outcome.ttype, topology_description.topology_type);
    }
}
