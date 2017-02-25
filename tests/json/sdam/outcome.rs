use mongodb::connstring::{self, Host};
use mongodb::topology::TopologyType;
use mongodb::topology::server::ServerType;

use serde_json::{Map, Value};
use std::collections::HashMap;
use std::str::FromStr;

pub struct Server {
    pub set_name: String,
    pub stype: ServerType,
}

pub struct Outcome {
    pub servers: HashMap<Host, Server>,
    pub set_name: String,
    pub ttype: TopologyType,
}

impl Outcome {
    pub fn from_json(object: &Map<String, Value>) -> Result<Outcome, String> {
        let mut servers = HashMap::new();

        if let Some(&Value::Object(ref obj)) = object.get("servers") {
            for (host, json) in obj {
                let doc = val_or_err!(*json,
                                      Value::Object(ref obj) => obj,
                                      "`servers` must be an object map.");

                let server_set_name = match doc.get("setName") {
                    Some(&Value::String(ref s)) => s.to_owned(),
                    _ => String::new(),
                };

                let server_type = val_or_err!(doc.get("type"),
                                              Some(&Value::String(ref s)) =>
                                              ServerType::from_str(s).unwrap(),
                                              "`type` must be a string.");

                let server_obj = Server {
                    set_name: server_set_name,
                    stype: server_type,
                };
                servers.insert(connstring::parse_host(host).unwrap(), server_obj);
            }
        }

        let set_name = match object.get("setName") {
            Some(&Value::String(ref s)) => s.to_owned(),
            _ => String::new(),
        };

        let ttype = match object.get("topologyType") {
            Some(&Value::String(ref s)) => TopologyType::from_str(s).unwrap(),
            _ => TopologyType::Unknown,
        };

        Ok(Outcome {
            servers: servers,
            set_name: set_name,
            ttype: ttype,
        })
    }
}
