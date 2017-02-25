use mongodb::connstring::{self, Host};
use mongodb::topology::server::ServerType;

use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::str::FromStr;

#[derive(PartialEq, Eq)]
pub struct Server {
    pub host: Host,
    pub rtt: i64,
    pub tags: BTreeMap<String, String>,
    pub stype: ServerType,
}

impl Server {
    pub fn from_json(object: &Map<String, Value>) -> Result<Server, String> {
        let address = val_or_err!(object.get("address"),
                                  Some(&Value::String(ref s)) => s.to_owned(),
                                  "server must have an address.");

        let rtt = val_or_err!(object.get("avg_rtt_ms"),
                              Some(&Value::Number(ref v)) => v.as_i64()
                              .expect("server must have a numerical avg_rtt_ms"),
                              "server must have an average rtt.");

        let mut tags = BTreeMap::new();
        let json_doc = val_or_err!(object.get("tags"),
                                   Some(&Value::Object(ref obj)) => obj.clone(),
                                   "server must have tags.");

        for (key, json) in json_doc {
            match json {
                Value::String(val) => {
                    tags.insert(key, val);
                }
                _ => return Err(String::from("server must have tags that are string => string maps.")),
            }
        }

        let stype = val_or_err!(object.get("type"),
                                Some(&Value::String(ref s)) => ServerType::from_str(s)
                                .expect("Failed to parse server type"),
                                "server must have a type.");

        Ok(Server {
            host: connstring::parse_host(&address).expect("Failed to parse host."),
            rtt: rtt,
            tags: tags,
            stype: stype,
        })
    }
}
