//! MongoDB server representation.
use {Client, Result};
use Error::{self, OperationError};

use bson::oid;
use connstring::Host;
use pool::{ConnectionPool, PooledStream, DEFAULT_TIMEOUT_ON_IDLE};
use stream::StreamConnector;

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::sync::atomic::Ordering;
use std::thread;

use super::monitor::{IsMasterResult, Monitor};
use super::TopologyDescription;
use std::time::Duration;

/// Server round trip time is calculated as an exponentially-weighted moving
/// averaging formula with a weighting factor. A factor of 0.2 places approximately
/// 85% of the RTT weight on the 9 most recent observations. Using a divisor instead
/// of a floating point provides the closest integer accuracy.
pub const ROUND_TRIP_DIVISOR: i64 = 5;

/// Describes the server role within a server set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ServerType {
    /// Standalone server.
    Standalone,
    /// Shard router.
    Mongos,
    /// Replica set primary.
    RSPrimary,
    /// Replica set secondary.
    RSSecondary,
    /// Replica set arbiter.
    RSArbiter,
    /// Replica set member of some other type.
    RSOther,
    /// Replica set ghost member.
    RSGhost,
    /// Server type is currently unknown.
    Unknown,
}

impl Default for ServerType {
    fn default() -> Self {
        ServerType::Unknown
    }
}

/// Server information gathered from server monitoring.
#[derive(Clone, Debug, Default)]
pub struct ServerDescription {
    /// The server type.
    pub server_type: ServerType,
    /// Any error encountered while monitoring this server.
    pub err: Arc<Option<Error>>,
    /// The average round-trip time over the last 5 monitoring checks.
    pub round_trip_time: Option<i64>,
    /// The minimum wire version supported by this server.
    pub min_wire_version: i64,
    /// The maximum wire version supported by this server.
    pub max_wire_version: i64,
    /// The server's host information, if it is part of a replica set.
    pub me: Option<Host>,
    /// All hosts in the replica set known by this server.
    pub hosts: Vec<Host>,
    /// All passive members of the replica set known by this server.
    pub passives: Vec<Host>,
    /// All arbiters in the replica set known by this server.
    pub arbiters: Vec<Host>,
    /// Server tags for targeted read operations on specific replica set members.
    pub tags: BTreeMap<String, String>,
    /// The replica set name.
    pub set_name: String,
    /// The server's current election id, if it believes it is a primary.
    pub election_id: Option<oid::ObjectId>,
    /// The server's opinion of who the primary is.
    pub primary: Option<Host>,
    /// The current replica set version number.
    pub set_version: Option<i64>,
}

/// Holds status and connection information about a single server.
#[derive(Clone, Debug)]
pub struct Server {
    /// Host connection details.
    pub host: Host,
    /// Monitored server information.
    pub description: Arc<RwLock<ServerDescription>>,
    /// The connection pool for this server.
    pool: Arc<ConnectionPool>,
    /// A reference to the associated server monitor.
    monitor: Arc<Monitor>,
}

impl FromStr for ServerType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "Standalone" => ServerType::Standalone,
            "Mongos" => ServerType::Mongos,
            "RSPrimary" => ServerType::RSPrimary,
            "RSSecondary" => ServerType::RSSecondary,
            "RSArbiter" => ServerType::RSArbiter,
            "RSOther" => ServerType::RSOther,
            "RSGhost" => ServerType::RSGhost,
            _ => ServerType::Unknown,
        })
    }
}

impl ServerDescription {
    /// Returns a default, unknown server description.
    pub fn new() -> ServerDescription {
        Default::default()
    }

    // Updates the server description using an isMaster server response.
    pub fn update(&mut self, ismaster: IsMasterResult, round_trip_time: i64) {
        if !ismaster.ok {
            self.set_err(OperationError(
                String::from("ismaster returned a not-ok response."),
            ));
            return;
        }

        self.min_wire_version = ismaster.min_wire_version;
        self.max_wire_version = ismaster.max_wire_version;
        self.me = ismaster.me;
        self.hosts = ismaster.hosts;
        self.passives = ismaster.passives;
        self.arbiters = ismaster.arbiters;
        self.tags = ismaster.tags;
        self.set_name = ismaster.set_name;
        self.election_id = ismaster.election_id;
        self.primary = ismaster.primary;
        self.set_version = ismaster.set_version;
        self.round_trip_time = match self.round_trip_time {
            Some(old_rtt) => {
                // (rtt / div) + (old_rtt * (div-1)/div)
                Some(
                    round_trip_time / ROUND_TRIP_DIVISOR +
                        (old_rtt / (ROUND_TRIP_DIVISOR)) * (ROUND_TRIP_DIVISOR - 1),
                )
            }
            None => Some(round_trip_time),
        };

        let set_name_empty = self.set_name.is_empty();
        let msg_empty = ismaster.msg.is_empty();

        self.server_type = if msg_empty && set_name_empty && !ismaster.is_replica_set {
            ServerType::Standalone
        } else if !msg_empty {
            ServerType::Mongos
        } else if ismaster.is_master && !set_name_empty {
            ServerType::RSPrimary
        } else if ismaster.is_secondary && !set_name_empty && !ismaster.hidden {
            ServerType::RSSecondary
        } else if ismaster.arbiter_only && !set_name_empty {
            ServerType::RSArbiter
        } else if !set_name_empty {
            ServerType::RSOther
        } else if ismaster.is_replica_set {
            ServerType::RSGhost
        } else {
            ServerType::Unknown
        }
    }

    // Sets an encountered error and reverts the server type to Unknown.
    pub fn set_err(&mut self, err: Error) {
        self.err = Arc::new(Some(err));
        self.clear();
    }

    // Reset the server type to unknown.
    pub fn clear(&mut self) {
        self.election_id = None;
        self.round_trip_time = None;
        self.server_type = ServerType::Unknown;
        self.set_name = String::new();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.monitor.running.store(false, Ordering::SeqCst);
    }
}

impl Server {
    /// Returns a new server with the given host, initializing a new connection pool and monitor.
    pub fn new(
        client: Client,
        host: Host,
        top_description: Arc<RwLock<TopologyDescription>>,
        run_monitor: bool,
        connector: StreamConnector,
        pool_size: Option<usize>,
        idle_timeout: Option<Duration>,
    ) -> Server {
        let description = Arc::new(RwLock::new(ServerDescription::new()));

        // Create new monitor thread
        let host_clone = host.clone();
        let desc_clone = description.clone();

        let pool_inner = if let Some(size) = pool_size {
            ConnectionPool::with_options(host.clone(), connector.clone(), size, idle_timeout.unwrap_or_else(|| DEFAULT_TIMEOUT_ON_IDLE))
        } else {
            ConnectionPool::new(host.clone(), connector.clone())
        };

        let pool = Arc::new(pool_inner);

        // Fails silently
        let monitor = Arc::new(Monitor::new(
            client,
            host_clone,
            pool.clone(),
            top_description,
            desc_clone,
            connector,
        ));

        if run_monitor {
            let monitor_clone = monitor.clone();
            thread::spawn(move || { monitor_clone.run(); });
        }

        Server {
            host: host,
            pool: pool,
            description: description,
            monitor: monitor,
        }
    }

    /// Returns a server stream from the connection pool.
    pub fn acquire_stream(&self, client: Client) -> Result<PooledStream> {
        self.pool.acquire_stream(client)
    }

    /// Request an update from the monitor on the server status.
    pub fn request_update(&self) {
        self.monitor.request_update();
    }
}
