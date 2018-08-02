//! MongoDB server set topology and asynchronous monitoring.
pub mod server;
pub mod monitor;

use {Client, Result};
use Error::{self, ArgumentError, OperationError};

use bson::oid;

use common::{ReadPreference, ReadMode};
use connstring::{ConnectionString, Host};
use pool::PooledStream;
use stream::StreamConnector;

use rand::{thread_rng, Rng};

use std::collections::HashMap;
use std::fmt;
use std::i64;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;
use time;

use self::server::{Server, ServerDescription, ServerType};

pub const DEFAULT_HEARTBEAT_FREQUENCY_MS: u32 = 10000;
pub const DEFAULT_LOCAL_THRESHOLD_MS: i64 = 15;
pub const DEFAULT_SERVER_SELECTION_TIMEOUT_MS: i64 = 30000;

/// Describes the type of topology for a server set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TopologyType {
    Single,
    ReplicaSetNoPrimary,
    ReplicaSetWithPrimary,
    Sharded,
    Unknown,
}

/// Topology information gathered from server set monitoring.
#[derive(Clone)]
pub struct TopologyDescription {
    pub topology_type: TopologyType,
    /// The set name for a replica set topology. If the topology
    /// is not a replica set, this will be an empty string.
    pub set_name: String,
    /// Known servers within the topology.
    pub servers: HashMap<Host, Server>,
    /// The server connection health check frequency.
    /// The default is 10 seconds.
    pub heartbeat_frequency_ms: u32,
    /// The size of the latency window for selecting suitable servers.
    /// The default is 15 milliseconds.
    pub local_threshold_ms: i64,
    /// This defines how long to block for server selection before
    /// returning an error. The default is 30 seconds.
    pub server_selection_timeout_ms: i64,
    // The largest election id seen from a server in the topology.
    max_election_id: Option<oid::ObjectId>,
    // If true, all servers in the topology fall within the compatible
    // mongodb version for this driver.
    compatible: bool,
    // The largest set version seen from a primary in the topology.
    max_set_version: Option<i64>,
    compat_error: String,
    stream_connector: StreamConnector,
}

impl fmt::Debug for TopologyDescription {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TopologyDescription")
            .field("topology_type", &self.topology_type)
            .field("set_name", &self.set_name)
            .field("servers", &"HashMap<Host, Server> { .. }")
            .field("heartbeat_frequency_ms", &self.heartbeat_frequency_ms)
            .field("local_threshold_ms", &self.local_threshold_ms)
            .field("server_selection_timeout_ms", &self.server_selection_timeout_ms)
            .field("max_election_id", &self.max_election_id)
            .field("compatible", &self.compatible)
            .field("max_set_version", &self.max_set_version)
            .field("compat_error", &self.compat_error)
            .field("stream_connector", &"StreamConnector { .. }")
            .finish()
    }
}

/// Holds status and connection information about a server set.
#[derive(Clone, Debug)]
pub struct Topology {
    /// The initial connection configuration.
    pub config: ConnectionString,
    /// Monitored topology information.
    pub description: Arc<RwLock<TopologyDescription>>,
}

impl FromStr for TopologyType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "Single" => TopologyType::Single,
            "ReplicaSetNoPrimary" => TopologyType::ReplicaSetNoPrimary,
            "ReplicaSetWithPrimary" => TopologyType::ReplicaSetWithPrimary,
            "Sharded" => TopologyType::Sharded,
            _ => TopologyType::Unknown,
        })
    }
}

impl Default for TopologyDescription {
    fn default() -> Self {
        TopologyDescription {
            topology_type: TopologyType::Unknown,
            set_name: String::new(),
            heartbeat_frequency_ms: DEFAULT_HEARTBEAT_FREQUENCY_MS,
            server_selection_timeout_ms: DEFAULT_SERVER_SELECTION_TIMEOUT_MS,
            local_threshold_ms: DEFAULT_LOCAL_THRESHOLD_MS,
            servers: HashMap::new(),
            max_election_id: None,
            compatible: true,
            compat_error: String::new(),
            max_set_version: None,
            stream_connector: StreamConnector::Tcp,
        }
    }
}

impl TopologyDescription {
    /// Returns a default, unknown topology description.
    pub fn new(stream_connector: StreamConnector) -> TopologyDescription {
        TopologyDescription { stream_connector, ..Default::default() }
    }

    /// Returns the nearest server stream, calculated by round trip time.
    fn get_nearest_from_vec(&self, client: Client, servers: &mut Vec<Host>) -> Result<(PooledStream, ServerType)> {
        servers.sort_by(|a, b| {
            let mut a_rtt = i64::MAX;
            let mut b_rtt = i64::MAX;
            if let Some(server) = self.servers.get(a) {
                if let Ok(a_description) = server.description.read() {
                    a_rtt = a_description.round_trip_time.unwrap_or(i64::MAX);
                }
            }
            if let Some(server) = self.servers.get(b) {
                if let Ok(b_description) = server.description.read() {
                    b_rtt = b_description.round_trip_time.unwrap_or(i64::MAX);
                }
            }

            a_rtt.cmp(&b_rtt)
        });

        // Iterate over each host until one's stream can be acquired.
        for host in servers {
            if let Some(server) = self.servers.get(host) {
                if let Ok(description) = server.description.read() {
                    if description.round_trip_time.is_none() {
                        break;
                    } else if let Ok(stream) = server.acquire_stream(client.clone()) {
                        return Ok((stream, description.server_type));
                    }
                }
            }
        }
        Err(OperationError(String::from(
            "No servers available for the provided ReadPreference.",
        )))
    }

    /// Returns a random server stream from the vector.
    fn get_rand_from_vec(&self, client: Client, servers: &mut Vec<Host>) -> Result<(PooledStream, ServerType)> {
        while !servers.is_empty() {
            let len = servers.len();
            let index = thread_rng().gen_range(0, len);

            if let Some(server) = self.servers.get(&servers[index]) {
                if let Ok(stream) = server.acquire_stream(client.clone()) {
                    if let Ok(description) = server.description.read() {
                        return Ok((stream, description.server_type));
                    }
                }
            }
            servers.remove(index);
        }
        Err(OperationError(String::from(
            "No servers available for the provided ReadPreference.",
        )))
    }

    /// Returns a server stream for read operations.
    pub fn acquire_stream(
        &self,
        client: Client,
        read_preference: &ReadPreference,
    ) -> Result<(PooledStream, bool, bool)> {
        let (mut hosts, rand) = self.choose_hosts(read_preference)?;

        // Filter hosts by tagsets
        if self.topology_type != TopologyType::Sharded &&
            self.topology_type != TopologyType::Single
        {
            self.filter_hosts(&mut hosts, read_preference);
        }

        // Special case - If secondaries are found, by are filtered out by tag sets,
        // the topology should return any available primaries instead.
        if hosts.is_empty() && read_preference.mode == ReadMode::SecondaryPreferred {
            let read_pref = ReadPreference {
                mode: ReadMode::PrimaryPreferred,
                ..read_preference.clone()
            };
            return self.acquire_stream(client, &read_pref);
        }

        // If no servers are available, request an update from all monitors.
        if hosts.is_empty() {
            for server in self.servers.values() {
                server.request_update();
            }
        }

        // Filter hosts by round trip times within the latency window.
        self.filter_latency_hosts(&mut hosts);

        // Retrieve a server stream from the list of acceptable hosts.
        let (pooled_stream, server_type) = if rand {
            self.get_rand_from_vec(client, &mut hosts)?
        } else {
            self.get_nearest_from_vec(client, &mut hosts)?
        };

        // Determine how to handle server-side logic based on ReadMode and TopologyType.
        let (slave_ok, send_read_pref) = match self.topology_type {
            TopologyType::Unknown => (false, false),
            TopologyType::Single => {
                match server_type {
                    ServerType::Mongos => {
                        match read_preference.mode {
                            ReadMode::Primary => (false, false),
                            ReadMode::SecondaryPreferred => {
                                (true, !read_preference.tag_sets.is_empty())
                            }
                            ReadMode::Secondary |
                            ReadMode::PrimaryPreferred |
                            ReadMode::Nearest => (true, true),
                        }
                    }
                    _ => (true, false),
                }
            }
            TopologyType::ReplicaSetWithPrimary |
            TopologyType::ReplicaSetNoPrimary => {
                match read_preference.mode {
                    ReadMode::Primary => (false, false),
                    _ => (true, false),
                }
            }
            TopologyType::Sharded => {
                match read_preference.mode {
                    ReadMode::Primary => (false, false),
                    ReadMode::SecondaryPreferred => (true, !read_preference.tag_sets.is_empty()),
                    ReadMode::Secondary |
                    ReadMode::PrimaryPreferred |
                    ReadMode::Nearest => (true, true),
                }
            }
        };

        Ok((pooled_stream, slave_ok, send_read_pref))
    }

    /// Returns a server stream for write operations.
    pub fn acquire_write_stream(&self, client: Client) -> Result<PooledStream> {
        let (mut hosts, rand) = self.choose_write_hosts();

        // If no servers are available, request an update from all monitors.
        if hosts.is_empty() {
            for server in self.servers.values() {
                server.request_update();
            }
        }

        if rand {
            Ok(self.get_rand_from_vec(client, &mut hosts)?.0)
        } else {
            Ok(self.get_nearest_from_vec(client, &mut hosts)?.0)
        }
    }

    /// Filters a given set of hosts based on the provided read preference tag sets.
    pub fn filter_hosts(&self, hosts: &mut Vec<Host>, read_preference: &ReadPreference) {
        let mut tag_filter = None;

        if read_preference.tag_sets.is_empty() {
            return;
        }

        // Set the tag_filter to the first tag set that matches at least one server in the set.
        for tags in &read_preference.tag_sets {
            for host in &*hosts {
                if let Some(server) = self.servers.get(host) {
                    let description = server.description.read().unwrap();

                    // Check whether the read preference tags are contained
                    // within the server description tags.
                    let mut valid = true;
                    for (key, val) in tags {
                        match description.tags.get(key) {
                            Some(v) => {
                                if val != v {
                                    valid = false;
                                    break;
                                }
                            }
                            None => {
                                valid = false;
                                break;
                            }
                        }
                    }

                    if valid {
                        tag_filter = Some(tags);
                        break;
                    }
                }
            }

            // Short-circuit if tag filter has been found.
            if tag_filter.is_some() {
                break;
            }
        }

        match tag_filter {
            None => {
                // If no tags match but the replica set has a primary that is returnable with
                // the given ReadMode, return that primary server.
                if self.topology_type == TopologyType::ReplicaSetWithPrimary &&
                    (read_preference.mode == ReadMode::Primary ||
                         read_preference.mode == ReadMode::PrimaryPreferred)
                {
                    // Retain primaries.
                    hosts.retain(|host| if let Some(server) = self.servers.get(host) {
                        let description = server.description.read().unwrap();
                        description.server_type == ServerType::RSPrimary
                    } else {
                        false
                    });
                } else {
                    // If no tags match and the above case does not occur,
                    // filter out all provided servers.
                    hosts.clear();
                }
            }
            Some(tag_filter) => {
                // Filter out hosts by the discovered matching tagset.
                hosts.retain(|host| {
                    if let Some(server) = self.servers.get(host) {
                        let description = server.description.read().unwrap();

                        // Validate tag sets.
                        for (key, val) in tag_filter {
                            match description.tags.get(key) {
                                Some(v) => {
                                    if val != v {
                                        return false;
                                    }
                                }
                                None => return false,
                            }
                        }
                        true
                    } else {
                        false
                    }
                });
            }
        }
    }

    /// Filter out provided hosts by creating a latency window around
    /// the server with the lowest round-trip time.
    pub fn filter_latency_hosts(&self, hosts: &mut Vec<Host>) {
        if hosts.len() <= 1 {
            return;
        }

        // Find the shortest round-trip time.
        let shortest_rtt = hosts.iter().fold(
            {
                // Initialize the value to the first server's round-trip-time, or i64::MAX.
                if let Some(server) = self.servers.get(&hosts[0]) {
                    if let Ok(description) = server.description.read() {
                        description.round_trip_time.unwrap_or(i64::MAX)
                    } else {
                        i64::MAX
                    }
                } else {
                    i64::MAX
                }
            },
            |acc, host| {
                // Compare the previous shortest rtt with the host rtt.
                if let Some(server) = self.servers.get(host) {
                    if let Ok(description) = server.description.read() {
                        let item_rtt = description.round_trip_time.unwrap_or(i64::MAX);
                        if acc < item_rtt {
                            return acc;
                        } else {
                            return item_rtt;
                        }
                    }
                }
                acc
            },
        );

        // If the shortest rtt is i64::MAX, all server rtts are None or could not be read.
        if shortest_rtt == i64::MAX {
            return;
        }

        let high_rtt = shortest_rtt + self.local_threshold_ms;

        // Filter hosts by the latency window [shortest_rtt, high_rtt].
        hosts.retain(|host| {
            if let Some(server) = self.servers.get(host) {
                if let Ok(description) = server.description.read() {
                    let rtt = description.round_trip_time.unwrap_or(i64::MAX);
                    return shortest_rtt <= rtt && rtt <= high_rtt;
                }
            }
            false
        });
    }

    /// Returns suitable servers for write operations and whether to take a random element.
    pub fn choose_write_hosts(&self) -> (Vec<Host>, bool) {
        if self.servers.is_empty() {
            return (Vec::new(), true);
        }

        match self.topology_type {
            // No servers are suitable.
            TopologyType::Unknown => (Vec::new(), true),
            // All servers are suitable.
            TopologyType::Single => (self.servers.keys().cloned().collect(), true),
            TopologyType::Sharded => (self.servers.keys().cloned().collect(), false),
            // Only primary replica set members are suitable.
            _ => {
                (
                    self.servers
                        .keys()
                        .filter_map(|host| {
                            if let Some(server) = self.servers.get(host) {
                                if let Ok(description) = server.description.read() {
                                    if description.server_type == ServerType::RSPrimary {
                                        return Some(host.clone());
                                    }
                                }
                            }
                            None
                        })
                        .collect(),
                    true,
                )
            }
        }
    }

    /// Returns suitable servers for read operations and whether to take a random element.
    pub fn choose_hosts(&self, read_preference: &ReadPreference) -> Result<(Vec<Host>, bool)> {
        if self.servers.is_empty() {
            return Ok((Vec::new(), true));
        }

        match self.topology_type {
            // No servers are suitable.
            TopologyType::Unknown => Ok((Vec::new(), true)),
            // All servers are suitable.
            TopologyType::Single => Ok((self.servers.keys().cloned().collect(), true)),
            TopologyType::Sharded => Ok((self.servers.keys().cloned().collect(), false)),
            _ => {

                // Handle replica set server selection
                // Short circuit if nearest
                if read_preference.mode == ReadMode::Nearest {
                    let mut hosts = Vec::new();

                    for (host, server) in &self.servers {
                        if server.description.read()?.server_type == ServerType::Unknown {
                            continue;
                        }

                        hosts.push(host.clone());
                    }

                    return Ok((hosts, false));
                }

                let mut primaries = Vec::new();
                let mut secondaries = Vec::new();

                // Collect a list of primaries and secondaries in the set
                for (host, server) in &self.servers {
                    let stype = server.description.read().unwrap().server_type;
                    match stype {
                        ServerType::RSPrimary => primaries.push(host.clone()),
                        ServerType::RSSecondary => secondaries.push(host.clone()),
                        _ => (),
                    }
                }

                // Choose an appropriate server at random based on the read preference.
                match read_preference.mode {
                    ReadMode::Primary => Ok((primaries, true)),
                    ReadMode::PrimaryPreferred => {
                        let servers = if primaries.is_empty() {
                            secondaries
                        } else {
                            primaries
                        };
                        Ok((servers, true))
                    }
                    ReadMode::Secondary => Ok((secondaries, true)),
                    ReadMode::SecondaryPreferred => {
                        let servers = if secondaries.is_empty() {
                            primaries
                        } else {
                            secondaries
                        };
                        Ok((servers, true))
                    }
                    ReadMode::Nearest => Ok((self.servers.keys().cloned().collect(), false)),
                }
            }
        }
    }

    /// Update the topology description, but don't start any monitors for new servers.
    pub fn update_without_monitor(
        &mut self,
        host: Host,
        description: Arc<RwLock<ServerDescription>>,
        client: Client,
        top_arc: Arc<RwLock<TopologyDescription>>,
    ) {
        self.update_private(host, description, client, top_arc, false);
    }

    /// Updates the topology description based on an updated server description.
    pub fn update(
        &mut self,
        host: Host,
        description: Arc<RwLock<ServerDescription>>,
        client: Client,
        top_arc: Arc<RwLock<TopologyDescription>>,
    ) {
        self.update_private(host, description, client, top_arc, true);
    }

    // Internal topology description update helper.
    fn update_private(
        &mut self,
        host: Host,
        description: Arc<RwLock<ServerDescription>>,
        client: Client,
        top_arc: Arc<RwLock<TopologyDescription>>,
        run_monitor: bool,
    ) {

        let stype = description.read().unwrap().server_type;
        match self.topology_type {
            TopologyType::Unknown => {
                match stype {
                    ServerType::Standalone => self.update_unknown_with_standalone(host),
                    ServerType::Mongos => self.topology_type = TopologyType::Sharded,
                    ServerType::RSPrimary => {
                        self.update_rs_from_primary(host, description, client, top_arc, run_monitor)
                    }
                    ServerType::RSSecondary | ServerType::RSArbiter | ServerType::RSOther => {
                        self.update_rs_without_primary(
                            host,
                            description,
                            client,
                            top_arc,
                            run_monitor,
                        )
                    }
                    _ => (),
                }
            }
            TopologyType::ReplicaSetNoPrimary => {
                match stype {
                    ServerType::Standalone | ServerType::Mongos => {
                        self.servers.remove(&host);
                        self.check_if_has_primary();
                    }
                    ServerType::RSPrimary => {
                        self.update_rs_from_primary(host, description, client, top_arc, run_monitor)
                    }
                    ServerType::RSSecondary | ServerType::RSArbiter | ServerType::RSOther => {
                        self.update_rs_without_primary(
                            host,
                            description,
                            client,
                            top_arc,
                            run_monitor,
                        )
                    }
                    _ => self.check_if_has_primary(),
                }
            }
            TopologyType::ReplicaSetWithPrimary => {
                match stype {
                    ServerType::Standalone | ServerType::Mongos => {
                        self.servers.remove(&host);
                        self.check_if_has_primary();
                    }
                    ServerType::RSPrimary => {
                        self.update_rs_from_primary(host, description, client, top_arc, run_monitor)
                    }
                    ServerType::RSSecondary | ServerType::RSArbiter | ServerType::RSOther => {
                        self.update_rs_with_primary_from_member(host, description)
                    }
                    _ => self.check_if_has_primary(),
                }
            }
            TopologyType::Sharded => {
                match stype {
                    ServerType::Unknown | ServerType::Mongos => (),
                    _ => {
                        self.servers.remove(&host);
                    }
                }
            }
            TopologyType::Single => (),
        }
    }

    // Sets the correct replica set topology type.
    fn check_if_has_primary(&mut self) {
        for server in self.servers.values() {
            let stype = server.description.read().unwrap().server_type;
            if stype == ServerType::RSPrimary {
                self.topology_type = TopologyType::ReplicaSetWithPrimary;
                return;
            }
        }
        self.topology_type = TopologyType::ReplicaSetNoPrimary;
    }


    // Updates an unknown topology with a new standalone server description.
    fn update_unknown_with_standalone(&mut self, host: Host) {
        if !self.servers.contains_key(&host) {
            return;
        }

        if self.servers.len() == 1 {
            self.topology_type = TopologyType::Single;
        } else {
            self.servers.remove(&host);
        }
    }

    // Updates a replica set topology with a new primary server description.
    fn update_rs_from_primary(
        &mut self,
        host: Host,
        description: Arc<RwLock<ServerDescription>>,
        client: Client,
        top_arc: Arc<RwLock<TopologyDescription>>,
        run_monitor: bool,
    ) {

        if !self.servers.contains_key(&host) {
            return;
        }

        let description_set_name = description.read().unwrap().set_name.clone();

        if self.set_name.is_empty() {
            self.set_name = description_set_name;
        } else if self.set_name != description_set_name {
            // Primary found, but it doesn't have the setName
            // provided by the user or previously discovered.
            self.servers.remove(&host);
            self.check_if_has_primary();
            return;
        }

        let (description_set_version, description_election_id) = {
            let description_guard = description.read().unwrap();

            (
                description_guard.set_version,
                description_guard.election_id.clone(),
            )
        };

        if description_set_version.is_some() && description_election_id.is_some() {
            if self.max_set_version.is_some() && self.max_election_id.is_some() &&
                (self.max_set_version.unwrap() > description_set_version.unwrap() ||
                     (self.max_set_version.unwrap() == description_set_version.unwrap() &&
                          self.max_election_id.as_ref().unwrap() >
                              description_election_id.as_ref().unwrap()))
            {
                // Stale primary
                if let Some(server) = self.servers.get(&host) {
                    {
                        let mut server_description = server.description.write().unwrap();
                        server_description.server_type = ServerType::Unknown;
                        server_description.set_name = String::new();
                        server_description.election_id = None;
                    }
                }
                self.check_if_has_primary();
                return;
            } else {
                self.max_election_id = description_election_id.clone();
            }
        }

        if description_set_version.is_some() &&
            (self.max_set_version.is_none() ||
                 description_set_version.unwrap() > self.max_set_version.unwrap())
        {
            self.max_set_version = description_set_version;
        }

        // Invalidate any old primaries
        for (top_host, server) in &self.servers {
            if *top_host != host {
                let mut server_description = server.description.write().unwrap();
                if server_description.server_type == ServerType::RSPrimary {
                    server_description.server_type = ServerType::Unknown;
                    server_description.set_name = String::new();
                    server_description.election_id = None;
                }
            }
        }

        self.add_missing_hosts(description.clone(), client, top_arc, run_monitor);

        // Remove hosts that are not reported by the primary.
        let valid_hosts: Vec<_> = {
            let description_guard = description.read().unwrap();

            description_guard
                .hosts
                .iter()
                .cloned()
                .chain(description_guard.passives.iter().cloned())
                .chain(description_guard.arbiters.iter().cloned())
                .collect()
        };

        self.servers.retain(|host, _| valid_hosts.contains(host));

        self.check_if_has_primary();
    }

    // Updates a replica set topology with a missing primary.
    fn update_rs_without_primary(
        &mut self,
        host: Host,
        description: Arc<RwLock<ServerDescription>>,
        client: Client,
        top_arc: Arc<RwLock<TopologyDescription>>,
        run_monitor: bool,
    ) {

        self.topology_type = TopologyType::ReplicaSetNoPrimary;
        if !self.servers.contains_key(&host) {
            return;
        }

        let set_name = description.read().unwrap().set_name.clone();

        if self.set_name.is_empty() {
            self.set_name = set_name;
        } else if self.set_name != set_name {
            self.servers.remove(&host);
            self.check_if_has_primary();
            return;
        }

        self.add_missing_hosts(description.clone(), client, top_arc, run_monitor);

        let description_me = description.read().unwrap().me.clone();

        if let Some(me) = description_me {
            if host != me {
                self.servers.remove(&host);
                self.check_if_has_primary();
            }
        }
    }

    // Updates a replica set topology with an updated member description.
    fn update_rs_with_primary_from_member(
        &mut self,
        host: Host,
        description: Arc<RwLock<ServerDescription>>,
    ) {
        if !self.servers.contains_key(&host) {
            return;
        }

        if self.set_name != description.read().unwrap().set_name {
            self.servers.remove(&host);
        }

        let description_me = description.read().unwrap().me.clone();

        if let Some(me) = description_me {
            if host != me {
                self.servers.remove(&host);
            }
            return;
        }

        self.check_if_has_primary();
    }

    // Begins monitoring hosts that are not currently being monitored.
    fn add_missing_hosts(
        &mut self,
        description: Arc<RwLock<ServerDescription>>,
        client: Client,
        top_arc: Arc<RwLock<TopologyDescription>>,
        run_monitor: bool,
    ) {

        let hosts: Vec<_> = {
            let description_guard = description.read().unwrap();

            description_guard
                .hosts
                .iter()
                .cloned()
                .chain(description_guard.passives.iter().cloned())
                .chain(description_guard.arbiters.iter().cloned())
                .collect()
        };

        for host in hosts {
            if !self.servers.contains_key(&host) {
                let server = Server::new(
                    client.clone(),
                    host.clone(),
                    top_arc.clone(),
                    run_monitor,
                    self.stream_connector.clone(),
                );
                self.servers.insert(host, server);
            }
        }
    }
}

impl Topology {
    /// Returns a new topology with the given configuration and description.
    pub fn new(
        config: ConnectionString,
        description: Option<TopologyDescription>,
        connector: StreamConnector,
    ) -> Result<Topology> {

        let mut options = description.unwrap_or_else(|| TopologyDescription::new(connector));

        if config.hosts.len() > 1 && options.topology_type == TopologyType::Single {
            return Err(ArgumentError(String::from(
                "TopologyType::Single cannot be used with multiple seeds.",
            )));
        }

        if let Some(ref config_opts) = config.options {
            if let Some(name) = config_opts.options.get("replicaSet") {
                options.set_name = name.to_owned();
                options.topology_type = TopologyType::ReplicaSetNoPrimary;
            }
        }

        if !options.set_name.is_empty() &&
            options.topology_type != TopologyType::ReplicaSetNoPrimary
        {
            return Err(ArgumentError(String::from(
                "TopologyType must be ReplicaSetNoPrimary if set_name is provided.",
            )));
        }

        let top_description = Arc::new(RwLock::new(options));

        Ok(Topology {
            config: config,
            description: top_description,
        })
    }

    // Private server stream acquisition helper.
    fn acquire_stream_private(
        &self,
        client: Client,
        read_preference: Option<ReadPreference>,
        write: bool,
    ) -> Result<(PooledStream, bool, bool)> {
        // Note start of server selection.
        let time = time::get_time();
        let start_ms = time.sec * 1000 + (time.nsec as i64) / 1000000;

        loop {
            let result = if write {
                match self.description.read()?.acquire_write_stream(client.clone()) {
                    Ok(stream) => Ok((stream, false, false)),
                    Err(err) => Err(err),
                }
            } else {
                self.description.read()?.acquire_stream(
                    client.clone(),
                    read_preference.as_ref().unwrap(),
                )
            };

            match result {
                Ok(stream) => return Ok(stream),
                Err(err) => {
                    // Check duration of current server selection and return an error if
                    // overdue.
                    let end_time = time::get_time();
                    let end_ms = end_time.sec * 1000 + (end_time.nsec as i64) / 1000000;
                    if end_ms - start_ms >= self.description.read()?.server_selection_timeout_ms {
                        return Err(err);
                    }
                }
            };

            // Otherwise, sleep for a little while.
            thread::sleep(Duration::from_millis(500));
        }
    }

    /// Returns a server stream for read operations.
    pub fn acquire_stream(
        &self,
        client: Client,
        read_preference: ReadPreference,
    ) -> Result<(PooledStream, bool, bool)> {
        self.acquire_stream_private(client, Some(read_preference), false)
    }

    /// Returns a server stream for write operations.
    pub fn acquire_write_stream(&self, client: Client) -> Result<PooledStream> {
        let (stream, _, _) = self.acquire_stream_private(client, None, true)?;
        Ok(stream)
    }
}
