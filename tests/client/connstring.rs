use mongodb::connstring;

#[test]
fn valid_uri() {
    let valid_uris = vec!(
        "mongodb://localhost",
        "mongodb://localhost/",
        "mongodb://localhost/?",
        "mongodb://localhost:27017",
        "mongodb://localhost:27017/",
        "mongodb://localhost:27017/?",
        "mongodb://127.0.0.1",
        "mongodb://127.0.0.1/",
        "mongodb://127.0.0.1/?",
        "mongodb://127.0.0.1:27017",
        "mongodb://127.0.0.1:27017/",
        "mongodb://127.0.0.1:27017/?",
    );

    for uri in valid_uris {
        assert!(connstring::parse(uri).is_ok());
    }
}

#[test]
fn invalid_prefix() {
    let invalid_uris = vec!(
        "mongodb:/localhost",
        "mngodb://localhost",
        "mongodb//localhost",
        "://localhost",
        "localhost:27017",
    );

    for uri in invalid_uris {
        assert!(connstring::parse(uri).is_err());
    }
}

#[test]
fn optional_user_password() {
    let uri = "mongodb://local:27017";
    assert!(connstring::parse(uri).is_ok());
}

#[test]
fn parse_user_password() {
    let uri = "mongodb://user:password@local:27017";
    let connstr = connstring::parse(uri).unwrap();
    assert_eq!("user", connstr.user.unwrap());
    assert_eq!("password", connstr.password.unwrap());
}

#[test]
fn hash_in_username() {
    let uri = "mongodb://us#er:password@local:27017";
    let connstr = connstring::parse(uri).unwrap();
    assert_eq!("us#er", connstr.user.unwrap());
    assert_eq!("password", connstr.password.unwrap());
}

#[test]
fn hash_in_password() {
    let uri = "mongodb://user:pass#word@local:27017";
    let connstr = connstring::parse(uri).unwrap();
    assert_eq!("user", connstr.user.unwrap());
    assert_eq!("pass#word", connstr.password.unwrap());
}

#[test]
fn required_host() {
    let missing_hosts = vec!(
        "mongodb://",
        "mongodb:///fake",
        "mongodb://?opt",
        "mongodb:///?opt",
        );

    for uri in missing_hosts {
        assert!(connstring::parse(uri).is_err());
    }

    let good_host = "mongodb://local";
    let result = connstring::parse(good_host);
    assert!(result.is_ok());

    let connstr = result.unwrap();
    assert_eq!(1, connstr.hosts.len());
    assert_eq!("local", connstr.hosts[0].host_name);
}

#[test]
fn replica_sets() {
    let uri = "mongodb://local:27017,remote:27018,japan:30000";
    let result = connstring::parse(uri);
    assert!(result.is_ok());

    let connstr = result.unwrap();
    assert_eq!(3, connstr.hosts.len());
    assert_eq!("local", connstr.hosts[0].host_name);
    assert_eq!(27017, connstr.hosts[0].port);
    assert_eq!("remote", connstr.hosts[1].host_name);
    assert_eq!(27018, connstr.hosts[1].port);
    assert_eq!("japan", connstr.hosts[2].host_name);
    assert_eq!(30000, connstr.hosts[2].port);
}

#[test]
fn default_port_on_single_host() {
    let uri = "mongodb://local/";
    let connstring = connstring::parse(uri).unwrap();
    assert_eq!(connstring::DEFAULT_PORT, connstring.hosts[0].port);
}

#[test]
fn default_port_on_replica_set() {
    let uri = "mongodb://local,remote/";
    let connstring = connstring::parse(uri).unwrap();
    assert_eq!(connstring::DEFAULT_PORT, connstring.hosts[0].port);
    assert_eq!(connstring::DEFAULT_PORT, connstring.hosts[1].port);
}

#[test]
fn default_database() {
    let uri1 = "mongodb://local/";
    let uri2 = "mongodb://local";
    let connstring1 = connstring::parse(uri1).unwrap();
    let connstring2 = connstring::parse(uri2).unwrap();
    assert_eq!("test", connstring1.database.unwrap());
    assert_eq!("test", connstring2.database.unwrap());
}

#[test]
fn overridable_database() {
    let uri = "mongodb://localhost,a,x:34343,b/tools";
    let connstring = connstring::parse(uri).unwrap();
    assert_eq!("tools", connstring.database.unwrap());
}

#[test]
fn query_separators() {
    for delim in &[";", "&"] {
        let uri = format!("mongodb://rust/?replicaSet=myreplset{}slaveOk=true{}x=1", delim, delim);
        let result = connstring::parse(&uri);
        assert!(result.is_ok());

        let connstr = result.unwrap();
        let options = connstr.options.unwrap();
        assert_eq!("true", options.get("slaveOk").unwrap());
        assert_eq!("myreplset", options.get("replicaSet").unwrap());
        assert_eq!("1", options.get("x").unwrap());
    }
}

#[test]
fn read_pref_tags() {
    let pref_set = vec!(
        vec!("dc:ny"),
        vec!("dc:ny,rack:1"),
        vec!("dc:ny,rack:1", "dc:sf,rack:2"),
        vec!("dc:ny,rack:1", "dc:sf,rack:2", ""),
        vec!("dc:ny,rack:1", "dc:ny", ""),
        );

    for delim in &["&", ";"] {
        for prefs in &pref_set {
            let mut uri = format!("mongodb://localhost/?readPreferenceTags={}", prefs[0]);
            for pref in &prefs[1..] {
                uri = format!("{}{}readPreferenceTags={}", uri, delim, pref);
            }

            let connstr = connstring::parse(&uri).unwrap();
            let options = connstr.options.unwrap();
            assert_eq!(options.read_pref_tags.len(), prefs.len());

            for (i, pref) in prefs.iter().enumerate().take(prefs.len() - 1) {
                assert_eq!(pref, &options.read_pref_tags[i]);
            }
        }
    }
}

#[test]
fn unix_domain_socket_single() {
    let uri = "mongodb:///tmp/mongodb-27017.sock/?safe=false";
    let connstr = connstring::parse(uri).unwrap();
    assert!(connstr.hosts[0].has_ipc());
    assert_eq!("/tmp/mongodb-27017.sock", connstr.hosts[0].ipc);
}

#[test]
fn unix_domain_socket_auth() {
    let uri = "mongodb://user:password@/tmp/mongodb-27017.sock/?safe=false";
    let connstr = connstring::parse(uri).unwrap();
    let options = connstr.options.unwrap();
    assert!(connstr.hosts[0].has_ipc());
    assert_eq!("/tmp/mongodb-27017.sock", connstr.hosts[0].ipc);
    assert_eq!("user", connstr.user.unwrap());
    assert_eq!("password", connstr.password.unwrap());
    assert_eq!("false", options.get("safe").unwrap());
}

#[test]
fn unix_domain_socket_replica_set() {
    let uri = "mongodb://user:password@/tmp/mongodb-27017.sock,/tmp/mongodb-27018.\
               sock/dbname?safe=false";
    let connstr = connstring::parse(uri).unwrap();
    let options = connstr.options.unwrap();
    assert!(connstr.hosts[0].has_ipc());
    assert!(connstr.hosts[1].has_ipc());
    assert_eq!("/tmp/mongodb-27017.sock", connstr.hosts[0].ipc);
    assert_eq!("/tmp/mongodb-27018.sock", connstr.hosts[1].ipc);
    assert_eq!("user", connstr.user.unwrap());
    assert_eq!("password", connstr.password.unwrap());
    assert_eq!("dbname", connstr.database.unwrap());
    assert_eq!("false", options.get("safe").unwrap());
}

#[test]
fn ipv6() {
    let uri = "mongodb://[::1]:27017/test";
    let connstr = connstring::parse(uri).unwrap();
    assert_eq!(1, connstr.hosts.len());
    assert_eq!("::1", connstr.hosts[0].host_name);
    assert_eq!(27017, connstr.hosts[0].port);
}

#[test]
fn full() {
    let opts = "?replicaSet=myreplset&journal=true&w=2&wtimeoutMS=50";
    let uri = format!("mongodb://u#ser:pas#s@local,remote:27018,japan:27019/rocksdb{}", opts);
    let connstr = connstring::parse(&uri).unwrap();
    assert_eq!("u#ser", connstr.user.unwrap());
    assert_eq!("pas#s", connstr.password.unwrap());
    assert_eq!("rocksdb", connstr.database.unwrap());
    assert_eq!(3, connstr.hosts.len());
    assert_eq!("local", connstr.hosts[0].host_name);
    assert_eq!(connstring::DEFAULT_PORT, connstr.hosts[0].port);
    assert_eq!("remote", connstr.hosts[1].host_name);
    assert_eq!(27018, connstr.hosts[1].port);
    assert_eq!("japan", connstr.hosts[2].host_name);
    assert_eq!(27019, connstr.hosts[2].port);

    let options = connstr.options.unwrap();
    assert_eq!("myreplset", options.get("replicaSet").unwrap());
    assert_eq!("true", options.get("journal").unwrap());
    assert_eq!("50", options.get("wtimeoutMS").unwrap());
}
