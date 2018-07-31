//! Role-based database and command authorization.
use std::string::ToString;

use bson::Bson;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SingleDatabaseRole {
    Read,
    ReadWrite,
    DbAdmin,
    DbOwner,
    UserAdmin,
    ClusterAdmin,
    ClusterManager,
    ClusterMonitor,
    HostManager,
    Backup,
    Restore,
}

impl SingleDatabaseRole {
    fn to_str(&self) -> &'static str {
        match *self {
            SingleDatabaseRole::Read => "read",
            SingleDatabaseRole::ReadWrite => "readWrite",
            SingleDatabaseRole::DbAdmin => "dbAdmin",
            SingleDatabaseRole::DbOwner => "dbOwner",
            SingleDatabaseRole::UserAdmin => "userAdmin",
            SingleDatabaseRole::ClusterAdmin => "clusterAdmin",
            SingleDatabaseRole::ClusterManager => "clusterManager",
            SingleDatabaseRole::ClusterMonitor => "clusterMonitor",
            SingleDatabaseRole::HostManager => "hostManager",
            SingleDatabaseRole::Backup => "backup",
            SingleDatabaseRole::Restore => "restore",
        }
    }
}

impl ToString for SingleDatabaseRole {
    fn to_string(&self) -> String {
        self.to_str().into()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AllDatabaseRole {
    Read,
    ReadWrite,
    UserAdmin,
    DbAdmin,
}

impl AllDatabaseRole {
    fn to_str(&self) -> &'static str {
        match *self {
            AllDatabaseRole::Read => "read",
            AllDatabaseRole::ReadWrite => "readWrite",
            AllDatabaseRole::UserAdmin => "userAdmin",
            AllDatabaseRole::DbAdmin => "dbAdmin",
        }
    }
}

impl ToString for AllDatabaseRole {
    fn to_string(&self) -> String {
        self.to_str().into()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Role {
    All(AllDatabaseRole),
    Single {
        role: SingleDatabaseRole,
        db: String,
    },
}

impl From<Role> for Bson {
    fn from(role: Role) -> Bson {
        match role {
            Role::All(role) => role.to_string().into(),
            Role::Single { role, db } => {
                Bson::Document(doc! {
                  "role": role.to_string(),
                  "db": db
              })
            }
        }
    }
}

impl Role {
    pub fn to_bson(&self) -> Bson {
        self.clone().into()
    }

    #[deprecated(since = "0.2.4", note = "this method will be removed in the next major release")]
    pub fn to_bson_array(vec: Vec<Role>) -> Bson {
        Bson::Array(vec.iter().map(Self::to_bson).collect())
    }
}
