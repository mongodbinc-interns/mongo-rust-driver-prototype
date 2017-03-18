//! Role-based database and command authorization.
use std::string::ToString;

use bson::Bson;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

impl ToString for SingleDatabaseRole {
    fn to_string(&self) -> String {
        let string = match *self {
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
        };

        String::from(string)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AllDatabaseRole {
    Read,
    ReadWrite,
    UserAdmin,
    DbAdmin,
}

impl ToString for AllDatabaseRole {
    fn to_string(&self) -> String {
        let string = match *self {
            AllDatabaseRole::Read => "read",
            AllDatabaseRole::ReadWrite => "readWrite",
            AllDatabaseRole::UserAdmin => "userAdmin",
            AllDatabaseRole::DbAdmin => "dbAdmin",
        };

        String::from(string)
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
            Role::All(role) => Bson::String(role.to_string()),
            Role::Single { role, db } => {
              Bson::Document(doc! {
                  "role" => (Bson::String(role.to_string())),
                  "db" => (Bson::String(db))
              })
            }
        }
    }
}

impl Role {
    pub fn to_bson(&self) -> Bson {
        self.clone().into()
    }

    #[deprecated(since="0.2.4", note="this method will be removed in the next major release")]
    pub fn to_bson_array(vec: Vec<Role>) -> Bson {
        Bson::Array(vec.into_iter().map(|r| r.to_bson()).collect())
    }
}
