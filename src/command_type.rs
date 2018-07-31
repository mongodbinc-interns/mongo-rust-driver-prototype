//! Monitorable command types.

/// Executable command types that can be monitored by the driver.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub enum CommandType {
    Aggregate,
    BuildInfo,
    Count,
    CreateCollection,
    CreateIndexes,
    CreateUser,
    DeleteMany,
    DeleteOne,
    Distinct,
    DropAllUsers,
    DropCollection,
    DropDatabase,
    DropIndexes,
    DropUser,
    Find,
    FindOneAndDelete,
    FindOneAndReplace,
    FindOneAndUpdate,
    GetUser,
    GetUsers,
    InsertMany,
    InsertOne,
    IsMaster,
    ListCollections,
    ListDatabases,
    ListIndexes,
    Suppressed,
    UpdateMany,
    UpdateOne,
}

impl CommandType {
    pub fn to_str(&self) -> &str {
        match *self {
            CommandType::Aggregate => "aggregate",
            CommandType::BuildInfo => "buildinfo",
            CommandType::Count => "count",
            CommandType::CreateCollection => "create_collection",
            CommandType::CreateIndexes => "create_indexes",
            CommandType::CreateUser => "create_user",
            CommandType::DeleteMany => "delete_many",
            CommandType::DeleteOne => "delete_one",
            CommandType::Distinct => "distinct",
            CommandType::DropAllUsers => "drop_all_users",
            CommandType::DropCollection => "drop_collection",
            CommandType::DropDatabase => "drop_database",
            CommandType::DropIndexes => "drop_indexes",
            CommandType::DropUser => "drop_user",
            CommandType::Find => "find",
            CommandType::FindOneAndDelete => "find_one_and_delete",
            CommandType::FindOneAndReplace => "find_one_and_replace",
            CommandType::FindOneAndUpdate => "find_one_and_update",
            CommandType::GetUser => "get_user",
            CommandType::GetUsers => "get_users",
            CommandType::InsertMany => "insert_many",
            CommandType::InsertOne => "insert_one",
            CommandType::IsMaster => "is_master",
            CommandType::ListCollections => "list_collections",
            CommandType::ListDatabases => "list_databases",
            CommandType::ListIndexes => "list_indexes",
            CommandType::Suppressed => "suppressed",
            CommandType::UpdateMany => "update_many",
            CommandType::UpdateOne => "update_one",
        }
    }

    pub fn is_write_command(&self) -> bool {
        match *self {
            CommandType::CreateCollection |
            CommandType::CreateIndexes |
            CommandType::CreateUser |
            CommandType::DeleteMany |
            CommandType::DeleteOne |
            CommandType::DropAllUsers |
            CommandType::DropCollection |
            CommandType::DropDatabase |
            CommandType::DropIndexes |
            CommandType::DropUser |
            CommandType::FindOneAndDelete |
            CommandType::FindOneAndReplace |
            CommandType::FindOneAndUpdate |
            CommandType::InsertMany |
            CommandType::InsertOne |
            CommandType::UpdateMany |
            CommandType::UpdateOne => true,
            CommandType::Aggregate |
            CommandType::BuildInfo |
            CommandType::Count |
            CommandType::Distinct |
            CommandType::Find |
            CommandType::GetUser |
            CommandType::GetUsers |
            CommandType::IsMaster |
            CommandType::ListCollections |
            CommandType::ListDatabases |
            CommandType::ListIndexes |
            CommandType::Suppressed => false,
        }
    }
}
