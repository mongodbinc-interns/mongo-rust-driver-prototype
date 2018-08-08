//! Options for database-level commands.
use bson::{Bson, Document};
use common::WriteConcern;
use db::roles::Role;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct CreateCollectionOptions {
    pub capped: Option<bool>,
    pub auto_index_id: Option<bool>,
    pub size: Option<i64>,
    pub max: Option<i64>,
    pub use_power_of_two_sizes: Option<bool>,
    pub no_padding: Option<bool>,
}

impl CreateCollectionOptions {
    pub fn new() -> CreateCollectionOptions {
        Default::default()
    }
}

impl From<CreateCollectionOptions> for Document {
    fn from(options: CreateCollectionOptions) -> Self {
        let mut document = Document::new();

        if let Some(capped) = options.capped {
            document.insert("capped", Bson::Boolean(capped));
        }

        if let Some(auto_index_id) = options.auto_index_id {
            document.insert("autoIndexId", Bson::Boolean(auto_index_id));
        }

        if let Some(size) = options.size {
            document.insert("size", Bson::I64(size));
        }

        if let Some(max) = options.max {
            document.insert("max", Bson::I64(max));
        }

        let mut flags = 0;

        if let Some(true) = options.use_power_of_two_sizes {
            flags |= 1;
        }

        if let Some(true) = options.no_padding {
            flags |= 2;
        }

        if flags != 0 {
            document.insert("flags", flags);
        }

        document
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
pub struct CreateUserOptions {
    pub custom_data: Option<Document>,
    pub roles: Vec<Role>,
    pub write_concern: Option<WriteConcern>,
}

impl CreateUserOptions {
    pub fn new() -> CreateUserOptions {
        Default::default()
    }
}

impl From<CreateUserOptions> for Document {
    fn from(options: CreateUserOptions) -> Self {
        let mut document = Document::new();

        if let Some(custom_data) = options.custom_data {
            document.insert("customData", Bson::Document(custom_data));
        }

        let roles_barr = options.roles.iter().map(Role::to_bson).collect();

        document.insert("roles", Bson::Array(roles_barr));

        if let Some(write_concern) = options.write_concern {
            document.insert("writeConcern", write_concern.to_bson());
        }

        document
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct UserInfoOptions {
    pub show_credentials: Option<bool>,
    pub show_privileges: Option<bool>,
}

impl UserInfoOptions {
    pub fn new() -> UserInfoOptions {
        Default::default()
    }
}

impl From<UserInfoOptions> for Document {
    fn from(options: UserInfoOptions) -> Self {
        let mut document = Document::new();

        if let Some(show_credentials) = options.show_credentials {
            document.insert("showCredentials", show_credentials);
        }

        if let Some(show_privileges) = options.show_privileges {
            document.insert("showPrivileges", show_privileges);
        }

        document
    }
}
