//! Options for database-level commands.
use bson::Document;
use common::WriteConcern;
use db::roles::Role;

#[derive(Default)]
pub struct CreateCollectionOptions {
    pub capped: bool,
    pub size: Option<i64>,
    pub max: Option<i64>,
    pub use_power_of_two_sizes: bool,
    pub no_padding: bool,
}

impl CreateCollectionOptions {
    pub fn new() -> CreateCollectionOptions {
        CreateCollectionOptions {
            capped: false,
            size: None,
            max: None,
            use_power_of_two_sizes: true,
            no_padding: false,
        }
    }
}

#[derive(Default)]
pub struct CreateUserOptions {
    pub custom_data: Option<Document>,
    pub roles: Vec<Role>,
    pub write_concern: Option<WriteConcern>,
}

impl CreateUserOptions {
    pub fn new() -> CreateUserOptions {
        CreateUserOptions {
            custom_data: None,
            roles: vec![],
            write_concern: None,
        }
    }
}

#[derive(Default)]
pub struct UserInfoOptions {
    pub show_credentials: bool,
    pub show_privileges: bool,
}

impl UserInfoOptions {
    pub fn new() -> UserInfoOptions {
        UserInfoOptions {
            show_credentials: false,
            show_privileges: false,
        }
    }
}
