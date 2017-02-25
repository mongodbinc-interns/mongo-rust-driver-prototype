#[macro_use]
mod macros;

pub mod crud;
pub mod eq;
pub mod sdam;
pub mod server_selection;

use serde_json::Map;

pub trait FromJson: Sized {
    fn from_json(object: &Map<String, Value>) -> Self;
}

pub trait FromJsonResult: Sized {
    fn from_json(object: &Map<String, Value>) -> Result<Self, String>;
}
