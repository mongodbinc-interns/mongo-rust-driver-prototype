//! Library-wide utilities.
use Error::{self, ArgumentError};
use Result;

use bson::{self, Bson};
use std::collections::BTreeMap;
use std::str::FromStr;

/// Indicates how a server should be selected during read operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReadMode {
    Primary,
    PrimaryPreferred,
    Secondary,
    SecondaryPreferred,
    Nearest,
}

impl FromStr for ReadMode {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "Primary" => ReadMode::Primary,
            "PrimaryPreferred" => ReadMode::PrimaryPreferred,
            "Secondary" => ReadMode::Secondary,
            "SecondaryPreferred" => ReadMode::SecondaryPreferred,
            "Nearest" => ReadMode::Nearest,
            _ => {
                return Err(ArgumentError(
                    format!("Could not convert '{}' to ReadMode.", s),
                ))
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReadPreference {
    /// Indicates how a server should be selected during read operations.
    pub mode: ReadMode,
    /// Filters servers based on the first tag set that matches at least one server.
    pub tag_sets: Vec<BTreeMap<String, String>>,
}

impl ReadPreference {
    pub fn new(mode: ReadMode, tag_sets: Option<Vec<BTreeMap<String, String>>>) -> ReadPreference {
        ReadPreference {
            mode: mode,
            tag_sets: tag_sets.unwrap_or_else(Vec::new),
        }
    }

    pub fn to_document(&self) -> bson::Document {
        let mut doc = doc! { "mode": stringify!(self.mode).to_ascii_lowercase() };
        let bson_tag_sets: Vec<_> = self.tag_sets
            .iter()
            .map(|map| {
                let mut bson_map = bson::Document::new();
                for (key, val) in map.iter() {
                    bson_map.insert(&key[..], Bson::String(val.to_owned()));
                }
                Bson::Document(bson_map)
            })
            .collect();

        doc.insert("tag_sets", Bson::Array(bson_tag_sets));
        doc
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WriteConcern {
    /// Write replication
    pub w: i32,
    /// Used in conjunction with 'w'. Propagation timeout in ms.
    pub w_timeout: i32,
    /// If true, will block until write operations have been committed to journal.
    pub j: bool,
    /// If true and server is not journaling, blocks until server has synced all data files to disk.
    pub fsync: bool,
}

impl WriteConcern {
    pub fn new() -> WriteConcern {
        WriteConcern {
            w: 1,
            w_timeout: 0,
            j: false,
            fsync: false,
        }
    }

    pub fn to_bson(&self) -> bson::Document {
        doc! {
            "w": self.w,
            "wtimeout": self.w_timeout,
            "j": self.j,
        }
    }
}

impl Default for WriteConcern {
    fn default() -> Self {
        WriteConcern::new()
    }
}

pub fn merge_options<T: Into<bson::Document>>(
    document: bson::Document,
    options: T,
) -> bson::Document {
    let options_doc: bson::Document = options.into();
    document
        .into_iter()
        .chain(options_doc.into_iter())
        .collect()
}
