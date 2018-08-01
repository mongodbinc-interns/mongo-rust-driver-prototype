//! Operation flags.
use coll::options::{CursorType, FindOptions};

bitflags! {
    /// Represents the bit vector of options for an OP_REPLY message.
    pub struct OpReplyFlags: i32 {
        const CURSOR_NOT_FOUND  = 0b00000001;
        const QUERY_FAILURE     = 0b00000010;
        const AWAIT_CAPABLE     = 0b00001000;
    }
}

bitflags! {
    /// Represents the bit vector of options for an OP_UPDATE message.
    pub struct OpUpdateFlags: i32 {
        const UPSERT       = 0b00000001;
        const MULTI_UPDATE = 0b00000010;
    }
}

bitflags! {
    /// Represents the bit vector of flags for an OP_INSERT message.
    pub struct OpInsertFlags: i32 {
        const CONTINUE_ON_ERROR = 0b00000001;
    }
}

bitflags! {
    /// Represents the bit vector of flags for an OP_QUERY message.
    pub struct OpQueryFlags: i32 {
        const TAILABLE_CURSOR   = 0b00000010;
        const SLAVE_OK          = 0b00000100;
        const OPLOG_RELAY       = 0b00001000;
        const NO_CURSOR_TIMEOUT = 0b00010000;
        const AWAIT_DATA        = 0b00100000;
        const EXHAUST           = 0b01000000;
        const PARTIAL           = 0b10000000;
    }
}

impl OpQueryFlags {
    /// Constructs a new struct with flags based on a FindOptions struct.
    ///
    /// # Arguments
    ///
    /// options - Struct whose fields contain the flags to initialize the new
    ///           OpQueryFlags with
    ///
    /// # Return value
    ///
    /// Returns the newly created OpQueryFlags struct.
    pub fn with_find_options(options: &FindOptions) -> OpQueryFlags {
        let mut flags = OpQueryFlags::empty();

        if options.cursor_type != CursorType::NonTailable {
            flags.insert(Self::TAILABLE_CURSOR);
        }

        if options.oplog_replay {
            flags.insert(Self::OPLOG_RELAY);
        }

        if options.no_cursor_timeout {
            flags.insert(Self::NO_CURSOR_TIMEOUT);
        }

        if options.cursor_type == CursorType::TailableAwait {
            flags.insert(Self::AWAIT_DATA);
        }

        if options.allow_partial_results {
            flags.insert(Self::PARTIAL);
        }

        flags
    }
}
