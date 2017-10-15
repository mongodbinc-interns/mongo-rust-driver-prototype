//! Wire protocol operational client-server communication logic.
use bson;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use Error::{ArgumentError, ResponseError};
use Result;
use wire_protocol::header::{Header, OpCode};
use wire_protocol::flags::{OpInsertFlags, OpQueryFlags, OpReplyFlags, OpUpdateFlags};

use std::io::{Read, Write};
use std::mem;
use std::result::Result::{Ok, Err};

trait ByteLength {
    /// Calculates the number of bytes in the serialized version of the struct.
    fn byte_length(&self) -> Result<i32>;
}

impl ByteLength for bson::Document {
    /// Gets the length of a BSON document.
    ///
    /// # Return value
    ///
    /// Returns the number of bytes in the serialized BSON document, or an
    /// Error if the document couldn't be serialized.
    fn byte_length(&self) -> Result<i32> {
        let mut temp_buffer = vec![];

        try!(bson::encode_document(&mut temp_buffer, self));
        Ok(temp_buffer.len() as i32)
    }
}

/// Represents a message in the MongoDB Wire Protocol.
pub enum Message {
    OpReply {
        /// The message header.
        header: Header,
        /// A Bit vector of reply options.
        flags: OpReplyFlags,
        /// Uniquely identifies the cursor being returned.
        cursor_id: i64,
        /// The starting position for the cursor.
        starting_from: i32,
        /// The total number of documents being returned.
        number_returned: i32,
        /// The documents being returned.
        documents: Vec<bson::Document>,
    },
    OpUpdate {
        /// The message header.
        header: Header,
        // The wire protocol specifies that a 32-bit 0 field goes here
        /// The full qualified name of the collection, beginning with the
        /// database name and a dot separator.
        namespace: String,
        /// A bit vector of update options.
        flags: OpUpdateFlags,
        /// Identifies the document(s) to be updated.
        selector: bson::Document,
        /// Instruction document for how to update the document(s).
        update: bson::Document,
    },
    OpInsert {
        /// The message header.
        header: Header,
        /// A bit vector of insert options.
        flags: OpInsertFlags,
        /// The full qualified name of the collection, beginning with the
        /// database name and a dot separator.
        namespace: String,
        /// The documents to be inserted.
        documents: Vec<bson::Document>,
    },
    OpQuery {
        /// The message header.
        header: Header,
        /// A bit vector of query options.
        flags: OpQueryFlags,
        /// The full qualified name of the collection, beginning with the
        /// database name and a dot separator.
        namespace: String,
        /// The number of initial documents to skip over in the query results.
        number_to_skip: i32,
        /// The total number of documents that should be returned by the query.
        number_to_return: i32,
        /// Specifies which documents to return.
        query: bson::Document,
        /// An optional projection of which fields should be present in the
        /// documents to be returned by the query.
        return_field_selector: Option<bson::Document>,
    },
    OpGetMore {
        /// The message header.
        header: Header,
        // The wire protocol specifies that a 32-bit 0 field goes here
        /// The full qualified name of the collection, beginning with the
        /// database name and a dot separator.
        namespace: String,
        /// The total number of documents that should be returned by the query.
        number_to_return: i32,
        /// Uniquely identifies the cursor being returned.
        cursor_id: i64,
    },
}

impl Message {
    /// Constructs a new message for a reply.
    fn new_reply(
        header: Header,
        flags: i32,
        cursor_id: i64,
        starting_from: i32,
        number_returned: i32,
        documents: Vec<bson::Document>,
    ) -> Message {
        Message::OpReply {
            header: header,
            flags: OpReplyFlags::from_bits_truncate(flags),
            cursor_id: cursor_id,
            starting_from: starting_from,
            number_returned: number_returned,
            documents: documents,
        }
    }

    /// Constructs a new message for an update.
    pub fn new_update(
        request_id: i32,
        namespace: String,
        flags: OpUpdateFlags,
        selector: bson::Document,
        update: bson::Document,
    ) -> Result<Message> {
        let header_length = mem::size_of::<Header>() as i32;

        // Add an extra byte after the string for null-termination.
        let string_length = namespace.len() as i32 + 1;

        // There are two i32 fields -- `flags` is represented in the struct as
        // a bit vector, and the wire protocol-specified ZERO field.
        let i32_length = mem::size_of::<i32>() as i32 * 2;

        let selector_length = try!(selector.byte_length());
        let update_length = try!(update.byte_length());

        let total_length = header_length + string_length + i32_length + selector_length +
            update_length;

        let header = Header::new_update(total_length, request_id);

        Ok(Message::OpUpdate {
            header: header,
            namespace: namespace,
            flags: flags,
            selector: selector,
            update: update,
        })
    }

    /// Constructs a new message request for an insertion.
    pub fn new_insert(
        request_id: i32,
        flags: OpInsertFlags,
        namespace: String,
        documents: Vec<bson::Document>,
    ) -> Result<Message> {
        let header_length = mem::size_of::<Header>() as i32;
        let flags_length = mem::size_of::<i32>() as i32;

        // Add an extra byte after the string for null-termination.
        let string_length = namespace.len() as i32 + 1;

        let mut total_length = header_length + flags_length + string_length;

        for doc in &documents {
            total_length += try!(doc.byte_length());
        }

        let header = Header::new_insert(total_length, request_id);

        Ok(Message::OpInsert {
            header: header,
            flags: flags,
            namespace: namespace,
            documents: documents,
        })
    }

    /// Constructs a new message request for a query.
    pub fn new_query(
        request_id: i32,
        flags: OpQueryFlags,
        namespace: String,
        number_to_skip: i32,
        number_to_return: i32,
        query: bson::Document,
        return_field_selector: Option<bson::Document>,
    ) -> Result<Message> {

        let header_length = mem::size_of::<Header>() as i32;

        // There are three i32 fields in the an OpQuery (since OpQueryFlags is
        // represented as an 32-bit vector in the wire protocol).
        let i32_length = 3 * mem::size_of::<i32>() as i32;

        // Add an extra byte after the string for null-termination.
        let string_length = namespace.len() as i32 + 1;

        let bson_length = try!(query.byte_length());

        // Add the length of the optional BSON document only if it exists.
        let option_length = match return_field_selector {
            Some(ref bson) => try!(bson.byte_length()),
            None => 0,
        };

        let total_length = header_length + i32_length + string_length + bson_length + option_length;

        let header = Header::new_query(total_length, request_id);

        Ok(Message::OpQuery {
            header: header,
            flags: flags,
            namespace: namespace,
            number_to_skip: number_to_skip,
            number_to_return: number_to_return,
            query: query,
            return_field_selector: return_field_selector,
        })
    }

    /// Constructs a new "get more" request message.
    pub fn new_get_more(
        request_id: i32,
        namespace: String,
        number_to_return: i32,
        cursor_id: i64,
    ) -> Message {
        let header_length = mem::size_of::<Header>() as i32;

        // There are two i32 fields because of the reserved "ZERO".
        let i32_length = 2 * mem::size_of::<i32>() as i32;

        // Add an extra byte after the string for null-termination.
        let string_length = namespace.len() as i32 + 1;

        let i64_length = mem::size_of::<i64>() as i32;
        let total_length = header_length + i32_length + string_length + i64_length;

        let header = Header::new_get_more(total_length, request_id);

        Message::OpGetMore {
            header: header,
            namespace: namespace,
            number_to_return: number_to_return,
            cursor_id: cursor_id,
        }
    }

    /// Writes a serialized BSON document to a given buffer.
    ///
    /// # Arguments
    ///
    /// `buffer` - The buffer to write to.
    /// `bson` - The document to serialize and write.
    ///
    /// # Return value
    ///
    /// Returns nothing on success, or an Error on failure.
    fn write_bson_document<W: Write>(buffer: &mut W, bson: &bson::Document) -> Result<()> {
        let mut temp_buffer = vec![];

        try!(bson::encode_document(&mut temp_buffer, bson));
        try!(buffer.write_all(&temp_buffer));
        Ok(())
    }

    /// Writes a serialized update message to a given buffer.
    ///
    /// # Arguments
    ///
    /// `buffer` - The buffer to write to.
    /// `header` - The header for the given message.
    /// `namespace` - The full qualified name of the collection, beginning with
    ///               the database name and a dot.
    /// `flags` - Bit vector of query option.
    /// `selector` - Identifies the document(s) to be updated.
    /// `update` - Instructs how to update the document(s).
    ///
    /// # Return value
    ///
    /// Returns nothing on success, or an Error on failure.
    pub fn write_update<W: Write>(
        buffer: &mut W,
        header: &Header,
        namespace: &str,
        flags: &OpUpdateFlags,
        selector: &bson::Document,
        update: &bson::Document,
    ) -> Result<()> {

        try!(header.write(buffer));

        // Write ZERO field
        try!(buffer.write_i32::<LittleEndian>(0));

        for byte in namespace.bytes() {
            try!(buffer.write_u8(byte));
        }

        // Writes the null terminator for the collection name string.
        try!(buffer.write_u8(0));

        try!(buffer.write_i32::<LittleEndian>(flags.bits()));

        try!(Message::write_bson_document(buffer, selector));
        try!(Message::write_bson_document(buffer, update));

        let _ = buffer.flush();
        Ok(())
    }

    /// Writes a serialized update message to a given buffer.
    ///
    /// # Arguments
    ///
    /// `buffer` - The buffer to write to.
    /// `header` - The header for the given message.
    /// `flags` - Bit vector of query options.
    /// `namespace` - The full qualified name of the collection, beginning with
    ///               the database name and a dot.
    /// `documents` - The documents to insert.
    ///
    /// # Return value
    ///
    /// Returns nothing on success, or an Error on failure.
    fn write_insert<W: Write>(
        buffer: &mut W,
        header: &Header,
        flags: &OpInsertFlags,
        namespace: &str,
        documents: &[bson::Document],
    ) -> Result<()> {

        try!(header.write(buffer));
        try!(buffer.write_i32::<LittleEndian>(flags.bits()));

        for byte in namespace.bytes() {
            try!(buffer.write_u8(byte));
        }

        // Writes the null terminator for the collection name string.
        try!(buffer.write_u8(0));

        for doc in documents {
            try!(Message::write_bson_document(buffer, doc));
        }

        let _ = buffer.flush();
        Ok(())
    }

    /// Writes a serialized query message to a given buffer.
    ///
    /// # Arguments
    ///
    /// `buffer` - The buffer to write to.
    /// `header` - The header for the given message.
    /// `flags` - Bit vector of query option.
    /// `namespace` - The full qualified name of the collection, beginning with
    ///               the database name and a dot.
    /// `number_to_skip` - The number of initial documents to skip over in the
    ///                    query results.
    /// `number_to_return - The total number of documents that should be
    ///                     returned by the query.
    /// `query` - Specifies which documents to return.
    /// `return_field_selector - An optional projection of which fields should
    ///                          be present in the documents to be returned by
    ///                          the query.
    ///
    /// # Return value
    ///
    /// Returns nothing on success, or an Error on failure.
    fn write_query<W: Write>(
        buffer: &mut W,
        header: &Header,
        flags: &OpQueryFlags,
        namespace: &str,
        number_to_skip: i32,
        number_to_return: i32,
        query: &bson::Document,
        return_field_selector: &Option<bson::Document>,
    ) -> Result<()> {

        try!(header.write(buffer));
        try!(buffer.write_i32::<LittleEndian>(flags.bits()));

        for byte in namespace.bytes() {
            try!(buffer.write_u8(byte));
        }

        // Writes the null terminator for the collection name string.
        try!(buffer.write_u8(0));

        try!(buffer.write_i32::<LittleEndian>(number_to_skip));
        try!(buffer.write_i32::<LittleEndian>(number_to_return));
        try!(Message::write_bson_document(buffer, query));

        if let Some(ref doc) = *return_field_selector {
            try!(Message::write_bson_document(buffer, doc));
        }

        let _ = buffer.flush();
        Ok(())
    }

    /// Writes a serialized "get more" request to a given buffer.
    ///
    /// # Arguments
    ///
    /// `buffer` - The buffer to write to.
    /// `header` - The header for the given message.
    /// `namespace` - The full qualified name of the collection, beginning with
    ///               the database name and a dot.
    /// `number_to_return - The total number of documents that should be
    ///                     returned by the query.
    /// `cursor_id` - Specifies which cursor to get more documents from.
    ///
    /// # Return value
    ///
    /// Returns nothing on success, or an Error on failure.
    pub fn write_get_more<W: Write>(
        buffer: &mut W,
        header: &Header,
        namespace: &str,
        number_to_return: i32,
        cursor_id: i64,
    ) -> Result<()> {

        try!(header.write(buffer));

        // Write ZERO field
        try!(buffer.write_i32::<LittleEndian>(0));

        for byte in namespace.bytes() {
            try!(buffer.write_u8(byte));
        }

        // Writes the null terminator for the collection name string.
        try!(buffer.write_u8(0));

        try!(buffer.write_i32::<LittleEndian>(number_to_return));
        try!(buffer.write_i64::<LittleEndian>(cursor_id));

        let _ = buffer.flush();
        Ok(())
    }

    /// Attemps to write the serialized message to a buffer.
    ///
    /// # Arguments
    ///
    /// `buffer` - The buffer to write to.
    ///
    /// # Return value
    ///
    /// Returns nothing on success, or an error string on failure.
    pub fn write<W: Write>(&self, buffer: &mut W) -> Result<()> {
        match *self {
            // Only the server should send replies
            Message::OpReply { .. } => {
                Err(ArgumentError(
                    String::from("OP_REPLY should not be sent to the client."),
                ))
            }
            Message::OpUpdate {
                ref header,
                ref namespace,
                ref flags,
                ref selector,
                ref update,
            } => Message::write_update(buffer, header, namespace, flags, selector, update),
            Message::OpInsert {
                ref header,
                ref flags,
                ref namespace,
                ref documents,
            } => Message::write_insert(buffer, header, flags, namespace, documents),
            Message::OpQuery {
                ref header,
                ref flags,
                ref namespace,
                number_to_skip,
                number_to_return,
                ref query,
                ref return_field_selector,
            } => {
                Message::write_query(
                    buffer,
                    header,
                    flags,
                    namespace,
                    number_to_skip,
                    number_to_return,
                    query,
                    return_field_selector,
                )
            }
            Message::OpGetMore {
                ref header,
                ref namespace,
                number_to_return,
                cursor_id,
            } => Message::write_get_more(buffer, header, namespace, number_to_return, cursor_id),
        }
    }

    /// Reads a serialized reply message from a buffer
    ///
    /// # Arguments
    ///
    /// `buffer` - The buffer to read from.
    ///
    /// # Return value
    ///
    /// Returns the reply message on success, or an Error on failure.
    fn read_reply<R: Read>(buffer: &mut R, header: Header) -> Result<Message> {
        let mut length = header.message_length - mem::size_of::<Header>() as i32;

        // Read flags
        let flags = try!(buffer.read_i32::<LittleEndian>());
        length -= mem::size_of::<i32>() as i32;

        // Read cursor_id
        let cid = try!(buffer.read_i64::<LittleEndian>());
        length -= mem::size_of::<i64>() as i32;

        // Read starting_from
        let sf = try!(buffer.read_i32::<LittleEndian>());
        length -= mem::size_of::<i32>() as i32;

        // Read number_returned
        let nr = try!(buffer.read_i32::<LittleEndian>());
        length -= mem::size_of::<i32>() as i32;

        let mut v = vec![];

        while length > 0 {
            let bson = try!(bson::decode_document(buffer));
            length -= try!(bson.byte_length());
            v.push(bson);
        }

        Ok(Message::new_reply(header, flags, cid, sf, nr, v))
    }

    /// Attempts to read a serialized reply Message from a buffer.
    ///
    /// # Arguments
    ///
    /// `buffer` - The buffer to read from.
    ///
    /// # Return value
    ///
    /// Returns the reply message on success, or an Error on failure.
    pub fn read<T>(buffer: &mut T) -> Result<Message>
    where
        T: Read + Write,
    {
        let header = try!(Header::read(buffer));
        match header.op_code {
            OpCode::Reply => Message::read_reply(buffer, header),
            opcode => {
                Err(ResponseError(format!(
                    "Expected to read OpCode::Reply but instead found \
                                           opcode {}",
                    opcode
                )))
            }
        }
    }
}
