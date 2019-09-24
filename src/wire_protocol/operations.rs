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
        let mut len = 5;
        for (key, value) in self {
            len += 1; // Type specifier
            len += key.len() as i32 + 1; // cstring
            len += value.byte_length()?;
        }
        Ok(len)
    }
}

impl ByteLength for bson::Bson {
    fn byte_length(&self) -> Result<i32> {
        Ok(match self {
            bson::Bson::Document(d) => {
                d.byte_length()?
            }
            bson::Bson::Array(a) => {
                let mut len = 5; // Total array byte size + array terminator
                for (mut i, elem) in a.iter().enumerate() {
                    // There's a faster way to calculate this up front.
                    len += 1;
                    while i >= 10 {
                        len += 1;
                        i /= 10;
                    }
                    len += 2;  // type tag + key cstring terminator
                    len += elem.byte_length()?;
                }
                len
            }
            bson::Bson::ObjectId(_) => 12,
            bson::Bson::Boolean(_) => 1,
            bson::Bson::UtcDatetime(_) => 8,
            bson::Bson::Null => 0,
            bson::Bson::Binary(_, data) => 5 + data.len() as i32,
            bson::Bson::RegExp(pat, opts) => pat.len() as i32 + opts.len() as i32 + 2,
            bson::Bson::I32(_) => 4,
            bson::Bson::I64(_) => 8,
            bson::Bson::TimeStamp(_) => 8,
            bson::Bson::String(s) => 5 + s.len() as i32,
            bson::Bson::Symbol(s) => 5 + s.len() as i32,
            bson::Bson::FloatingPoint(_) => 8,
            bson::Bson::JavaScriptCode(js) => 5 + js.len() as i32,
            bson::Bson::JavaScriptCodeWithScope(js, s) => 9 + js.len() as i32 + s.byte_length()?,
        })
    }
}

#[cfg(test)]
mod byte_length_test {
    use bson::{Bson, bson, doc};
    use super::ByteLength;

    #[test]
    fn test_scalar_lengths() {
        assert_eq!(Bson::I32(1i32).byte_length().unwrap(), 4);
        assert_eq!(Bson::I64(1i64).byte_length().unwrap(), 8);
        assert_eq!(Bson::FloatingPoint(2.0).byte_length().unwrap(), 8);
        assert_eq!(Bson::FloatingPoint(2.0).byte_length().unwrap(), 8);
        assert_eq!(Bson::Null.byte_length().unwrap(), 0);
        assert_eq!(Bson::Boolean(true).byte_length().unwrap(), 1);
        assert_eq!(Bson::ObjectId(bson::oid::ObjectId::new().unwrap()).byte_length().unwrap(), 12);
        assert_eq!(Bson::UtcDatetime("2012-06-02T01:12:00.000Z".parse().unwrap()).byte_length().unwrap(), 8);
        assert_eq!(Bson::TimeStamp(4).byte_length().unwrap(), 8);
    }

    #[test]
    fn test_string_length() {
        assert_eq!(Bson::String(String::from("hello")).byte_length().unwrap(), 10);
        assert_eq!(Bson::Symbol(String::from("hello")).byte_length().unwrap(), 10);
        assert_eq!(Bson::Binary(bson::spec::BinarySubtype::Generic, b"12345".to_vec()).byte_length().unwrap(), 10);
        assert_eq!(Bson::JavaScriptCode("alert(\"hello\");".into()).byte_length().unwrap(), 20);
        assert_eq!(Bson::JavaScriptCodeWithScope("alert(\"hello\");".into(), doc!{}).byte_length().unwrap(), 29);
    }

    #[test]
    fn test_document_lengths() {
        assert_eq!(doc!{}.byte_length().unwrap(), 5);
        assert_eq!(doc!{"answer": 42}.byte_length().unwrap(), 17);
    }

    #[test]
    fn test_array_lengths() {
        for (ct, expected_len) in vec![(16, 123), (110, 885)] {
            let arr = Bson::Array(
                (0..ct).into_iter()
                    .map(Bson::from)
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                arr.byte_length().unwrap(),
                expected_len,
            );
        }

    }

    #[test]
    fn test_complex_document() {
        let doc = bson!({ "isMaster": 1, "client": { "driver": { "name": "mongo-rust-driver-prototype", "version": "0.4.0" }, "os": { "type": "linux", "architecture": "x86_64" } } });
        assert_eq!(
            doc.byte_length().unwrap(),
            152,
        );
    }
}


/// Represents a message in the MongoDB Wire Protocol.
#[derive(Debug, Clone, PartialEq)]
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

        let selector_length = selector.byte_length()?;
        let update_length = update.byte_length()?;

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
            total_length += doc.byte_length()?;
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

        let bson_length = query.byte_length()?;

        // Add the length of the optional BSON document only if it exists.
        let option_length = match return_field_selector {
            Some(ref bson) => bson.byte_length()?,
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
        let mut temp_buffer = Vec::new();

        bson::encode_document(&mut temp_buffer, bson)?;
        buffer.write_all(&temp_buffer)?;

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

        header.write(buffer)?;

        // Write ZERO field
        buffer.write_i32::<LittleEndian>(0)?;

        for byte in namespace.bytes() {
            buffer.write_u8(byte)?;
        }

        // Writes the null terminator for the collection name string.
        buffer.write_u8(0)?;

        buffer.write_i32::<LittleEndian>(flags.bits())?;

        Message::write_bson_document(buffer, selector)?;
        Message::write_bson_document(buffer, update)?;

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

        header.write(buffer)?;
        buffer.write_i32::<LittleEndian>(flags.bits())?;

        for byte in namespace.bytes() {
            buffer.write_u8(byte)?;
        }

        // Writes the null terminator for the collection name string.
        buffer.write_u8(0)?;

        for doc in documents {
            Message::write_bson_document(buffer, doc)?;
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

        header.write(buffer)?;
        buffer.write_i32::<LittleEndian>(flags.bits())?;

        for byte in namespace.bytes() {
            buffer.write_u8(byte)?;
        }

        // Writes the null terminator for the collection name string.
        buffer.write_u8(0)?;

        buffer.write_i32::<LittleEndian>(number_to_skip)?;
        buffer.write_i32::<LittleEndian>(number_to_return)?;
        Message::write_bson_document(buffer, query)?;

        if let Some(ref doc) = *return_field_selector {
            Message::write_bson_document(buffer, doc)?;
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

        header.write(buffer)?;

        // Write ZERO field
        buffer.write_i32::<LittleEndian>(0)?;

        for byte in namespace.bytes() {
            buffer.write_u8(byte)?;
        }

        // Writes the null terminator for the collection name string.
        buffer.write_u8(0)?;

        buffer.write_i32::<LittleEndian>(number_to_return)?;
        buffer.write_i64::<LittleEndian>(cursor_id)?;

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
        let flags = buffer.read_i32::<LittleEndian>()?;
        length -= mem::size_of::<i32>() as i32;

        // Read cursor_id
        let cid = buffer.read_i64::<LittleEndian>()?;
        length -= mem::size_of::<i64>() as i32;

        // Read starting_from
        let sf = buffer.read_i32::<LittleEndian>()?;
        length -= mem::size_of::<i32>() as i32;

        // Read number_returned
        let nr = buffer.read_i32::<LittleEndian>()?;
        length -= mem::size_of::<i32>() as i32;

        let mut v = Vec::new();

        while length > 0 {
            let bson = bson::decode_document(buffer)?;
            length -= bson.byte_length()?;
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
        let header = Header::read(buffer)?;
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
