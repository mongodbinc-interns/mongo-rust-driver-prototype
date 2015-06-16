use bson;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use client::wire_protocol::flags::{OpInsertFlags, OpQueryFlags, OpReplyFlags,
                                   OpUpdateFlags};
use client::wire_protocol::header::{Header, OpCode};
use std::io::{Read, Write};
use std::mem;
use std::result::Result::{Ok, Err};

trait ByteLength {
    /// Calculates the number of bytes in the serialized version of the struct.
    fn byte_length(&self) -> Result<i32, String>;
}

impl ByteLength for bson::Document {
    /// Gets the length of a BSON document.
    ///
    /// # Return value
    ///
    /// Returns the number of bytes in the serialized BSON document.
    fn byte_length(&self) -> Result<i32, String> {
        let mut temp_buffer = vec![];

        match bson::encode_document(&mut temp_buffer, self) {
            Ok(_) => Ok(temp_buffer.len() as i32),
            Err(_) => Err("unable to serialize BSON document".to_owned())
        }
    }
}

/// Represents a message in the MongoDB Wire Protocol.
pub enum Message {
    OpReply {
        header: Header,
        flags: OpReplyFlags,
        cursor_id: i64,
        starting_from: i32,
        number_returned: i32,
        documents: Vec<bson::Document>,
    },
    OpUpdate {
        header: Header,
        // The wire protocol specifies that 32-bit 0 goes here
        namespace: String,
        flags: OpUpdateFlags,
        selector: bson::Document,
        update: bson::Document,
    },
    OpInsert {
        header: Header,
        flags: OpInsertFlags,
        namespace: String,
        documents: Vec<bson::Document>,
    },
    OpQuery {
        header: Header,
        flags: OpQueryFlags,
        namespace: String,
        number_to_skip: i32,
        number_to_return: i32,
        query: bson::Document,
        return_field_selector: Option<bson::Document>,
    },
    OpGetMore {
        header: Header,
        // The wire protocol specifies that 32-bit 0 goes here
        namespace: String,
        number_to_return: i32,
        cursor_id: i64,
    }
}

impl Message {
    fn with_reply(header: Header, flags: i32, cursor_id: i64,
                  starting_from: i32, number_returned: i32,
                  documents: Vec<bson::Document>) -> Message {
        Message::OpReply { header: header, flags: OpReplyFlags::from_i32(flags),
                           cursor_id: cursor_id, starting_from: starting_from,
                           number_returned: number_returned,
                           documents: documents }
    }

    pub fn with_update(request_id: i32, namespace: String,
                       flags: OpUpdateFlags, selector: bson::Document,
                       update: bson::Document) -> Result<Message, String> {
        let header_length = mem::size_of::<Header>() as i32;

        // Add an extra byte after the string for null-termination.
        let string_length = namespace.len() as i32 + 1;

        // There are two i32 fields -- `flags` is represented in the struct as
        // a bit vector, and the wire protocol-specified ZERO field.
        let i32_length = mem::size_of::<i32>() as i32 * 2;

        let selector_length = match selector.byte_length() {
            Ok(i) => i,
            Err(_) =>
                return Err("Unable to serialize `selector` field".to_owned())
        };

        let update_length = match update.byte_length() {
            Ok(i) => i,
            Err(_) =>
                return Err("Unable to serialize `update` field".to_owned())
        };

        let total_length = header_length + string_length + i32_length +
                           selector_length + update_length;

        let header = Header::with_update(total_length, request_id);

        Ok(Message::OpUpdate { header: header,
                               namespace: namespace,
                               flags: flags, selector: selector,
                               update: update })
   }

    pub fn with_insert(request_id: i32, flags: OpInsertFlags,
                       namespace: String,
                       documents: Vec<bson::Document>) -> Result<Message, String> {
        let header_length = mem::size_of::<Header>() as i32;
        let flags_length = mem::size_of::<i32>() as i32;

        // Add an extra byte after the string for null-termination.
        let string_length = namespace.len() as i32 + 1;

        let mut total_length = header_length + flags_length + string_length;

        for bson in documents.iter() {
            total_length += match bson.byte_length() {
                Ok(i) => i,
                Err(_) => return Err("Unable to serialize documents".to_owned())
            }
        }

        let header = Header::with_insert(total_length, request_id);

        Ok(Message::OpInsert { header: header, flags: flags,
                               namespace: namespace,
                               documents: documents })
    }

    /// Constructs a new message request for a query.
    ///
    /// # Arguments
    ///
    /// `header_request_id` - The request ID to be placed in the message header.
    /// `flags` - Bit vector of query options.
    /// `namespace` - The full qualified name of the collection, beginning with
    ///               the database name and a dot.
    /// `number_to_skip` - The number of initial documents to skip over in the query
    ///                    results.
    /// `number_to_return - The total number of documents that should be returned by
    ///                     the query.
    /// `return_field_selector - An optional projection of which fields should be
    ///                          present in the documents to be returned by the
    ///                          query.
    ///
    /// # Return value
    ///
    /// Returns the newly-created Message.
    pub fn with_query(request_id: i32, flags: OpQueryFlags,
                     namespace: String, number_to_skip: i32,
                     number_to_return: i32, query: bson::Document,
                     return_field_selector: Option<bson::Document>) -> Result<Message, String> {
        let header_length = mem::size_of::<Header>() as i32;

        // There are three i32 fields in the an OpQuery (since OpQueryFlags is
        // represented as an 32-bit vector in the wire protocol).
        let i32_length = 3 * mem::size_of::<i32>() as i32;

        // Add an extra byte after the string for null-termination.
        let string_length = namespace.len() as i32 + 1;

        let bson_length = match query.byte_length() {
            Ok(i) => i,
            Err(_) => return Err("Unable to serialize query".to_owned())
        };

        // Add the length of the optional BSON document only if it exists.
        let option_length = match return_field_selector {
            Some(ref bson) => match bson.byte_length() {
                Ok(i) => i,
                Err(_) => return Err("Unable to serialize return_field_selector".to_owned())
            },
            None => 0
        };

        let total_length = header_length + i32_length + string_length +
                           bson_length + option_length;

        let header = Header::with_query(total_length, request_id);

        Ok(Message::OpQuery { header: header, flags: flags,
                              namespace: namespace,
                              number_to_skip: number_to_skip,
                              number_to_return: number_to_return, query: query,
                              return_field_selector: return_field_selector })
    }

    pub fn with_get_more(request_id: i32, namespace: String,
                         number_to_return: i32, cursor_id: i64) -> Message {
        let header_length = mem::size_of::<Header>() as i32;

        // There are two i32 fields because of the reserved "ZERO".
        let i32_length = 2 * mem::size_of::<i32>() as i32;

        // Add an extra byte after the string for null-termination.
        let string_length = namespace.len() as i32 + 1;

        let i64_length = mem::size_of::<i64>() as i32;
        let total_length = header_length + i32_length + string_length +
                           i64_length;

        let header = Header::with_get_more(total_length, request_id);

        Message::OpGetMore { header: header,
                             namespace: namespace,
                             number_to_return: number_to_return,
                             cursor_id: cursor_id }
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
    /// Returns nothing on success, or an error string on failure.
    fn write_bson_document(buffer: &mut Write,
                           bson: &bson::Document) -> Result<(), String>{
        let mut temp_buffer = vec![];

        match bson::encode_document(&mut temp_buffer, bson) {
            Ok(_) => match buffer.write(&temp_buffer) {
                Ok(_) => Ok(()),
                Err(_) => Err("unable to write BSON".to_owned())
            },
            Err(_) => Err("unable to encode BSON".to_owned())
        }
    }

    pub fn write_update(buffer: &mut Write, header: &Header,
                        namespace: &str, flags: &OpUpdateFlags,
                        selector: &bson::Document,
                        update: &bson::Document) -> Result<(), String> {
        match header.write(buffer) {
            Ok(_) => (),
            Err(e) => return Err(e)
        };

        // Write ZERO field
        match buffer.write_i32::<LittleEndian>(0) {
            Ok(_) => (),
            Err(_) => return Err("Unable to write flags".to_owned())
        };

        for byte in namespace.bytes() {
            let _byte_reponse = match buffer.write_u8(byte) {
                Ok(_) => (),
                Err(_) => return Err("Unable to write namespace".to_owned())
            };
        }

        // Writes the null terminator for the collection name string.
        match buffer.write_u8(0) {
            Ok(_) => (),
            Err(_) => return Err("Unable to write namespace".to_owned())
        };


        match buffer.write_i32::<LittleEndian>(flags.to_i32()) {
            Ok(_) => (),
            Err(_) => return Err("Unable to write flags".to_owned())
        };

        match Message::write_bson_document(buffer, selector) {
            Ok(_) => (),
            Err(s) =>
                return Err(format!("Unable to write `selector` field: {}", s))
        };

        match Message::write_bson_document(buffer, update) {
            Ok(_) => (),
            Err(s) =>
                return Err(format!("Unable to write `update` field: {}", s))
        };

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
    /// `return_field_selector - An optional projection of which fields should
    ///                          be present in the documents to be returned by
    ///                          the query.
    ///
    /// # Return value
    ///
    /// Returns nothing on success, or an error string on failure.
    fn write_query(buffer: &mut Write, header: &Header,
                   flags: &OpQueryFlags, namespace: &str,
                   number_to_skip: i32, number_to_return: i32, query: &bson::Document,
                   return_field_selector: &Option<bson::Document>) -> Result<(), String> {
        match header.write(buffer) {
            Ok(_) => (),
            Err(e) => return Err(e)
        };

        match buffer.write_i32::<LittleEndian>(flags.to_i32()) {
            Ok(_) => (),
            Err(_) => return Err("Unable to write flags".to_owned())
        };

        for byte in namespace.bytes() {
            let _byte_reponse = match buffer.write_u8(byte) {
                Ok(_) => (),
                Err(_) => return Err("Unable to write namespace".to_owned())
            };
        }

        // Writes the null terminator for the collection name string.
        match buffer.write_u8(0) {
            Ok(_) => (),
            Err(_) => return Err("Unable to write namespace".to_owned())
        };

        match buffer.write_i32::<LittleEndian>(number_to_skip) {
            Ok(_) => (),
            Err(_) => return Err("Unable to write number_to_skip".to_owned())
        };

        match buffer.write_i32::<LittleEndian>(number_to_return) {
            Ok(_) => (),
            Err(_) => return Err("Unable to write number_to_return".to_owned())
        };

        match Message::write_bson_document(buffer, query) {
            Ok(_) => (),
            Err(s) => return Err(format!("Unable to write query: {}", s))
        };

        match return_field_selector {
            &Some(ref bson) => match Message::write_bson_document(buffer, bson) {
                Ok(_) => (),
                Err(s) => {
                    let str = format!("Unable to write return_field_selector: {}", s);

                    return Err(str)
                }
            },
            &None => ()
        };

        let _ = buffer.flush();

        Ok(())
    }



    fn write_insert(buffer: &mut Write, header: &Header, flags: &OpInsertFlags,
                    namespace: &str,
                    documents: &[bson::Document]) -> Result<(), String> {
        match header.write(buffer) {
            Ok(_) => (),
            Err(e) => return Err(e)
        };

        match buffer.write_i32::<LittleEndian>(flags.to_i32()) {
            Ok(_) => (),
            Err(_) => return Err("Unable to write flags".to_owned())
        };

        for byte in namespace.bytes() {
            let _byte_reponse = match buffer.write_u8(byte) {
                Ok(_) => (),
                Err(_) => return Err("Unable to write namespace".to_owned())
            };
        }

        // Writes the null terminator for the collection name string.
        match buffer.write_u8(0) {
            Ok(_) => (),
            Err(_) => return Err("Unable to write namespace".to_owned())
        };


        for bson in documents {
            match Message::write_bson_document(buffer, bson) {
                Ok(_) => (),
                Err(s) => return Err(format!("Unable to insert document: {}", s))
            };

        }

        let _ = buffer.flush();

        Ok(())
    }

    pub fn write_get_more(buffer: &mut Write, header: &Header,
                          namespace: &str, number_to_return: i32,
                          cursor_id: i64) -> Result<(), String> {
        match header.write(buffer) {
            Ok(_) => (),
            Err(e) => return Err(e)
        };

        // Write ZERO field
        match buffer.write_i32::<LittleEndian>(0) {
            Ok(_) => (),
            Err(_) => return Err("Unable to write ZERO field".to_owned())
        };

        for byte in namespace.bytes() {
            let _byte_reponse = match buffer.write_u8(byte) {
                Ok(_) => (),
                Err(_) => return Err("Unable to write \
                                      namespace".to_owned())
            };
        }

        // Writes the null terminator for the collection name string.
        match buffer.write_u8(0) {
            Ok(_) => (),
            Err(_) =>
                return Err("Unable to write namespace".to_owned())
        };


        match buffer.write_i32::<LittleEndian>(number_to_return) {
            Ok(_) => (),
            Err(_) => return Err("Unable to write number_to_return".to_owned())
        };


        match buffer.write_i64::<LittleEndian>(cursor_id) {
            Ok(_) => (),
            Err(_) => return Err("Unable to write cursor_id".to_owned())
        };

        let _ = buffer.flush();

        Ok(())
    }

    /// Attemps to write a serialized message to a buffer.
    ///
    /// # Arguments
    ///
    /// `buffer` - The buffer to write to.
    ///
    /// # Return value
    ///
    /// Returns nothing on success, or an error string on failure.
    pub fn write(&self, buffer: &mut Write) -> Result<(), String> {
        match self {
            /// Only the server should sent replies
            &Message::OpReply {..} =>
                Err("OP_REPLY should not be sent by the client".to_owned()),
            &Message::OpUpdate { ref header, ref namespace,
                                 ref flags, ref selector, ref update } =>
                Message::write_update(buffer, &header,&namespace,
                                      &flags, &selector, &update),
            &Message::OpInsert { ref header, ref flags,
                                 ref namespace, ref documents } =>
                Message::write_insert(buffer, &header, &flags,
                                      &namespace, &documents),
            &Message::OpQuery { ref header, ref flags, ref namespace,
                                number_to_skip, number_to_return, ref query,
                                ref return_field_selector } =>
                Message::write_query(buffer, &header, &flags,
                                     &namespace, number_to_skip,
                                     number_to_return, &query,
                                     &return_field_selector),
            &Message::OpGetMore { ref header, ref namespace,
                                  number_to_return, cursor_id } =>
                Message::write_get_more(buffer, &header, &namespace,
                                        number_to_return, cursor_id)
        }
    }

    /// Reads a serialized reply message from a buffer
    ///
    /// Right now, this returns only the first BSON document from the
    /// response; if there are more, it ignores the rest, and if there are none,
    /// it fails.
    ///
    /// # Arguments
    ///
    /// `buffer` - The buffer to read from.
    ///
    /// # Return value
    ///
    /// Returns a single BSON document on success, or an error string on
    /// failure.
    fn read_reply(buffer: &mut Read, h: Header) -> Result<Message, String> {
        let mut length = h.message_length - mem::size_of::<Header>() as i32;

        let flags = match buffer.read_i32::<LittleEndian>() {
            Ok(i) => i,
            Err(_) => return Err("Unable to read flags".to_owned())
        };

        length -= mem::size_of::<i32>() as i32;

        let cid = match buffer.read_i64::<LittleEndian>() {
            Ok(i) => i,
            Err(_) => return Err("Unable to read cursor_id".to_owned())
        };

        length -= mem::size_of::<i64>() as i32;

        let sf = match buffer.read_i32::<LittleEndian>() {
            Ok(i) => i,
            Err(_) => return Err("Unable to read starting_from".to_owned())
        };

        length -= mem::size_of::<i32>() as i32;

        let nr = match buffer.read_i32::<LittleEndian>() {
            Ok(i) => i,
            Err(_) => return Err("Unable to read number_returned".to_owned())
        };

        length -= mem::size_of::<i32>() as i32;

        let mut v = vec![];

        while length > 0 {
            match bson::decode_document(buffer) {
                Ok(bson) => {
                    match bson.byte_length() {
                        Ok(i) => length -= i,
                        Err(e) => return Err(e)
                    };

                    v.push(bson);
                },
                Err(_) => return Err("Unable to read BSON".to_owned())
            }
        }

        Ok(Message::with_reply(h, flags, cid, sf, nr, v))
    }

    /// Attempts to read a serialized reply Message from a buffer.
    ///
    /// # Arguments
    ///
    /// `buffer` - The buffer to read from.
    ///
    /// # Return value
    ///
    /// Returns a single BSON document on success, or an error string on
    /// failure.
    pub fn read<T>(buffer: &mut T) -> Result<Message, String> where T: Read + Write {
        let header = match Header::read(buffer) {
            Ok(h) => h,
            Err(s) => {
                let str = format!("Unable to read reply header: {}", s);

                return Err(str)
            }
        };

        match header.op_code {
            OpCode::Reply => {
                Message::read_reply(buffer, header)
            },
            opcode => {
                Err(format!("Expected to read response but instead found: {}",
                            opcode.to_string()))
            }
        }
    }
}
