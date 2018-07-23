//! Authentication schemes.
use bson::Bson::{self, Binary};
use bson::Document;
use bson::spec::BinarySubtype::Generic;
use CommandType::Suppressed;
use hmac::{Hmac, Mac};
use md5::Md5;
use pbkdf2::pbkdf2;
use sha1::{Sha1, Digest};
use hex;
use data_encoding::BASE64;
use db::{Database, ThreadedDatabase};
use error::Error::{DefaultError, MaliciousServerError, ResponseError};
use error::MaliciousServerErrorType;
use error::Result;
use textnonce::TextNonce;

/// Handles SCRAM-SHA-1 authentication logic.
pub struct Authenticator {
    db: Database,
}

struct InitialData {
    message: String,
    response: String,
    nonce: String,
    conversation_id: Bson,
}

struct AuthData {
    salted_password: [u8; 20],
    message: String,
    response: Document,
}

type HmacSha1 = Hmac<Sha1>;
const SHA1_OUTPUT: usize = 20;

impl Authenticator {
    /// Creates a new authenticator.
    pub fn new(db: Database) -> Authenticator {
        Authenticator { db: db }
    }

    /// Authenticates a user-password pair against a database.
    pub fn auth(self, user: &str, password: &str) -> Result<()> {
        let initial_data = try!(self.start(user));
        let conversation_id = initial_data.conversation_id.clone();
        let full_password = format!("{}:mongo:{}", user, password);
        let auth_data = try!(self.next(full_password, initial_data));

        self.finish(conversation_id, auth_data)
    }

    fn start(&self, user: &str) -> Result<InitialData> {
        let text_nonce = match TextNonce::sized(64) {
            Ok(text_nonce) => text_nonce,
            Err(string) => return Err(DefaultError(string)),
        };

        let nonce = format!("{}", text_nonce);
        let message = format!("n={},r={}", user, nonce);
        let bytes = format!("n,,{}", message).into_bytes();
        let binary = Binary(Generic, bytes);

        let start_doc =
            doc! {
            "saslStart": 1,
            "autoAuthorize": 1,
            "payload": binary,
            "mechanism": "SCRAM-SHA-1"
        };

        let doc = try!(self.db.command(start_doc, Suppressed, None));

        let data = match doc.get("payload") {
            Some(&Binary(_, ref payload)) => payload.to_owned(),
            _ => return Err(ResponseError(String::from("Invalid payload returned"))),
        };

        let id = match doc.get("conversationId") {
            Some(bson) => bson.clone(),
            None => return Err(ResponseError(String::from("No conversationId returned"))),
        };

        let response = match String::from_utf8(data) {
            Ok(string) => string,
            Err(_) => {
                return Err(ResponseError(
                    String::from("Invalid UTF-8 payload returned"),
                ))
            }
        };

        Ok(InitialData {
            message: message,
            response: response,
            nonce: nonce,
            conversation_id: id,
        })
    }

    fn next(&self, password: String, initial_data: InitialData) -> Result<AuthData> {
        // Parse out rnonce, salt, and iteration count
        let (rnonce_opt, salt_opt, i_opt) = scan_fmt!(
            &initial_data.response[..],
            "r={},s={},i={}",
            String,
            String,
            u32
        );

        let rnonce_b64 = rnonce_opt.ok_or_else(|| {
            ResponseError(String::from("Invalid rnonce returned"))
        })?;

        // Validate rnonce to make sure server isn't malicious
        if !rnonce_b64.starts_with(&initial_data.nonce[..]) {
            return Err(MaliciousServerError(
                MaliciousServerErrorType::InvalidRnonce,
            ));
        }

        let salt_b64 = salt_opt.ok_or_else(|| {
            ResponseError(String::from("Invalid salt returned"))
        })?;

        let salt = BASE64.decode(salt_b64.as_bytes()).or_else(|e| {
            Err(ResponseError(
                format!("Invalid base64 salt returned: {}", e),
            ))
        })?;

        let i = i_opt.ok_or_else(|| {
            ResponseError(String::from("Invalid iteration count returned"))
        })?;

        // Hash password
        let hashed_password = hex::encode(Md5::digest(password.as_bytes()));

        // Salt password
        let mut salted_password = [0u8; SHA1_OUTPUT];
        pbkdf2::<HmacSha1>(hashed_password.as_bytes(), &salt, i as usize, &mut salted_password);

        // Compute client key
        let mut client_key_hmac = HmacSha1::new_varkey(&salted_password)
            .expect("HMAC can take key of any size");
        let client_key_bytes = b"Client Key";
        client_key_hmac.input(client_key_bytes);
        let client_key = client_key_hmac.result().code().to_owned();

        // Hash into stored key
        let mut stored_key_sha1 = Sha1::new();
        stored_key_sha1.input(&client_key[..]);
        let stored_key = stored_key_sha1.result();

        // Create auth message
        let without_proof = format!("c=biws,r={}", rnonce_b64);
        let auth_message = format!(
            "{},{},{}",
            initial_data.message,
            initial_data.response,
            without_proof
        );

        // Compute client signature
        let mut client_signature_hmac = HmacSha1::new_varkey(&stored_key)
            .expect("HMAC can take key of any size");
        client_signature_hmac.input(auth_message.as_bytes());
        let client_signature = client_signature_hmac.result().code().to_owned();

        // Sanity check
        if client_key.len() != client_signature.len() {
            return Err(DefaultError(String::from(
                "Generated client key and/or client signature is invalid",
            )));
        }

        // Compute proof by xor'ing key and signature
        let mut proof = vec![];
        for i in 0..client_key.len() {
            proof.push(client_key[i] ^ client_signature[i]);
        }

        // Encode proof and produce the message to send to the server
        let b64_proof = BASE64.encode(&proof);
        let final_message = format!("{},p={}", without_proof, b64_proof);
        let binary = Binary(Generic, final_message.into_bytes());

        let next_doc =
            doc! {
                "saslContinue": 1,
                "payload": binary,
                "conversationId": initial_data.conversation_id.clone()
            };

        let response = try!(self.db.command(next_doc, Suppressed, None));

        Ok(AuthData {
            salted_password: salted_password,
            message: auth_message,
            response: response,
        })
    }

    fn finish(&self, conversation_id: Bson, auth_data: AuthData) -> Result<()> {
        let final_doc =
            doc! {
                "saslContinue": 1,
                "payload": Binary(Generic, vec![]),
                "conversationId": conversation_id
            };

        // Compute server key
        let mut server_key_hmac = HmacSha1::new_varkey(&auth_data.salted_password)
            .expect("HMAC can take key of any size");
        let server_key_bytes = b"Server Key";
        server_key_hmac.input(server_key_bytes);
        let server_key = server_key_hmac.result().code();

        // Compute server signature
        let mut server_signature_hmac = HmacSha1::new_varkey(&server_key)
            .expect("HMAC can take key of any size");
        server_signature_hmac.input(auth_data.message.as_bytes());
        let server_signature = server_signature_hmac.result().code();

        let mut doc = auth_data.response;

        loop {
            // Verify server signature
            if let Some(&Binary(_, ref payload)) = doc.get("payload") {
                let payload_str = match String::from_utf8(payload.to_owned()) {
                    Ok(string) => string,
                    Err(_) => {
                        return Err(ResponseError(
                            String::from("Invalid UTF-8 payload returned"),
                        ))
                    }
                };

                // Check that the signature exists
                let verifier = match scan_fmt!(&payload_str[..], "v={}", String) {
                    Some(string) => string,
                    None => {
                        return Err(MaliciousServerError(
                            MaliciousServerErrorType::NoServerSignature,
                        ))
                    }
                };

                // Check that the signature is valid
                if verifier.ne(&BASE64.encode(&server_signature)[..]) {
                    return Err(MaliciousServerError(
                        MaliciousServerErrorType::InvalidServerSignature,
                    ));
                }
            }

            doc = try!(self.db.command(final_doc.clone(), Suppressed, None));

            if let Some(&Bson::Boolean(true)) = doc.get("done") {
                return Ok(());
            }
        }
    }
}
