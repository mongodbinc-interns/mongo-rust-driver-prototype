use bson::Bson;
use mongodb::{CommandType, Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use mongodb::error::Error::OperationError;

fn doc_vec_find(vec: &[Bson], key: &str, val: &str) -> Option<Bson> {
    vec.iter()
        .cloned()
        .find(|bdoc| match *bdoc {
            Bson::Document(ref doc) => {
                match doc.get(key) {
                    Some(&Bson::String(ref s)) => s == val,
                    _ => false,
                }
            }
            _ => false,
        })
        .map(|ref bson| bson.to_owned())
}

#[test]
fn invalid_user() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-auth-mod-invalid_user");
    let _ = db.drop_user("test-auth-mod-invalid_user-saghm", None);
    let doc = doc! { "connectionStatus": 1};
    let before = db.command(doc.clone(), CommandType::Suppressed, None)
        .unwrap();

    let info = match before.get("authInfo") {
        Some(&Bson::Document(ref doc)) => doc.clone(),
        _ => panic!("Invalid response for initial connectionStatus command"),
    };

    match info.get("authenticatedUsers") {
        Some(&Bson::Array(ref vec)) => {
            assert!(doc_vec_find(&vec, "user", "test-auth-mod-invalid_user-saghm").is_none())
        }
        _ => panic!("Invalid array of authenticatedUsers for initial connectionStatus command"),
    };

    match db.auth("test-auth-mod-invalid_user-saghm", "some_password") {
        Err(OperationError(_)) => (),
        Err(_) => {
            panic!(
                "Expected OperationError for invalid authentication, but got some other error instead"
            )
        }
        _ => panic!("Authentication succeeded despite invalid credentials"),
    };

    let after = db.command(doc, CommandType::Suppressed, None).unwrap();

    let info = match after.get("authInfo") {
        Some(&Bson::Document(ref doc)) => doc.clone(),
        _ => panic!("Invalid response for subsequent connectionStatus command"),
    };

    match info.get("authenticatedUsers") {
        Some(&Bson::Array(ref vec)) => {
            assert!(doc_vec_find(&vec, "user", "test-auth-mod-invalid_user-saghm").is_none())
        }
        _ => panic!("Invalid array of authenticatedUsers for initial connectionStatus command"),
    };
}

#[test]
fn invalid_password() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-auth-mod-invalid_password");
    let _ = db.drop_user("test-auth-mod-invalid_password-saghm", None);
    let doc = doc! { "connectionStatus": 1};
    let before = db.command(doc.clone(), CommandType::Suppressed, None)
        .unwrap();

    let info = match before.get("authInfo") {
        Some(&Bson::Document(ref doc)) => doc.clone(),
        _ => panic!("Invalid response for initial connectionStatus command"),
    };

    match info.get("authenticatedUsers") {
        Some(&Bson::Array(ref vec)) => {
            assert!(doc_vec_find(&vec, "user", "test-auth-mod-invalid_password-saghm").is_none())
        }
        _ => panic!("Invalid array of authenticatedUsers for initial connectionStatus command"),
    };

    db.create_user(
        "test-auth-mod-invalid_password-saghm",
        "such_secure_password",
        None,
    ).unwrap();

    match db.auth("test-auth-mod-invalid_password-saghm", "wrong_password") {
        Err(OperationError(_)) => (),
        Err(_) => {
            panic!(
                "Expected OperationError for invalid authentication, but got some other error instead"
            )
        }
        _ => panic!("Authentication succeeded despite invalid credentials"),
    };

    let after = db.command(doc, CommandType::Suppressed, None).unwrap();

    let info = match after.get("authInfo") {
        Some(&Bson::Document(ref doc)) => doc.clone(),
        _ => panic!("Invalid response for subsequent connectionStatus command"),
    };

    match info.get("authenticatedUsers") {
        Some(&Bson::Array(ref vec)) => {
            assert!(doc_vec_find(&vec, "user", "test-auth-mod-invalid_password-saghm").is_none())
        }
        _ => panic!("Invalid array of authenticatedUsers for subsequent connectionStatus command"),
    };
}

#[test]
fn successful_login() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-auth-mod-successful_login");
    let _ = db.drop_user("test-auth-mod-successful_login-saghm", None);
    let doc = doc! { "connectionStatus": 1};
    let before = db.command(doc.clone(), CommandType::Suppressed, None)
        .unwrap();

    let info = match before.get("authInfo") {
        Some(&Bson::Document(ref doc)) => doc.clone(),
        _ => panic!("Invalid response for initial connectionStatus command"),
    };

    match info.get("authenticatedUsers") {
        Some(&Bson::Array(ref vec)) => {
            assert!(doc_vec_find(&vec, "user", "test-auth-mod-successful_login-saghm").is_none())
        }
        _ => panic!("Invalid array of authenticatedUsers for initial connectionStatus command"),
    };

    db.create_user(
        "test-auth-mod-successful_login-saghm",
        "such_secure_password",
        None,
    ).unwrap();
    db.auth(
        "test-auth-mod-successful_login-saghm",
        "such_secure_password",
    ).unwrap();

    let after = db.command(doc, CommandType::Suppressed, None).unwrap();

    let info = match after.get("authInfo") {
        Some(&Bson::Document(ref doc)) => doc.clone(),
        _ => panic!("Invalid response for subsequent connectionStatus command"),
    };

    let authed_users = match info.get("authenticatedUsers") {
        Some(&Bson::Array(ref vec)) => vec.clone(),
        _ => panic!("Invalid array of authenticatedUsers for subsequent connectionStatus command"),
    };

    let bson_user = doc_vec_find(
        &authed_users,
        "user",
        "test-auth-mod-successful_login-saghm",
    ).unwrap();

    let user = match bson_user {
        Bson::Document(ref doc) => doc.clone(),
        _ => panic!("Invalid auth'd user in subsequent connectionStatus response"),
    };

    match user.get("user") {
        Some(&Bson::String(ref s)) => assert_eq!(s, "test-auth-mod-successful_login-saghm"),
        _ => panic!("Invalid `user` field of auth'd user"),
    };

    match user.get("db") {
        Some(&Bson::String(ref s)) => assert_eq!(s, "test-auth-mod-successful_login"),
        _ => panic!("Invalid `db` field of auth'd user"),
    };
}
