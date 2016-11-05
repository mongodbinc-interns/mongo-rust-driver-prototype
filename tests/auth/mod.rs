use bson::Bson;
use mongodb::{CommandType, Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use mongodb::error::Error::OperationError;

#[test]
fn invalid_user() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("auth");
    let _ = db.drop_all_users(None).unwrap();
    let doc = doc! { "connectionStatus" => 1};
    let before = db.command(doc.clone(), CommandType::Suppressed, None).unwrap();

    let info = match before.get("authInfo") {
        Some(&Bson::Document(ref doc)) => doc.clone(),
        _ => panic!("Invalid response for initial connectionStatus command"),
    };

    match info.get("authenticatedUsers") {
        Some(&Bson::Array(ref vec)) => assert!(vec.is_empty()),
        _ => panic!("Invalid array of authenticatedUsers for initial connectionStatus command"),
    };

    match db.auth("invalid_user", "some_password") {
        Err(OperationError(_)) => (),
        Err(_) => {
            panic!("Expected OperationError for invalid authentication, but got some other error \
                    instead")
        }
        _ => panic!("Authentication succeeded despite invalid credentials"),
    };

    let after = db.command(doc, CommandType::Suppressed, None).unwrap();

    let info = match after.get("authInfo") {
        Some(&Bson::Document(ref doc)) => doc.clone(),
        _ => panic!("Invalid response for subsequent connectionStatus command"),
    };

    match info.get("authenticatedUsers") {
        Some(&Bson::Array(ref vec)) => assert!(vec.is_empty()),
        _ => panic!("Invalid array of authenticatedUsers for subsequent connectionStatus command"),
    };
}


#[test]
fn invalid_password() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("auth");
    let _ = db.drop_all_users(None).unwrap();
    let doc = doc! { "connectionStatus" => 1};
    let before = db.command(doc.clone(), CommandType::Suppressed, None).unwrap();

    let info = match before.get("authInfo") {
        Some(&Bson::Document(ref doc)) => doc.clone(),
        _ => panic!("Invalid response for initial connectionStatus command"),
    };

    match info.get("authenticatedUsers") {
        Some(&Bson::Array(ref vec)) => assert!(vec.is_empty()),
        _ => panic!("Invalid array of authenticatedUsers for initial connectionStatus command"),
    };

    db.create_user("saghm", "such_secure_password", None).unwrap();

    match db.auth("saghm", "wrong_password") {
        Err(OperationError(_)) => (),
        Err(_) => {
            panic!("Expected OperationError for invalid authentication, but got some other error \
                    instead")
        }
        _ => panic!("Authentication succeeded despite invalid credentials"),
    };

    let after = db.command(doc, CommandType::Suppressed, None).unwrap();

    let info = match after.get("authInfo") {
        Some(&Bson::Document(ref doc)) => doc.clone(),
        _ => panic!("Invalid response for subsequent connectionStatus command"),
    };

    match info.get("authenticatedUsers") {
        Some(&Bson::Array(ref vec)) => assert!(vec.is_empty()),
        _ => panic!("Invalid array of authenticatedUsers for subsequent connectionStatus command"),
    };
}

#[test]
fn successful_login() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("auth");
    let _ = db.drop_all_users(None).unwrap();
    let doc = doc! { "connectionStatus" => 1};
    let before = db.command(doc.clone(), CommandType::Suppressed, None).unwrap();

    let info = match before.get("authInfo") {
        Some(&Bson::Document(ref doc)) => doc.clone(),
        _ => panic!("Invalid response for initial connectionStatus command"),
    };

    match info.get("authenticatedUsers") {
        Some(&Bson::Array(ref vec)) => assert!(vec.is_empty()),
        _ => panic!("Invalid array of authenticatedUsers for initial connectionStatus command"),
    };

    db.create_user("saghm", "such_secure_password", None).unwrap();
    db.auth("saghm", "such_secure_password").unwrap();

    let after = db.command(doc, CommandType::Suppressed, None).unwrap();

    let info = match after.get("authInfo") {
        Some(&Bson::Document(ref doc)) => doc.clone(),
        _ => panic!("Invalid response for subsequent connectionStatus command"),
    };

    let authed_users = match info.get("authenticatedUsers") {
        Some(&Bson::Array(ref vec)) => vec.clone(),
        _ => panic!("Invalid array of authenticatedUsers for subsequent connectionStatus command"),
    };

    assert_eq!(authed_users.len(), 1);

    let user = match authed_users[0] {
        Bson::Document(ref doc) => doc.clone(),
        _ => panic!("Invalid auth'd user in subsequent connectionStatus response"),
    };

    match user.get("user") {
        Some(&Bson::String(ref s)) => assert_eq!(s, "saghm"),
        _ => panic!("Invalid `user` field of auth'd user"),
    };

    match user.get("db") {
        Some(&Bson::String(ref s)) => assert_eq!(s, "auth"),
        _ => panic!("Invalid `db` field of auth'd user"),
    };
}
