use bson::{self, Bson};
use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use mongodb::db::options::CreateUserOptions;
use mongodb::db::roles::{AllDatabaseRole, SingleDatabaseRole, Role};

#[test]
fn create_collection() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-db-create_collection");
    db.drop_database().unwrap();

    // Build collections
    db.create_collection("test1", None).unwrap();
    db.create_collection("test2", None).unwrap();

    // Check for namespaces
    let mut cursor = db.list_collections_with_batch_size(None, 1).expect(
        "Failed to execute list_collections command.",
    );

    let results = cursor.next_n(5).unwrap();

    let db_version = db.version().unwrap();
    let v3_1 = db_version.major <= 3 && db_version.minor <= 1;

    let result_size = if v3_1 { 3 } else { 2 };
    assert_eq!(result_size, results.len());

    if v3_1 {
        match results[0].get("name") {
            Some(&Bson::String(ref name)) => assert_eq!("system.indexes", name),
            _ => panic!("Expected BSON string!"),
        }
    }

    let db1 = if v3_1 { "test1" } else { "test2" };
    let db2 = if v3_1 { "test2" } else { "test1" };

    match results[result_size - 2].get("name") {
        Some(&Bson::String(ref name)) => assert_eq!(db1, name),
        _ => panic!("Expected BSON string!"),
    }
    match results[result_size - 1].get("name") {
        Some(&Bson::String(ref name)) => assert_eq!(db2, name),
        _ => panic!("Expected BSON string!"),
    }
}

#[test]
fn list_collections() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-db-list_collections");

    db.drop_database().expect("Failed to drop database");

    // Build collections
    db.collection("test")
        .insert_one(bson::Document::new(), None)
        .expect("Failed to insert placeholder document into collection");
    db.collection("test2")
        .insert_one(bson::Document::new(), None)
        .expect("Failed to insert placeholder document into collection");

    // Check for namespaces
    let mut cursor = db.list_collections_with_batch_size(None, 1).expect(
        "Failed to execute list_collections command.",
    );

    let results = cursor.next_n(5).unwrap();

    let db_version = db.version().unwrap();
    let v3_1 = db_version.major <= 3 && db_version.minor <= 1;

    let result_size = if v3_1 { 3 } else { 2 };
    assert_eq!(result_size, results.len());

    if v3_1 {
        match results[0].get("name") {
            Some(&Bson::String(ref name)) => assert_eq!("system.indexes", name),
            _ => panic!("Expected BSON string!"),
        };
    }

    let db1 = if v3_1 { "test" } else { "test2" };
    let db2 = if v3_1 { "test2" } else { "test" };

    match results[result_size - 2].get("name") {
        Some(&Bson::String(ref name)) => assert_eq!(db1, name),
        _ => panic!("Expected BSON string!"),
    };

    match results[result_size - 1].get("name") {
        Some(&Bson::String(ref name)) => assert_eq!(db2, name),
        _ => panic!("Expected BSON string!"),
    }
}

#[test]
fn create_and_get_users() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-db-create_and_get_users");
    db.drop_database().unwrap();
    db.drop_all_users(None).unwrap();

    let kevin_options = CreateUserOptions {
        custom_data: None,
        roles: vec![Role::All(AllDatabaseRole::Read)],
        write_concern: None,
    };

    db.create_user(
        "kevin",
        "ihavenosenseofhumorandthereforeihatepuns!",
        Some(kevin_options),
    ).unwrap();

    let saghm_options = CreateUserOptions {
        custom_data: Some(doc! { "foo": "bar" }),
        roles: vec![
            Role::Single {
                role: SingleDatabaseRole::DbAdmin,
                db: String::from("test"),
            },
            Role::All(AllDatabaseRole::ReadWrite),
        ],
        write_concern: None,
    };

    db.create_user("saghm", "ilikepuns!", Some(saghm_options))
        .unwrap();

    db.create_user("val", "ilikeangularjs!", None).unwrap();

    let user = db.get_user("saghm", None).unwrap();

    match user.get("db") {
        Some(&Bson::String(ref s)) => assert_eq!("test-client-db-create_and_get_users", s),
        _ => {
            panic!(
                "Invalid `db` specified for user 'saghm': {:?}",
                user.get("db")
            )
        }
    };

    let data = match user.get("customData") {
        Some(&Bson::Document(ref d)) => d.clone(),
        _ => panic!("Invalid `customData` specified for user 'saghm'"),
    };

    match data.get("foo") {
        Some(&Bson::String(ref s)) => assert_eq!("bar", s),
        _ => panic!("Invalid custom data for user 'saghm': {}", data),
    };

    let users = db.get_users(vec!["kevin", "val"], None).unwrap();
    assert_eq!(users.len(), 2 as usize);

    match users[0].get("user") {
        Some(&Bson::String(ref s)) => assert_eq!("kevin", s),
        _ => panic!("User isn't named 'kevin' but should be"),
    };

    match users[1].get("user") {
        Some(&Bson::String(ref s)) => assert_eq!("val", s),
        _ => panic!("User isn't named 'val' but should be"),
    };

    let users = db.get_all_users(false).unwrap();

    assert_eq!(users.len(), 3);

    match users[0].get("user") {
        Some(&Bson::String(ref s)) => assert_eq!("kevin", s),
        _ => panic!("User isn't named 'kevin' but should be"),
    };

    match users[1].get("user") {
        Some(&Bson::String(ref s)) => assert_eq!("saghm", s),
        _ => panic!("User isn't named 'saghm' but should be"),
    };

    match users[2].get("user") {
        Some(&Bson::String(ref s)) => assert_eq!("val", s),
        _ => panic!("User isn't named 'val' but should be"),
    };
}

#[test]
fn get_version() {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db("test-client-db-get_version");
    let _ = db.version().unwrap();
}
