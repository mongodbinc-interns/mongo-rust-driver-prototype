extern crate libc;
extern crate mongodb;

use libc::size_t;

#[test]
fn single_key_document() {
    let doc = mongodb::get_single_key_bson_doc();
    let s = mongodb::bson_to_json(doc);
    assert_eq!(s, "{ \"foo\" : 42 }");
}

#[test]
fn multi_key_document() {
    let doc = mongodb::get_multi_key_bson_doc();
    let s = mongodb::bson_to_json(doc);
    assert_eq!(s, "{ \"foo\" : 42, \"bar\" : \"shallow\" }");
}
