extern crate libc;

use libc::c_char;
use libc::size_t;
use std::ffi::CStr;

#[repr(C)]
pub struct BSONDocument {
    flags: u32,
    len: u32,
    padding: [u8;120],
}

#[link(name = "bson-1.0")]
extern {
    fn bson_new() -> *mut BSONDocument;
    fn bson_append_int32(bson: *const BSONDocument, key: *const u8,
                         key_length: i32, value: i32) -> bool;
    fn bson_append_utf8(bson: *const BSONDocument, key: *const u8,
                        key_length: i32, value: *const u8, length: i32);
    fn bson_as_json(bson: *const BSONDocument,
                    length: *mut size_t) -> *mut c_char;
}

pub fn get_single_key_bson_doc() -> *mut BSONDocument {
    unsafe {
        let y = bson_new();
        bson_append_int32(y, (b"foo").as_ptr(), 3, 42 as i32);
        y
    }
}

pub fn get_multi_key_bson_doc() -> *mut BSONDocument {
    unsafe {
        let y = bson_new();
        bson_append_int32(y, (b"foo").as_ptr(), 3, 42 as i32);
        bson_append_utf8(y, (b"bar").as_ptr(), 3, (b"shallow").as_ptr(), 7);
        y
    }
}

pub fn bson_to_json(doc: *const BSONDocument) -> String {
    let mut size = 0 as size_t;

    let c_buf: *const c_char = unsafe { bson_as_json(doc, &mut size) };
    let c_str: &CStr = unsafe { CStr::from_ptr(c_buf) };
    let buf: &[u8] = c_str.to_bytes();
    let str_slice: &str = std::str::from_utf8(buf).unwrap();
    str_slice.to_owned()
}
