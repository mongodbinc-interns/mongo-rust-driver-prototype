use mongodb::common::WriteConcern;
use mongodb::coll::error::{BulkWriteException, WriteConcernError, WriteError};
use mongodb::Error;

#[test]
fn validate_write_result() {
    let doc =
        doc! {
            "ok": 1,
            "n": 5,
            "nModified": 5
        };

    let result = BulkWriteException::validate_bulk_write_result(doc, WriteConcern::new());
    assert!(result.is_ok());
}

#[test]
fn invalidate_write_result() {
    let err1 =
        doc! {
            "index": 0,
            "code": 1054,
            "errmsg": "Unreal error message."
        };

    let err2 =
        doc! {
            "index": 3,
            "code": 2105,
            "errmsg": "Modestly real error message."
     };

    let doc =
        doc! {
            "ok": 1,
            "n": 5,
            "nModified": 3,
            "writeConcernError": {
            "code": 1124,
            "errmsg": "Real error message."
        },
        "writeErrors": [err1, err2]
    };

    let result = BulkWriteException::validate_bulk_write_result(doc, WriteConcern::new());
    assert!(result.is_err());
    match result {
        Err(Error::BulkWriteError(err)) => {
            let wc_err = &err.write_concern_error;
            let w_errs = &err.write_errors;

            assert_eq!(2, w_errs.len());
            let w0 = &w_errs[0];
            let w1 = &w_errs[1];
            assert_eq!(0, w0.index);
            assert_eq!(3, w1.index);
            assert_eq!(1054, w0.code);
            assert_eq!(2105, w1.code);
            assert_eq!("Unreal error message.".to_owned(), w0.message);
            assert_eq!("Modestly real error message.".to_owned(), w1.message);

            assert!(wc_err.is_some());
            let wc = wc_err.as_ref().unwrap();
            assert_eq!(1124, wc.code);
            assert_eq!("Real error message.".to_owned(), wc.message);
            assert_eq!(WriteConcern::new(), wc.details);
        }
        Err(_) => panic!("Expected BulkWriteError, received alternative error!"),
        Ok(()) => panic!("Expected BulkWriteError, received an Ok(())!"),
    }
}

#[test]
fn parse_write_concern_error() {
    let doc =
        doc! {
            "code": 1124,
            "errmsg": "Real error message."
        };

    let result = WriteConcernError::parse(doc, WriteConcern::new());
    match result {
        Ok(err) => {
            assert_eq!(1124, err.code);
            assert_eq!("Real error message.".to_owned(), err.message);
            assert_eq!(WriteConcern::new(), err.details);
        }
        Err(_) => panic!("Failed to parse valid Write Concern Error from document."),
    }
}

#[test]
fn parse_invalid_write_concern_error() {
    let doc = doc! { "code": 1124 };
    let result = WriteConcernError::parse(doc, WriteConcern::new());
    assert!(result.is_err());

    let doc = doc! { "code": "string" };
    let result = WriteConcernError::parse(doc, WriteConcern::new());
    assert!(result.is_err());
}

#[test]
fn parse_write_error() {
    let doc =
        doc! {
            "code": 1054,
            "errmsg": "Unreal error message."
        };

    let result = WriteError::parse(doc);
    match result {
        Ok(err) => {
            assert_eq!(1054, err.code);
            assert_eq!("Unreal error message.".to_owned(), err.message);
        }
        Err(_) => panic!("Failed to parse valid Write Error from document."),
    }
}

#[test]
fn parse_invalid_write_error() {
    let doc = doc! { "code": 1124 };
    let result = WriteError::parse(doc);
    assert!(result.is_err());

    let doc = doc! { "code": "string" };
    let result = WriteError::parse(doc);
    assert!(result.is_err());
}
