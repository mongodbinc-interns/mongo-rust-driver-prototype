use bson::Bson;

use mongodb::{Client, ThreadedClient};
use mongodb::coll::Collection;
use mongodb::coll::options::{FindOptions, IndexOptions};
use mongodb::db::ThreadedDatabase;
use mongodb::gridfs::{Store, ThreadedStore};
use mongodb::gridfs::file::DEFAULT_CHUNK_SIZE;

use rand::{thread_rng, Rng};
use std::io::{Read, Write};

fn init_gridfs(name: &str) -> (Store, Collection, Collection) {
    let client = Client::connect("localhost", 27017).unwrap();
    let db = client.db(name);
    let fs = Store::with_db(db.clone());

    let fsfiles = db.collection("fs.files");
    let fschunks = db.collection("fs.chunks");
    fsfiles.drop().expect("Failed to drop files collection.");
    fschunks.drop().expect("Failed to drop chunks collection.");
    (fs, fsfiles, fschunks)
}

fn gen_rand_file(len: usize) -> Vec<u8> {
    let mut src = Vec::with_capacity(len);
    unsafe { src.set_len(len) };
    thread_rng().fill_bytes(&mut src);
    src
}

#[test]
fn put_get() {
    let (fs, _, fschunks) = init_gridfs("test-client-gridfs-grid_put");

    let name = "grid_put_file";
    let src_len = (DEFAULT_CHUNK_SIZE as f64 * 2.5) as usize;
    let src = gen_rand_file(src_len);

    let mut grid_file = fs.create(name.to_owned()).unwrap();
    let id = grid_file.id.clone();
    grid_file.write_all(&src).unwrap();
    grid_file.close().unwrap();

    // Check file
    let mut cursor = fs.find(Some(doc!{"filename": name}), None).unwrap();

    match cursor.next() {
        Some(file) => assert_eq!(file.len() as usize, src_len),
        _ => panic!("Expected to retrieve file from cursor."),
    }

    let mut opts = FindOptions::new();
    opts.sort = Some(doc!{ "n": 1});

    // Check chunks
    let mut cursor = fschunks
        .find(Some(doc!{"files_id": id.clone()}), Some(opts))
        .unwrap();

    let chunks = cursor.drain_current_batch().expect(
        "Failed to get current batch",
    );
    assert_eq!(3, chunks.len());

    for (i, chunk) in chunks.iter().enumerate().take(3) {
        if let Some(&Bson::I32(ref n)) = chunk.get("n") {
            assert_eq!(i as i32, *n);
        }

        if let Some(&Bson::Binary(_, ref data)) = chunk.get("data") {
            for (j, item) in data.iter().enumerate() {
                assert_eq!(src[j + i * DEFAULT_CHUNK_SIZE as usize], *item);
            }
        }
    }

    // Ensure index
    let mut cursor = fschunks.list_indexes().unwrap();
    let results = cursor.next_n(10).unwrap();
    assert_eq!(2, results.len());

    let mut opts = IndexOptions::new();
    opts.unique = Some(true);
    fschunks
        .create_index(doc!{ "files_id": 1, "n": 1}, Some(opts))
        .unwrap();
    let mut cursor = fschunks.list_indexes().unwrap();
    let results = cursor.next_n(10).unwrap();
    assert_eq!(2, results.len());

    // Get
    let mut dest = Vec::with_capacity(src_len);
    unsafe { dest.set_len(src_len) };
    let mut read_file = fs.open(name.to_owned()).unwrap();

    let n = read_file.read(&mut dest).unwrap();
    assert_eq!(src_len, n);

    read_file.close().unwrap();

    for i in 0..src_len {
        assert_eq!(src[i], dest[i]);
    }
}

#[test]
fn remove() {
    let (fs, fsfiles, fschunks) = init_gridfs("test-client-gridfs-grid_remove");

    let name = "grid_remove_file";
    let src_len = (DEFAULT_CHUNK_SIZE as f64 * 1.5) as usize;
    let src = gen_rand_file(src_len);

    let mut grid_file = fs.create(name.to_owned()).unwrap();
    let id = grid_file.id.clone();
    grid_file.write_all(&src).unwrap();
    grid_file.close().unwrap();

    assert!(
        fsfiles
            .find_one(Some(doc!{"_id": id.clone()}), None)
            .unwrap()
            .is_some()
    );

    let mut cursor = fschunks
        .find(Some(doc!{"files_id": id.clone()}), None)
        .unwrap();
    let results = cursor.drain_current_batch().unwrap();
    assert_eq!(2, results.len());

    fs.remove(name.to_owned()).unwrap();
    assert!(
        fsfiles
            .find_one(Some(doc!{"_id": id.clone()}), None)
            .unwrap()
            .is_none()
    );

    let mut cursor = fschunks
        .find(Some(doc!{"files_id": id.clone()}), None)
        .unwrap();
    let results = cursor.drain_current_batch().unwrap();
    assert_eq!(0, results.len());
}

#[test]
fn remove_id() {
    let (fs, fsfiles, fschunks) = init_gridfs("test-client-gridfs-grid_remove_id");

    let name = "grid_remove_id_file";
    let src_len = (DEFAULT_CHUNK_SIZE as f64 * 1.5) as usize;
    let src = gen_rand_file(src_len);

    let mut grid_file = fs.create(name.to_owned()).unwrap();
    let id = grid_file.id.clone();
    grid_file.write_all(&src).unwrap();
    grid_file.close().unwrap();

    assert!(
        fsfiles
            .find_one(Some(doc!{"_id": id.clone()}), None)
            .unwrap()
            .is_some()
    );

    let mut cursor = fschunks
        .find(Some(doc!{"files_id": id.clone()}), None)
        .unwrap();
    let results = cursor.drain_current_batch().unwrap();
    assert_eq!(2, results.len());

    fs.remove_id(id.clone()).unwrap();
    let mut cursor = fschunks
        .find(Some(doc!{"files_id": id.clone()}), None)
        .unwrap();
    let results = cursor.drain_current_batch().unwrap();
    assert_eq!(0, results.len());
}

#[test]
fn find() {
    let (fs, _, _) = init_gridfs("test-client-gridfs-grid_find");

    let name = "grid_find_file";
    let name2 = "grid_find_file_2";

    let src_len = (DEFAULT_CHUNK_SIZE as f64 * 1.5) as usize;
    let src = gen_rand_file(src_len);

    let mut grid_file = fs.create(name.to_owned()).unwrap();
    let id = grid_file.id.clone();
    grid_file.write_all(&src).unwrap();
    grid_file.close().unwrap();

    let mut grid_file2 = fs.create(name2.to_owned()).unwrap();
    let id2 = grid_file2.id.clone();
    grid_file2.write_all(&src).unwrap();
    grid_file2.close().unwrap();

    let mut cursor = fs.find(None, None).unwrap();
    let results = cursor.drain_current_batch().unwrap();
    assert_eq!(2, results.len());
    assert_eq!(name, results[0].name.as_ref().unwrap());
    assert_eq!(name2, results[1].name.as_ref().unwrap());
    assert_eq!(src_len, results[0].len() as usize);
    assert_eq!(src_len, results[1].len() as usize);
    assert_eq!(id, results[0].id);
    assert_eq!(id2, results[1].id);
}
