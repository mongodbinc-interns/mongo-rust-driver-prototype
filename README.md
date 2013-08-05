MongoDB Rust Driver Prototype
=============================

This is a prototype driver for MongoDB written in Rust. The API and implementation are currently subject to change at any time. You must not use this driver in production as it is still under development and is in no way supported by 10gen. We absolutely encourage you to experiment with it and provide us feedback on the API, design, and implementation. Bug reports and suggestions for improvements are welcomed, as are pull requests.

## Installation

#### Dependencies
- [Rust](http://rust-lang.org) 0.7 (WARNING: will likely not build on other versions)
- [gcc](http://gcc.gnu.org)
- [GNU Make](http://gnu.org/software/make)
- [Pandoc](http://johnmcfarlane.net/pandoc) for building documentation

#### Documentation
Please find documentation in the docs folder; BSON library documentation can be found [here](https://github.com/10gen-interns/mongo-rust-driver-prototype/tree/master/docs/bson) and Mongo library documentation can be found [here](https://github.com/10gen-interns/mongo-rust-driver-prototype/tree/master/docs/mongo). Documentation is built using rustdoc (please run ```make doc```).

#### Building
The Rust MongoDB driver is built using ```make```. Available targets include:
- ```all``` (default) build binaries for ```bson``` and ```mongo```
- ```libs``` compile C dependencies
- ```bson``` build a binary just for ```bson```
- ```mongo``` build a binary just for ```mongo``` (note: this requires an existing ```bson``` binary)
- ```test``` compile the test suite
- ```check``` compile and run the test suite
- ```doc``` generate documentation
- ```ex``` compile examples
- ```clean``` remove generated binaries
- ```tidy``` clean up unused whitespace

By default, files are compiled with ```unused-unsafe``` warnings denied and ```unnecessary-allocation``` warnings allowed (this is likely to change in the future to disallow all warnings). You can pass additional flags to rustc by setting the ```TOOLFLAGS``` variable. Additionally, integration tests can be enabled by setting ```MONGOTEST=1```. _Integration tests will fail unless there is a MongoDB instance running on 127.0.0.1:27017_.

## Tutorial
Once you've built MongoDB and have the compiled library files, you can make MongoDB available in your code with
```rust
extern mod bson;
extern mod mongo;
```

#### Mongo Driver
In general, aside from the BSON library imports (see below), we will need the following imports:
```rust
use mongo::client::*;
use mongo::util::*;     // access to option flags and specifications, etc.
use mongo::db::*;
use mongo::coll::*;
use mongo::cursor::*;
```

In order to connect with a Mongo server, we first create a client.
```rust
let client = @Client::new();
```
To connect to an unreplicated, unsharded server running on localhost, port 27017 (```MONGO_DEFAULT_PORT```), we use the ```connect``` method:
```rust
match client.connect(~"127.0.0.1", 27017 as uint) {
    Ok(_) => (),
    Err(e) => fail!("%s", e.to_str()),
        // if cannot connect, nothing to do; display error message
}
```
Now we may create handles to databases and collections on the server. We start with collections to demonstrate CRUD operations.
```rust
// create handles to the collections "foo_coll" and "bar_coll" in the
//      database "foo_db" (any may already exist; if not, it will be
//      created on the first insert)
let foo = Collection::new(~"foo_db", ~"foo_coll", client);
let bar = Collection::new(~"foo_db", ~"bar_coll", client);
```
Equivalently, we may create collection handles direction from the ```Client```:
```rust
let foo = client.get_collection(~"foo_db", ~"foo_coll");
```

##### CRUD Operations
We input JSON as strings formatted for JSON and manipulate them (in fact, we can insert anything implementing the ```BsonFormattable``` trait [see BSON section below] as long as its ```to_bson_t``` conversion returns an ```Embedded(~BsonDocument)```, i.e. it is represented as a BSON):
```rust
// insert a document into bar_coll
let ins = ~"{ \"_id\":0, \"a\":0, \"msg\":\"first insert!\" }";
bar.insert(ins, None);
    // no write concern specified---use default

// insert a big batch of documents into foo_coll
let mut ins_batch : ~[~str] = ~[];
let n = 200;
let mut i = 0;
for n.times {
    ins_batch.push(fmt!("{ \"a\":%d, \"b\":\"ins %d\" }", i/2, i));
    i += 1;
}
foo.insert_batch(ins_batch, None, None, None);
    // no write concern specified---use default; no special options

// read one back (no specific query or query options/flags)
match foo.find_one(None, None, None) {
    Ok(ret_doc) => println(fmt!("%?", *ret_doc)),
    Err(e) => fail!("%s", e.to_str()), // should not happen
}
```

In general, to specify options, we put the appropriate options into a vector and wrap them in ```Some```; for the default options we use ```None```. For specific options, see ```util.rs```. Nearly every method returns a ```Result```; operations usually return a ```()``` (for writes) or some variant on ```~BsonDocument``` or ```Cursor``` (for reads) if successful, and a ```MongoErr``` if unsuccessful due to improper arguments, network errors, etc.
```rust
// insert a big batch of documents with duplicated _ids
ins_batch = ~[];
for 5.times {
    ins_batch.push(fmt!("{ \"_id\":%d, \"a\":%d, \"b\":\"ins %d\" }", 2*i/3, i/2, i));
    i += 1;
}

// ***error returned***
match foo.insert_batch(ins_batch, None, None, None) {
    Ok(_) => fail!("bad insert succeeded"),          // should not happen
    Err(e) => println(fmt!("%s", e.to_str())),
}
// ***no error returned since duplicated _ids skipped (CONT_ON_ERR specified)***
match foo.insert_batch(ins_batch, Some(~[CONT_ON_ERR]), None, None) {
    Ok(_) => (),
    Err(e) => fail!("%s", e.to_str()),     // should not happen
}

// create an ascending index on the "b" field named "fubar"
match foo.create_index(~[NORMAL(~[(~"b", ASC)])], None, Some(~[INDEX_NAME(~"fubar")])) {
    Ok(_) => (),
    Err(e) => fail!("%s", e.to_str()),     // should not happen
}
```

##### Cursor and Query-related Operations
To specify queries and projections, we must input them either as ```BsonDocument```s or as properly formatted JSON strings using ```SpecObj```s or ```SpecNotation```s. In general, the order of arguments for CRUD operations is (as applicable) query, projection or operation-dependent specification (e.g. update document for ```update```), optional array of option flags, optional array of user-specified options (e.g. *number* to skip), and write concern.
```rust
// interact with a cursor projected on "b" and using indices and options
match foo.find(None, Some(SpecNotation(~"{ \"b\":1 }")), None) {
    Ok(c) => {
        let mut cursor = c;

        // hint the index "fubar" for the cursor
        cursor.hint(MongoIndexName(~"fubar"));

        // explain the cursor
        println(fmt!("%?", cursor.explain()));

        // sort on the cursor on the "a" field, ascending
        cursor.sort(NORMAL(~[(~"a", ASC)]));

        // iterate on the cursor---no query specified so over whole collection
        for cursor.advance |doc| {
            println(fmt!("%?", *doc));
        }
    }
    Err(e) => fail!("%s", e.to_str()),     // should not happen
}

// drop the index by name (if it were not given a name, specifying by
//      field would have the same effect as providing the default
//      constructed name)
match foo.drop_index(MongoIndexName(~"fubar")) {
    Ok(_) => (),
    Err(e) => fail!("%s", e.to_str()),     // should not happen
}

// remove every element where "a" is 1
match foo.remove(Some(SpecNotation(~"{ \"a\":1 }")), None, None, None) {
    Ok(_) => (),
    Err(e) => fail!("%s", e.to_str()),     // should not happen
}

// upsert every element where "a" is 2 to be 3
match foo.update(   SpecNotation(~"{ \"a\":2 }"),
                    SpecNotation(~"{ \"$set\": { \"a\":3 } }"),
                    Some(~[MULTI, UPSERT]), None, None) {
    Ok(_) => (),
    Err(e) => fail!("%s", e.to_str()),     // should not happen
}
```

##### Database Operations
Now we create a database handle.
```rust
let db = DB::new(~"foo_db", client);

// list the collections in the database
match db.get_collection_names() {
    Ok(names) => {
        // should output
        //      system.indexes
        //      bar_coll
        //      foo_coll
        for names.iter().advance |&n| { println(fmt!("%s", n)); }
    }
    Err(e) => println(fmt!("%s", e.to_str())), // should not happen
}

// perform a run_command, but the result (if successful, a ~BsonDocument)
//      must be parsed appropriately
println(fmt!("%?", db.run_command(SpecNotation(~"{ \"count\":1 }"))));

// drop the database
match client.drop_db(~"foo_db") {
    Ok(_) => (),
    Err(e) => println(fmt!("%s", e.to_str())), // should not happen
}
```

Finally, we should disconnect the client. It can be reconnected to another server after disconnection.
```rust
match client.disconnect() {
    Ok(_) => (),
    Err(e) => println(fmt!("%s", e.to_str())), // should not happen
}
```

Please refer to the documentation for a complete list of available operations.

#### BSON library
##### BSON data types
BSON-valid data items are represented in the ```Document``` type. (Valid types available from the [specification](http://bsonspec.org)).
To get a document for one of these types, you can wrap it yourself or call the ```to_bson_t``` method.
Example:
```rust
use bson::formattable::*;

let a = (1i).to_bson_t(); //Int32(1)
let b = (~"foo").to_bson_t(); //UString(~"foo")
let c = 3.14159.to_bson_t(); //Double(3.14159)
let d = extra::json::String(~"bar").to_bson_t(); //UString(~"bar")
let e = (~"{\"fizz\": \"buzz\"}").to_bson_t();
//e is an Embedded(hashmap associating ~"fizz" with UString(~"buzz"))
//strings will attempt to be parsed as JSON; if they fail,
//they will be silently treated as a plain string instead
```
```to_bson_t``` is contained in the ```BsonFormattable``` trait, so any type implementing this trait can be converted to a Document.

A complete BSON object is represented in the BsonDocument type. BsonDocument contains a size field (```i32```) and map between ```~str```s and ```Document```s.
This type exposes an API which is similar to that of a typical map.
Example:
```rust
use bson::encode::*;
use bson::formattable::*;

//Building a document {foo: "bar", baz: 5.1}
let doc = BsonDocument::new();
doc.put(~"foo", (~"bar").to_bson_t());
doc.put(~"baz", (5.1).to_bson_t());
```

In addition to constructing them directly, these types can also be built from JSON-formatted strings. The parser in ```extra::json``` will return a Json object (which implements BsonFormattable) but the fields will not necessarily be ordered properly.
The BSON library also publishes its own JSON parser, which supports [extended JSON](http://docs.mongodb.org/manual/reference/mongodb-extended-json/) and guarantees that fields will be serialized in the order they were inserted.
Calling this JSON parser is done through the ```ObjParser``` trait's ```from_string``` method, or by using the ```to_bson_t``` method on a valid JSON ```~str```.
Example:
```rust
use bson::json_parse::*;

let json_string = ~"{\"foo\": \"bar\", \"baz\": 5}";
let parsed_doc = ObjParser::from_string<Document, ExtendedJsonParser<~[char]>>(json_string);
match parsed_doc {
    Ok(ref d) => //the string was parsed successfully; d is a Document
    Err(e) => //the string was not valid JSON and an error was encountered while parsing
}

//alternative method; won't throw an error as above if the string is improperly formatted
let json_obj = json_string.to_bson_t();
```

##### Encoding values
```Document```s and ```BsonDocument```s can be encoded into bytes via their ```to_bson``` methods. This will produce a ```~[u8]``` meeting the specifications outlined by the [specification](http://bsonspec.org).
Through this method, standard BSON types can easily be serialized. Any type ```Foo``` can also be serialized in this way if it implements the ```BsonFormattable``` trait.
Example:
```rust
use bson::encode::*;
use bson::formattable::*;

struct Foo {
    ...
}

impl BsonFormattable for Foo {
    fn to_bson_t(&self) -> Document {
        //a common implementation of this might be creating a map from
        //the names of the fields in a Foo to their values
    }

    fn from_bson_t(doc: &Document) -> Foo {
        //this method is the inverse of to_bson_t,
        //in general it makes sense for the two of them to roundtrip
    }
}

//now if you call (Foo::new()).to_bson_t().to_bson(),
//you will produce a BSON representation of a Foo
```

##### Decoding values
The ```~[u8]``` representation of data is not especially useful for modifying or viewing. A ```~[u8]``` can be easily transformed into a BsonDocument for easier manipulation.
Example:
```rust
use bson::decode::*;

let b: ~[u8] = /*get a bson document from somewhere*/
let p = BsonParser::new(b);
let doc = p.document();
match doc {
    Ok(ref d) => //b was a valid BSON string. d is the corresponding document.
    Err(e) => //b was not a valid BSON string.
}

let c: ~[u8] = /*get another bson document*/
let doc = decode(c); //the standalone 'decode' function handles creation of the parser
```

The ```BsonFormattable``` trait also contains a method called ```from_bson_t``` as mentioned above. This static method allows a Document to be converted into the implementing type. This allows a quick path to decode a ```~[u8]``` into a ```T:BsonFormattable```.
Example:
```rust
use bson::decode::*;
use bson::formattable::*;

struct Foo {
    ...
}

impl BsonFormattable for Foo {
    ...
}

let b: ~[u8] = /*get a bson string from somewhere*/
let myfoo = BsonFormattable::from_bson_t::<Foo>(&Embedded(~decode(b).unwrap()));
//here it is assumed b was decoded successfully, though a match could be done
//the Embedded constructor is needed because decode returns a BsonDocument,
//while from_bson_t expects a document
```


## Roadmap

- Replication set support
- Implement read preferences
- Documentation to the [API site](http://api.mongodb.org)
- Thorough test suite for CRUD functionality
- GridFS implementation

To be continued...
