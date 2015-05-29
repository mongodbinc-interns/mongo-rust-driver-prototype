MongoDB Rust Driver Prototype
=============================

This branch contains active development on a new driver written for Rust 1.0.

The API and implementation are currently subject to change at any time. You must not use this driver in production as it is still under development and is in no way supported by 10gen. We absolutely encourage you to experiment with it and provide us feedback on the API, design, and implementation. Bug reports and suggestions for improvements are welcomed, as are pull requests.

## Installation

#### Dependencies
- [Rust 1.0 with Cargo](http://rust-lang.org)
- [libbson](https://github.com/mongodb/libbson)

The Rust driver uses libbson internally. To build the driver:
- Install the libbson prerequisites ```automake```, ```autoconf```, and ```libtool```.
- Clone libbson and build it:
```
git clone https://github.com/mongodb/libbson
cd libbson
./autogen.sh
make && sudo make install
```

#### Importing
To use the MongoDB driver in your code, add the package to your ```Cargo.toml```.
```
[dependencies]
mongodb="0.1.0"
```

Then, import the libc and driver libraries within your code.
```rust
extern crate libc;
extern crate mongodb;
```

#### Documentation
Documentation is built using Cargo. Generated documentation using ```cargo doc``` can be found under the _target/doc/_ folder.