//! Connection pooling for a single MongoDB server.
use error::Error::{self, ArgumentError, OperationError};
use error::Result;

use Client;
use coll::options::FindOptions;
use command_type::CommandType;
use connstring::Host;
use cursor::Cursor;
use stream::{Stream, StreamConnector};
use wire_protocol::flags::OpQueryFlags;

use bson::{bson, doc};
use bufstream::BufStream;

use std::fmt;
use std::sync::{Arc, Condvar, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

pub static DEFAULT_POOL_SIZE: usize = 5;

/// Handles threaded connections to a MongoDB server.
#[derive(Clone)]
pub struct ConnectionPool {
    /// The connection host.
    pub host: Host,
    // The socket pool.
    inner: Arc<Mutex<Pool>>,
    // A condition variable used for threads waiting for the pool
    // to be repopulated with available connections.
    wait_lock: Arc<Condvar>,
    stream_connector: StreamConnector,
}

impl fmt::Debug for ConnectionPool {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ConnectionPool")
            .field("host", &self.host)
            .finish()
    }
}

struct Pool {
    /// The maximum number of concurrent connections allowed.
    pub size: usize,
    // The current number of open connections.
    pub len: Arc<AtomicUsize>,
    // The idle socket pool.
    sockets: Vec<BufStream<Stream>>,
    // The pool iteration. When a server monitor fails to execute ismaster,
    // the connection pool is cleared and the iteration is incremented.
    iteration: usize,
}

/// Holds an available socket, with logic to return the socket
/// to the connection pool when dropped.
pub struct PooledStream {
    // This socket option will always be Some(stream) until it is
    // returned to the pool using take().
    socket: Option<BufStream<Stream>>,
    // A reference to the pool that the stream was taken from.
    pool: Arc<Mutex<Pool>>,
    // A reference to the waiting condvar associated with the pool.
    wait_lock: Arc<Condvar>,
    // The pool iteration at the moment of extraction.
    iteration: usize,
    // Whether the handshake occurred successfully.
    successful_handshake: bool,
}

impl PooledStream {
    /// Returns a reference to the socket.
    pub fn get_socket(&mut self) -> &mut BufStream<Stream> {
        self.socket.as_mut().unwrap()
    }
}

impl Drop for PooledStream {
    fn drop(&mut self) {
        // Don't add streams that couldn't successfully handshake to the pool.
        if !self.successful_handshake {
            return;
        }

        // Attempt to lock and return the socket to the pool,
        // or give up if the pool lock has been poisoned.
        if let Ok(mut locked) = self.pool.lock() {
            if self.iteration == locked.iteration {
                locked.sockets.push(self.socket.take().unwrap());
                // Notify waiting threads that the pool has been repopulated.
                self.wait_lock.notify_one();
            }
        }
    }
}

impl ConnectionPool {
    /// Returns a connection pool with a default size.
    pub fn new(host: Host, connector: StreamConnector) -> ConnectionPool {
        ConnectionPool::with_size(host, connector, DEFAULT_POOL_SIZE)
    }

    /// Returns a connection pool with a specified capped size.
    pub fn with_size(host: Host, connector: StreamConnector, size: usize) -> ConnectionPool {
        ConnectionPool {
            host: host,
            wait_lock: Arc::new(Condvar::new()),
            inner: Arc::new(Mutex::new(Pool {
                len: Arc::new(AtomicUsize::new(0)),
                size: size,
                sockets: Vec::with_capacity(size),
                iteration: 0,
            })),
            stream_connector: connector,
        }
    }

    /// Sets the maximum number of open connections.
    pub fn set_size(&self, size: usize) -> Result<()> {
        if size < 1 {
            Err(ArgumentError(String::from(
                "The connection pool size must be greater than zero.",
            )))
        } else {
            let mut locked = self.inner.lock()?;
            locked.size = size;
            Ok(())
        }
    }

    // Clear all open socket connections.
    pub fn clear(&self) {
        if let Ok(mut locked) = self.inner.lock() {
            locked.iteration += 1;
            locked.sockets.clear();
            locked.len.store(0, Ordering::SeqCst);
        }
    }

    /// Attempts to acquire a connected socket. If none are available and
    /// the pool has not reached its maximum size, a new socket will connect.
    /// Otherwise, the function will block until a socket is returned to the pool.
    pub fn acquire_stream(&self, client: Client) -> Result<PooledStream> {
        let mut locked = self.inner.lock()?;
        if locked.size == 0 {
            return Err(OperationError(String::from(
                "The connection pool does not allow connections; increase the size of the pool.",
            )));
        }

        loop {
            // Acquire available existing socket
            if let Some(stream) = locked.sockets.pop() {
                return Ok(PooledStream {
                    socket: Some(stream),
                    pool: self.inner.clone(),
                    wait_lock: self.wait_lock.clone(),
                    iteration: locked.iteration,
                    successful_handshake: true,
                });
            }

            // Attempt to make a new connection
            let len = locked.len.load(Ordering::SeqCst);
            if len < locked.size {
                let socket = self.connect()?;
                let mut stream = PooledStream {
                    socket: Some(socket),
                    pool: self.inner.clone(),
                    wait_lock: self.wait_lock.clone(),
                    iteration: locked.iteration,
                    successful_handshake: false,
                };

                self.handshake(client, &mut stream)?;
                let _ = locked.len.fetch_add(1, Ordering::SeqCst);
                return Ok(stream);
            }

            // Release lock and wait for pool to be repopulated
            locked = self.wait_lock.wait(locked)?;
        }
    }

    // Connects to a MongoDB server as defined by the initial configuration.
    fn connect(&self) -> Result<BufStream<Stream>> {
        match self.stream_connector.connect(
            &self.host.host_name[..],
            self.host.port,
        ) {
            Ok(s) => Ok(BufStream::new(s)),
            Err(e) => Err(Error::from(e)),
        }
    }

    // This sends the client metadata to the server as described by the handshake spec.
    //
    // See https://github.com/mongodb/specifications/blob/master/source/mongodb-handshake/handshake.rst
    fn handshake(&self, client: Client, stream: &mut PooledStream) -> Result<()> {
        let mut options = FindOptions::new();
        options.limit = Some(1);
        options.batch_size = Some(1);

        let flags = OpQueryFlags::with_find_options(&options);

        Cursor::query_with_stream(
            stream,
            client,
            String::from("local.$cmd"),
            flags,
            doc! {
                "isMaster": 1i32,
                "client": {
                    "driver": {
                        "name": ::DRIVER_NAME,
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                    "os": {
                        "type": ::std::env::consts::OS,
                        "architecture": ::std::env::consts::ARCH
                    }
                },
            },
            options,
            CommandType::IsMaster,
            false,
            None,
        )?;

        stream.successful_handshake = true;

        Ok(())
    }
}
