//! Connection pooling for a single MongoDB server.
use Error::{ArgumentError, OperationError};
use Result;

use connstring::Host;

use bufstream::BufStream;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Condvar, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};

#[cfg(feature = "ssl")]
use openssl::ssl::{Ssl, SslMethod, SslContext, SslStream, SSL_VERIFY_NONE};
#[cfg(feature = "ssl")]
use openssl::x509::X509_FILETYPE_PEM;

pub static DEFAULT_POOL_SIZE: usize = 5;

pub enum Stream {
    #[cfg(feature = "ssl")]
    Ssl(SslStream<TcpStream>),
    Tcp(TcpStream),
}

impl Stream {
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        match *self {
            #[cfg(feature = "ssl")]
            Stream::Ssl(ref stream) => stream.get_ref().peer_addr(),
            Stream::Tcp(ref stream) => stream.peer_addr(),
        }
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            #[cfg(feature = "ssl")]
            Stream::Ssl(ref mut stream) => stream.read(buf),
            Stream::Tcp(ref mut stream) => stream.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            #[cfg(feature = "ssl")]
            Stream::Ssl(ref mut stream) => stream.write(buf),
            Stream::Tcp(ref mut stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {
            #[cfg(feature = "ssl")]
            Stream::Ssl(ref mut stream) => stream.flush(),
            Stream::Tcp(ref mut stream) => stream.flush(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SslConfig {
    /// Path file containing list of trusted CA certificates.
    pub ca_file: String,
    /// Path to file containing client certificate.
    pub certificate_file: String,
    /// Path to file containing client private key.
    pub key_file: String,
}

impl SslConfig {
    pub fn new(ca_file: String, certificate_file: String, key_file: String) -> Self {
        SslConfig {
            ca_file: ca_file,
            certificate_file: certificate_file,
            key_file: key_file,
        }
    }
}

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
    ssl: Option<SslConfig>,
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
}

impl PooledStream {
    /// Returns a reference to the socket.
    pub fn get_socket(&mut self) -> &mut BufStream<Stream> {
        self.socket.as_mut().unwrap()
    }
}

impl Drop for PooledStream {
    fn drop(&mut self) {
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
    pub fn new(host: Host) -> ConnectionPool {
        ConnectionPool::with_size(host, DEFAULT_POOL_SIZE)
    }

    /// Returns a connection pool with a specified capped size.
    pub fn with_size(host: Host, size: usize) -> ConnectionPool {
        ConnectionPool::with_size_and_ssl(host, size, None)
    }

    pub fn with_ssl(host: Host, ssl: Option<SslConfig>) -> ConnectionPool {
        ConnectionPool::with_size_and_ssl(host, DEFAULT_POOL_SIZE, ssl)
    }

    /// Returns a connection pool with a specified capped size.
    pub fn with_size_and_ssl(host: Host, size: usize, ssl: Option<SslConfig>) -> ConnectionPool {
        ConnectionPool {
            host: host,
            wait_lock: Arc::new(Condvar::new()),
            inner: Arc::new(Mutex::new(Pool {
                len: Arc::new(ATOMIC_USIZE_INIT),
                size: size,
                sockets: Vec::with_capacity(size),
                iteration: 0,
            })),
            ssl: ssl,
        }
    }

    /// Sets the maximum number of open connections.
    pub fn set_size(&self, size: usize) -> Result<()> {
        if size < 1 {
            Err(ArgumentError(String::from("The connection pool size must be greater than zero.")))
        } else {
            let mut locked = try!(self.inner.lock());
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
    pub fn acquire_stream(&self) -> Result<PooledStream> {
        let mut locked = try!(self.inner.lock());
        if locked.size == 0 {
            return Err(OperationError(String::from("The connection pool does not allow \
                                                    connections; increase the size of the pool.")));
        }

        loop {
            // Acquire available existing socket
            if let Some(stream) = locked.sockets.pop() {
                return Ok(PooledStream {
                    socket: Some(stream),
                    pool: self.inner.clone(),
                    wait_lock: self.wait_lock.clone(),
                    iteration: locked.iteration,
                });
            }

            // Attempt to make a new connection
            let len = locked.len.load(Ordering::SeqCst);
            if len < locked.size {
                let socket = try!(self.connect());
                let _ = locked.len.fetch_add(1, Ordering::SeqCst);
                return Ok(PooledStream {
                    socket: Some(socket),
                    pool: self.inner.clone(),
                    wait_lock: self.wait_lock.clone(),
                    iteration: locked.iteration,
                });
            }

            // Release lock and wait for pool to be repopulated
            locked = try!(self.wait_lock.wait(locked));
        }
    }


    // Connects to a MongoDB server as defined by the initial
    #[allow(unreachable_code)] // Suppresses warning for `panic` when ssl is enabled.
    fn connect(&self) -> Result<BufStream<Stream>> {
        let host_name = &self.host.host_name;
        let port = self.host.port;
        let inner_stream = try!(TcpStream::connect((&host_name[..], port)));

        if let Some(SslConfig { ca_file: ref _ca_file,
                                certificate_file: ref _certificate_file,
                                key_file: ref _key_file }) = self.ssl {
            #[cfg(feature = "ssl")]
            {
                let mut ssl_context = SslContext::builder(SslMethod::tls())?;
                ssl_context.set_cipher_list("DEFAULT")?;
                ssl_context.set_ca_file(_ca_file)?;
                ssl_context.set_certificate_file(_certificate_file, X509_FILETYPE_PEM)?;
                ssl_context.set_private_key_file(_key_file, X509_FILETYPE_PEM)?;

                ssl_context.set_verify(SSL_VERIFY_NONE);
                let ssl = Ssl::new(&ssl_context.build())?;

                return Ok(BufStream::new(Stream::Ssl(ssl.connect(inner_stream)?)));
            }

            panic!("The client is trying to connect with SSL, but the `mongodb` crate was not \
                    compile with SSL enabled. To connect with SSL, first install OpenSSL (if you \
                    haven't already) and then recompile with the \"ssl\" feature enabled.");

        }

        Ok(BufStream::new(Stream::Tcp(inner_stream)))
    }
}
