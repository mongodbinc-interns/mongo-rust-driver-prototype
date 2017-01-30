#[cfg(feature = "ssl")]
use std::io::{Error, ErrorKind};
use std::io::{Read, Result, Write};
use std::net::{SocketAddr, TcpStream};

#[cfg(feature = "ssl")]
use openssl::ssl::{Ssl, SslMethod, SslContext, SslStream, SSL_OP_NO_COMPRESSION, SSL_OP_NO_SSLV2,
                   SSL_OP_NO_SSLV3, SSL_VERIFY_NONE, SSL_VERIFY_PEER};
#[cfg(feature = "ssl")]
use openssl::x509::X509_FILETYPE_PEM;

/// Encapsulates the functionality for how to connect to the server.
#[derive(Clone)]
pub enum StreamConnector {
    /// Connect to the server through a regular TCP stream.
    Tcp,
    #[cfg(feature = "ssl")]
    /// Connect to the server through a TCP stream encrypted with SSL.
    Ssl {
        ca_file: String,
        certificate_file: String,
        key_file: String,
        verify_peer: bool,
    },
}

impl Default for StreamConnector {
    fn default() -> Self {
        StreamConnector::Tcp
    }
}

impl StreamConnector {
    #[cfg(feature = "ssl")]
    /// Creates a StreamConnector that will connect with SSL encryption.
    /// 
    /// The SSL connection will use the cipher with the longest key length available to both the
    /// server and client, with the following caveats:
    ///   * SSLv2 and SSlv3 are disabled
    ///   * Export-strength ciphers are disabled
    ///   * Ciphers not offering encryption are disabled
    ///   * Ciphers not offering authentication are disabled
    ///   * Ciphers with key lengths of 128 or fewer bits are disabled.
    ///
    /// Note that TLS compression is disabled for SSL connections.
    ///
    /// # Arguments
    ///
    /// `ca_file` - Path to the file containing trusted CA certificates.
    /// `certificate_file` - Path to the file containing the client certificate.
    /// `key_file` - Path to the file containing the client private key.
    /// `verify_peer` - Whether or not to verify that the server's certificate is trusted.
    pub fn with_ssl(ca_file: &str,
                    certificate_file: &str,
                    key_file: &str,
                    verify_peer: bool)
                    -> Self {
        StreamConnector::Ssl {
            ca_file: String::from(ca_file),
            certificate_file: String::from(certificate_file),
            key_file: String::from(key_file),
            verify_peer: verify_peer,
        }
    }

    pub fn connect(&self, hostname: &str, port: u16) -> Result<Stream> {
        match *self {
            StreamConnector::Tcp => TcpStream::connect((hostname, port)).map(Stream::Tcp),
            #[cfg(feature = "ssl")]
            StreamConnector::Ssl { ref ca_file,
                                   ref certificate_file,
                                   ref key_file,
                                   verify_peer } => {
                let inner_stream = TcpStream::connect((hostname, port))?;

                let mut ssl_context = SslContext::builder(SslMethod::tls())?;
                ssl_context.set_cipher_list("ALL:!EXPORT:!eNULL:!aNULL:HIGH:@STRENGTH")?;
                ssl_context.set_options(SSL_OP_NO_SSLV2);
                ssl_context.set_options(SSL_OP_NO_SSLV3);
                ssl_context.set_options(SSL_OP_NO_COMPRESSION);
                ssl_context.set_ca_file(ca_file)?;
                ssl_context.set_certificate_file(certificate_file, X509_FILETYPE_PEM)?;
                ssl_context.set_private_key_file(key_file, X509_FILETYPE_PEM)?;

                let verify = if verify_peer {
                    SSL_VERIFY_PEER
                } else {
                    SSL_VERIFY_NONE
                };
                ssl_context.set_verify(verify);

                let mut ssl = Ssl::new(&ssl_context.build())?;
                ssl.set_hostname(hostname)?;

                match ssl.connect(inner_stream) {
                    Ok(s) => Ok(Stream::Ssl(s)),
                    Err(e) => Err(Error::new(ErrorKind::Other, e)),
                }
            }

        }
    }
}

pub enum Stream {
    Tcp(TcpStream),
    #[cfg(feature = "ssl")]
    Ssl(SslStream<TcpStream>),
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match *self {
            Stream::Tcp(ref mut s) => s.read(buf),
            #[cfg(feature = "ssl")]
            Stream::Ssl(ref mut s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        match *self {
            Stream::Tcp(ref mut s) => s.write(buf),
            #[cfg(feature = "ssl")]
            Stream::Ssl(ref mut s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> Result<()> {
        match *self {
            Stream::Tcp(ref mut s) => s.flush(),
            #[cfg(feature = "ssl")]
            Stream::Ssl(ref mut s) => s.flush(),
        }
    }
}

impl Stream {
    pub fn peer_addr(&self) -> Result<SocketAddr> {
        match *self {
            Stream::Tcp(ref stream) => stream.peer_addr(),
            #[cfg(feature = "ssl")]
            Stream::Ssl(ref stream) => stream.get_ref().peer_addr(),
        }
    }
}
