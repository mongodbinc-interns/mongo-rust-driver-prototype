use std::io::{BufReader, Read, Result, Write};
#[cfg(feature = "ssl")]
use std::io::{Error, ErrorKind};
use std::net::{SocketAddr, TcpStream};

#[cfg(feature = "ssl")]
use openssl::ssl::{Ssl, SslContext, SslFiletype, SslMethod, SslOptions, SslStream, SslVerifyMode};

/// Encapsulates the functionality for how to connect to the server.
#[derive(Clone, Debug)]
pub enum StreamConnector {
    /// Connect to the server through a regular TCP stream.
    Tcp,
    #[cfg(feature = "ssl")]
    /// Connect to the server through a TCP stream encrypted with SSL.
    ///
    /// Note that it's invalid to have one of certificate_file and key_file set but not the other.
    Ssl {
        ca_file: Option<String>,
        certificate_file: Option<String>,
        key_file: Option<String>,
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
    pub fn with_ssl(
        ca_file: Option<&str>,
        certificate_file: &str,
        key_file: &str,
        verify_peer: bool,
    ) -> Self {
        StreamConnector::Ssl {
            ca_file: ca_file.map(String::from),
            certificate_file: Some(String::from(certificate_file)),
            key_file: Some(String::from(key_file)),
            verify_peer: verify_peer,
        }
    }

    #[cfg(feature = "ssl")]
    /// Creates an unauthenticated StreamConnector that will connect with SSL encryption.
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
    /// `verify_peer` - Whether or not to verify that the server's certificate is trusted.
    pub fn with_unauthenticated_ssl(ca_file: Option<&str>, verify_peer: bool) -> Self {
        StreamConnector::Ssl {
            ca_file: ca_file.map(String::from),
            certificate_file: None,
            key_file: None,
            verify_peer: verify_peer,
        }
    }

    pub fn connect(&self, hostname: &str, port: u16) -> Result<Stream> {
        match *self {
            StreamConnector::Tcp => {
                let stream = TcpStream::connect((hostname, port))?;
                stream.set_nodelay(true)?;
                Ok(Stream::Tcp {
                    read_half: BufReader::new(stream.try_clone()?),
                    write_half: stream,
                })
            }
            #[cfg(feature = "ssl")]
            StreamConnector::Ssl {
                ref ca_file,
                ref certificate_file,
                ref key_file,
                verify_peer,
            } => {
                let inner_stream = TcpStream::connect((hostname, port))?;
                inner_stream.set_nodelay(true)?;

                let mut ssl_context = SslContext::builder(SslMethod::tls())?;
                ssl_context.set_cipher_list("ALL:!EXPORT:!eNULL:!aNULL:HIGH:@STRENGTH")?;
                ssl_context.set_options(SslOptions::NO_SSLV2);
                ssl_context.set_options(SslOptions::NO_SSLV3);
                ssl_context.set_options(SslOptions::NO_COMPRESSION);

                if let Some(ca_file) = ca_file {
                    ssl_context.set_ca_file(ca_file)?;
                }

                if let &Some(ref file) = certificate_file {
                    ssl_context.set_certificate_file(file, SslFiletype::PEM)?;
                }
                if let &Some(ref file) = key_file {
                    ssl_context.set_private_key_file(file, SslFiletype::PEM)?;
                }

                let verify = if verify_peer {
                    SslVerifyMode::PEER
                } else {
                    SslVerifyMode::NONE
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
    Tcp {
        read_half: BufReader<TcpStream>,
        write_half: TcpStream,
    },
    #[cfg(feature = "ssl")]
    Ssl(SslStream<TcpStream>),
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match *self {
            Stream::Tcp {
                ref mut read_half, ..
            } => read_half.read(buf),
            #[cfg(feature = "ssl")]
            Stream::Ssl(ref mut s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        match *self {
            Stream::Tcp {
                ref mut write_half, ..
            } => write_half.write(buf),
            #[cfg(feature = "ssl")]
            Stream::Ssl(ref mut s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> Result<()> {
        match *self {
            Stream::Tcp {
                ref mut write_half, ..
            } => write_half.flush(),
            #[cfg(feature = "ssl")]
            Stream::Ssl(ref mut s) => s.flush(),
        }
    }
}

impl Stream {
    pub fn peer_addr(&self) -> Result<SocketAddr> {
        match *self {
            Stream::Tcp { ref write_half, .. } => write_half.peer_addr(),
            #[cfg(feature = "ssl")]
            Stream::Ssl(ref stream) => stream.get_ref().peer_addr(),
        }
    }
}
