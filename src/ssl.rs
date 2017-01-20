use std::io::{self, ErrorKind};
use std::net::{SocketAddr, TcpStream};

use stream::Stream;

use openssl::ssl::{Ssl, SslMethod, SslContext, SslStream, SSL_OP_NO_COMPRESSION, SSL_OP_NO_SSLV2,
                   SSL_OP_NO_SSLV3, SSL_VERIFY_NONE, SSL_VERIFY_PEER};
use openssl::x509::X509_FILETYPE_PEM;

#[derive(Clone, Debug)]
pub struct SslConfig {
    /// Path file containing list of trusted CA certificates.
    pub ca_file: String,
    /// Path to file containing client certificate.
    pub certificate_file: String,
    /// Path to file containing client private key.
    pub key_file: String,
    /// Whether the peer certificate should be verified.
    pub verify_peer: bool,
}

impl SslConfig {
    pub fn new(ca_file: String,
               certificate_file: String,
               key_file: String,
               verify_peer: bool)
               -> Self {
        SslConfig {
            ca_file: ca_file,
            certificate_file: certificate_file,
            key_file: key_file,
            verify_peer: verify_peer,
        }
    }
}

impl Stream for SslStream<TcpStream> {
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.get_ref().peer_addr()
    }
}

pub fn connect(hostname: &str,
               port: u16,
               SslConfig { ca_file, certificate_file, key_file, verify_peer }: SslConfig)
               -> io::Result<SslStream<TcpStream>> {
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

    ssl.connect(inner_stream).map_err(|e| io::Error::new(ErrorKind::Other, e))
}
