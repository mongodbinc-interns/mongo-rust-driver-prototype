use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};

pub trait Stream: Read + Write + Send {
    fn peer_addr(&self) -> io::Result<SocketAddr>;
}

impl Stream for TcpStream {
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.peer_addr()
    }
}
