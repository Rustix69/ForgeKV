//! Minimal async client for the ForgeKV protocol (used by tests and, later,
//! benchmarks). One request at a time via the convenience methods, or
//! pipelined with [`Client::send`] / [`Client::recv`].

use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::protocol::{
    FRAME_HEADER_LEN, OP_DELETE, OP_GET, OP_PING, OP_PUT, Request, Response, decode_response,
    encode_request, frame,
};

#[derive(Debug)]
pub enum ClientError {
    Io(io::Error),
    /// The server sent something that does not follow the protocol.
    Protocol(String),
    /// The server answered with an error status.
    Status(Response),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Io(e) => write!(f, "i/o error: {e}"),
            ClientError::Protocol(m) => write!(f, "protocol error: {m}"),
            ClientError::Status(r) => write!(f, "server error: {r:?}"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<io::Error> for ClientError {
    fn from(e: io::Error) -> Self {
        ClientError::Io(e)
    }
}

pub struct Client {
    stream: BufReader<TcpStream>,
    max_value: usize,
}

impl Client {
    pub async fn connect(addr: impl ToSocketAddrs) -> io::Result<Client> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Client {
            stream: BufReader::new(stream),
            max_value: crate::Options::default().max_value_len,
        })
    }

    /// Largest GET value this client accepts (default: the engine default).
    pub fn set_max_value(&mut self, max_value: usize) {
        self.max_value = max_value;
    }

    /// Write one request without waiting for the response.
    pub async fn send(&mut self, req: &Request) -> io::Result<()> {
        self.stream
            .get_mut()
            .write_all(&frame(&encode_request(req)))
            .await
    }

    /// Read the next response; `op` is the opcode of the matching request.
    pub async fn recv(&mut self, op: u8) -> Result<Response, ClientError> {
        let mut header = [0u8; FRAME_HEADER_LEN];
        self.stream.read_exact(&mut header).await?;
        let len = u32::from_be_bytes(header) as usize;
        // Status + length + value, or status + a short message.
        let limit = 1 + 4 + self.max_value.max(64 * 1024);
        if len == 0 || len > limit {
            return Err(ClientError::Protocol(format!("bad response length {len}")));
        }
        let mut payload = Vec::new();
        let got = (&mut self.stream)
            .take(len as u64)
            .read_to_end(&mut payload)
            .await?;
        if got < len {
            return Err(ClientError::Io(io::ErrorKind::UnexpectedEof.into()));
        }
        decode_response(op, &payload, self.max_value).map_err(ClientError::Protocol)
    }

    /// Send a request and return the raw response (including error statuses).
    pub async fn call(&mut self, req: &Request) -> Result<Response, ClientError> {
        self.send(req).await?;
        self.recv(req.op()).await
    }

    async fn expect_ok(&mut self, req: &Request) -> Result<(), ClientError> {
        match self.call(req).await? {
            Response::Ok => Ok(()),
            other => Err(ClientError::Status(other)),
        }
    }

    pub async fn ping(&mut self) -> Result<(), ClientError> {
        debug_assert_eq!(Request::Ping.op(), OP_PING);
        self.expect_ok(&Request::Ping).await
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        let req = Request::Get { key: key.to_vec() };
        debug_assert_eq!(req.op(), OP_GET);
        match self.call(&req).await? {
            Response::Value(v) => Ok(Some(v)),
            Response::NotFound => Ok(None),
            other => Err(ClientError::Status(other)),
        }
    }

    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), ClientError> {
        let req = Request::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        debug_assert_eq!(req.op(), OP_PUT);
        self.expect_ok(&req).await
    }

    pub async fn delete(&mut self, key: &[u8]) -> Result<(), ClientError> {
        let req = Request::Delete { key: key.to_vec() };
        debug_assert_eq!(req.op(), OP_DELETE);
        self.expect_ok(&req).await
    }
}
