//! Client/server wire protocol (PLAN.md Phase 6). Pure encoding and
//! decoding; no I/O. All integers are **big-endian** (network order),
//! unlike the little-endian on-disk formats.
//!
//! ```text
//! frame    = len u32 | payload[len]            1 <= len <= max_frame
//! request  = op u8 | body
//!   1 PING    (empty)
//!   2 GET     key_len u32 | key
//!   3 PUT     key_len u32 | key | val_len u32 | value
//!   4 DELETE  key_len u32 | key
//! response = status u8 | body
//!   0 OK          GET: val_len u32 | value; PING/PUT/DELETE: empty
//!   1 NOT_FOUND   (GET only)
//!   2 BAD_REQUEST utf-8 message; the connection stays open
//!   3 TOO_LARGE   utf-8 message; the server closes the connection
//!   4 BUSY        utf-8 message; the connection stays open
//!   5 INTERNAL    utf-8 message (generic; details stay in the server)
//! ```
//!
//! Responses are sent in request order. A client may pipeline requests.
//!
//! # Malformed input
//! - A frame length above `max_frame` cannot be resynchronised: the server
//!   answers TOO_LARGE and closes. Nothing is allocated based on an
//!   unchecked length.
//! - A zero-length frame carries no payload to skip, so the stream stays in
//!   sync: BAD_REQUEST, and the connection stays open.
//! - Inside a valid frame: unknown opcode, short body, length fields that
//!   overrun the body, trailing bytes, or an empty key -> BAD_REQUEST; a key
//!   or value above the configured limits -> TOO_LARGE (and close, as above).
//!
//! There is no version handshake in v1; the protocol evolves by adding
//! opcodes and statuses.

use crate::codec::Cur;

pub const FRAME_HEADER_LEN: usize = 4;

pub const OP_PING: u8 = 1;
pub const OP_GET: u8 = 2;
pub const OP_PUT: u8 = 3;
pub const OP_DELETE: u8 = 4;

pub const STATUS_OK: u8 = 0;
pub const STATUS_NOT_FOUND: u8 = 1;
pub const STATUS_BAD_REQUEST: u8 = 2;
pub const STATUS_TOO_LARGE: u8 = 3;
pub const STATUS_BUSY: u8 = 4;
pub const STATUS_INTERNAL: u8 = 5;

/// Size limits enforced while decoding requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_key: usize,
    pub max_value: usize,
}

impl Limits {
    /// The largest legal request payload: a PUT with a maximal key and value.
    pub fn max_frame(&self) -> usize {
        1 + 4 + self.max_key + 4 + self.max_value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Ping,
    Get { key: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl Request {
    pub fn op(&self) -> u8 {
        match self {
            Request::Ping => OP_PING,
            Request::Get { .. } => OP_GET,
            Request::Put { .. } => OP_PUT,
            Request::Delete { .. } => OP_DELETE,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    /// PING/PUT/DELETE succeeded.
    Ok,
    /// GET found a value (possibly empty).
    Value(Vec<u8>),
    NotFound,
    BadRequest(String),
    TooLarge(String),
    Busy(String),
    Internal(String),
}

/// Why a request payload was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// Respond BAD_REQUEST; the connection may continue.
    Bad(String),
    /// Respond TOO_LARGE and close.
    TooLarge(String),
}

/// `len | payload`.
pub fn frame(payload: &[u8]) -> Vec<u8> {
    let mut f = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    f.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    f.extend_from_slice(payload);
    f
}

/// Validate a frame header before anything is allocated for the payload.
pub fn check_frame_len(len: u32, max_frame: usize) -> Result<usize, DecodeError> {
    let len = len as usize;
    if len == 0 {
        return Err(DecodeError::Bad("empty frame".into()));
    }
    if len > max_frame {
        return Err(DecodeError::TooLarge(format!(
            "frame of {len} bytes exceeds the {max_frame}-byte limit"
        )));
    }
    Ok(len)
}

fn put_bytes(b: &mut Vec<u8>, data: &[u8]) {
    b.extend_from_slice(&(data.len() as u32).to_be_bytes());
    b.extend_from_slice(data);
}

pub fn encode_request(r: &Request) -> Vec<u8> {
    let mut b = vec![r.op()];
    match r {
        Request::Ping => {}
        Request::Get { key } | Request::Delete { key } => put_bytes(&mut b, key),
        Request::Put { key, value } => {
            put_bytes(&mut b, key);
            put_bytes(&mut b, value);
        }
    }
    b
}

/// Big-endian reads on top of the shared bounds-checked cursor.
fn be_u32(c: &mut Cur<'_>) -> Result<usize, String> {
    let b = c.take(4)?;
    Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize)
}

fn read_field(c: &mut Cur<'_>, what: &str, max: usize) -> Result<Vec<u8>, DecodeError> {
    let len = be_u32(c).map_err(|e| DecodeError::Bad(format!("{what} length: {e}")))?;
    if len > max {
        return Err(DecodeError::TooLarge(format!(
            "{what} of {len} bytes exceeds the {max}-byte limit"
        )));
    }
    if len > c.remaining() {
        return Err(DecodeError::Bad(format!(
            "{what} length {len} overruns the frame"
        )));
    }
    let bytes = c.take(len).map_err(DecodeError::Bad)?;
    Ok(bytes.to_vec())
}

fn read_key(c: &mut Cur<'_>, limits: &Limits) -> Result<Vec<u8>, DecodeError> {
    let key = read_field(c, "key", limits.max_key)?;
    if key.is_empty() {
        return Err(DecodeError::Bad("empty key".into()));
    }
    Ok(key)
}

pub fn decode_request(payload: &[u8], limits: &Limits) -> Result<Request, DecodeError> {
    let mut c = Cur::new(payload);
    let op = c
        .u8()
        .map_err(|_| DecodeError::Bad("empty request".into()))?;
    let req = match op {
        OP_PING => Request::Ping,
        OP_GET => Request::Get {
            key: read_key(&mut c, limits)?,
        },
        OP_PUT => {
            let key = read_key(&mut c, limits)?;
            let value = read_field(&mut c, "value", limits.max_value)?;
            Request::Put { key, value }
        }
        OP_DELETE => Request::Delete {
            key: read_key(&mut c, limits)?,
        },
        other => return Err(DecodeError::Bad(format!("unknown opcode {other}"))),
    };
    if c.remaining() != 0 {
        return Err(DecodeError::Bad(format!(
            "{} trailing bytes after request",
            c.remaining()
        )));
    }
    Ok(req)
}

pub fn encode_response(r: &Response) -> Vec<u8> {
    let (status, body): (u8, &[u8]) = match r {
        Response::Ok => (STATUS_OK, &[]),
        Response::Value(v) => {
            let mut b = vec![STATUS_OK];
            put_bytes(&mut b, v);
            return b;
        }
        Response::NotFound => (STATUS_NOT_FOUND, &[]),
        Response::BadRequest(m) => (STATUS_BAD_REQUEST, m.as_bytes()),
        Response::TooLarge(m) => (STATUS_TOO_LARGE, m.as_bytes()),
        Response::Busy(m) => (STATUS_BUSY, m.as_bytes()),
        Response::Internal(m) => (STATUS_INTERNAL, m.as_bytes()),
    };
    let mut b = vec![status];
    b.extend_from_slice(body);
    b
}

/// Decode a response to a request with opcode `op` (an OK body depends on it).
/// `max_value` bounds a GET value.
pub fn decode_response(op: u8, payload: &[u8], max_value: usize) -> Result<Response, String> {
    let mut c = Cur::new(payload);
    let status = c.u8().map_err(|_| "empty response".to_string())?;
    let text = |c: &mut Cur<'_>| -> Result<String, String> {
        let rest = c.take(c.remaining())?;
        String::from_utf8(rest.to_vec()).map_err(|_| "message is not utf-8".to_string())
    };
    let r = match status {
        STATUS_OK if op == OP_GET => {
            let len = be_u32(&mut c)?;
            if len > max_value || len != c.remaining() {
                return Err(format!("bad value length {len}"));
            }
            Response::Value(c.take(len)?.to_vec())
        }
        STATUS_OK => Response::Ok,
        STATUS_NOT_FOUND => Response::NotFound,
        STATUS_BAD_REQUEST => Response::BadRequest(text(&mut c)?),
        STATUS_TOO_LARGE => Response::TooLarge(text(&mut c)?),
        STATUS_BUSY => Response::Busy(text(&mut c)?),
        STATUS_INTERNAL => Response::Internal(text(&mut c)?),
        other => return Err(format!("unknown status {other}")),
    };
    if c.remaining() != 0 {
        return Err("trailing bytes in response".into());
    }
    Ok(r)
}

#[cfg(test)]
mod tests {
    use super::*;

    const L: Limits = Limits {
        max_key: 16,
        max_value: 64,
    };

    fn all_requests() -> Vec<Request> {
        vec![
            Request::Ping,
            Request::Get { key: b"k".to_vec() },
            Request::Put {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            },
            Request::Put {
                key: b"e".to_vec(),
                value: vec![],
            },
            Request::Delete { key: b"d".to_vec() },
            Request::Put {
                key: vec![b'k'; 16],
                value: vec![7; 64],
            },
        ]
    }

    #[test]
    fn golden_bytes() {
        assert_eq!(frame(&[9, 8]), [0, 0, 0, 2, 9, 8]);
        assert_eq!(encode_request(&Request::Ping), [1]);
        assert_eq!(
            encode_request(&Request::Put {
                key: b"ab".to_vec(),
                value: b"x".to_vec()
            }),
            [3, 0, 0, 0, 2, b'a', b'b', 0, 0, 0, 1, b'x']
        );
        assert_eq!(
            encode_response(&Response::Value(b"v".to_vec())),
            [0, 0, 0, 0, 1, b'v']
        );
        assert_eq!(encode_response(&Response::NotFound), [1]);
        assert_eq!(encode_response(&Response::Busy("b".into())), [4, b'b']);
    }

    #[test]
    fn requests_round_trip() {
        for r in all_requests() {
            assert_eq!(decode_request(&encode_request(&r), &L).unwrap(), r);
        }
    }

    #[test]
    fn responses_round_trip() {
        let cases = [
            (OP_PUT, Response::Ok),
            (OP_GET, Response::Value(b"v".to_vec())),
            (OP_GET, Response::Value(vec![])),
            (OP_GET, Response::NotFound),
            (OP_PING, Response::BadRequest("bad".into())),
            (OP_PUT, Response::TooLarge("big".into())),
            (OP_GET, Response::Busy("busy".into())),
            (OP_DELETE, Response::Internal("oops".into())),
        ];
        for (op, r) in cases {
            assert_eq!(decode_response(op, &encode_response(&r), 64).unwrap(), r);
        }
        assert!(decode_response(OP_GET, &[9], 64).is_err());
        assert!(decode_response(OP_GET, &[0, 0, 0, 0, 5, 1], 64).is_err());
    }

    #[test]
    fn every_truncation_is_a_bad_request() {
        for r in all_requests() {
            let full = encode_request(&r);
            for cut in 0..full.len() {
                assert!(
                    matches!(decode_request(&full[..cut], &L), Err(DecodeError::Bad(_))),
                    "{r:?} cut at {cut}"
                );
            }
        }
    }

    #[test]
    fn malformed_requests() {
        let bad = |p: &[u8]| matches!(decode_request(p, &L), Err(DecodeError::Bad(_)));
        let too_large = |p: &[u8]| matches!(decode_request(p, &L), Err(DecodeError::TooLarge(_)));
        assert!(bad(&[]));
        assert!(bad(&[0]), "opcode 0");
        assert!(bad(&[99]), "unknown opcode");
        assert!(bad(&[1, 0]), "trailing byte after PING");
        assert!(bad(&[2, 0, 0, 0, 0]), "empty key");
        assert!(bad(&[2, 0, 0, 0, 5, b'a']), "key overruns frame");
        let mut trailing = encode_request(&Request::Get { key: b"k".to_vec() });
        trailing.push(0);
        assert!(bad(&trailing));
        assert!(
            too_large(&[2, 0, 0, 0, 17]),
            "key over limit, checked before reading it"
        );
        assert!(too_large(&[2, 0xff, 0xff, 0xff, 0xff]));
        let mut big_value = vec![3, 0, 0, 0, 1, b'k'];
        big_value.extend(65u32.to_be_bytes());
        assert!(too_large(&big_value));
    }

    #[test]
    fn frame_length_checks() {
        assert!(matches!(check_frame_len(0, 100), Err(DecodeError::Bad(_))));
        assert!(matches!(
            check_frame_len(101, 100),
            Err(DecodeError::TooLarge(_))
        ));
        assert!(matches!(
            check_frame_len(u32::MAX, 100),
            Err(DecodeError::TooLarge(_))
        ));
        assert_eq!(check_frame_len(100, 100).unwrap(), 100);
        assert_eq!(L.max_frame(), 1 + 4 + 16 + 4 + 64);
        // The largest legal request fits exactly.
        let biggest = encode_request(&all_requests()[5]);
        assert_eq!(biggest.len(), L.max_frame());
    }

    #[test]
    fn random_bytes_never_panic() {
        let mut x = 0x2545_F491_4F6C_DD1Du64;
        for _ in 0..20_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let len = (x % 40) as usize;
            let bytes: Vec<u8> = (0..len)
                .map(|i| (x.rotate_left(i as u32 * 7) & 0xff) as u8)
                .collect();
            let _ = decode_request(&bytes, &L);
            let _ = decode_response((x % 5) as u8, &bytes, 64);
        }
    }
}
