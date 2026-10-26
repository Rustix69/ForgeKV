//! Asynchronous TCP server (Tokio) for the protocol in [`crate::protocol`].
//!
//! # Resource bounds
//! - **Connections:** at most `max_connections` (a semaphore). An extra
//!   connection gets one BUSY response and is closed.
//! - **Engine work:** every storage call runs in `spawn_blocking` (it may
//!   fsync) behind a second semaphore of `max_inflight_ops` permits, so the
//!   blocking pool cannot grow without bound and the async worker threads
//!   never block on disk. A request that cannot get a permit within
//!   `busy_timeout` is answered BUSY; the connection stays open.
//! - **Memory per connection:** frame lengths are checked against the
//!   protocol limit before anything is read, and the payload buffer grows
//!   only as bytes actually arrive (a client that declares a large frame and
//!   sends nothing costs nothing) and never beyond the frame length. While a
//!   request is decoded, the frame and the decoded key/value coexist briefly
//!   (about `2 * max_frame`); then the frame is freed. Requests on a
//!   connection are processed one at a time, in order, so pipelining is
//!   bounded by TCP buffers. There is no global memory budget: the total is
//!   bounded by `max_connections` times that per-connection amount.
//! - **Time:** `idle_timeout` while waiting for the first byte of a request,
//!   `request_timeout` to receive the rest of it (slowloris defence),
//!   `write_timeout` for each response.
//!
//! # Shutdown
//! When the `shutdown` future completes: the listener is dropped (new
//! connections are refused), idle connections close, a connection that is
//! mid-request finishes that request and then closes, and after at most
//! `shutdown_grace` the remaining tasks are aborted. Finally the engine is
//! shut down (`Db::shutdown`, which syncs the WAL) on a blocking thread.

use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::engine::Db;
use crate::error::Error;
use crate::protocol::{
    DecodeError, FRAME_HEADER_LEN, Limits, Request, Response, check_frame_len, decode_request,
    encode_response, frame,
};

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub max_connections: usize,
    pub max_inflight_ops: usize,
    pub idle_timeout: Duration,
    pub request_timeout: Duration,
    pub write_timeout: Duration,
    /// How long a request may wait for an engine permit before BUSY.
    pub busy_timeout: Duration,
    /// How long shutdown waits for connections to finish.
    pub shutdown_grace: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            max_connections: 1024,
            max_inflight_ops: 64,
            idle_timeout: Duration::from_secs(60),
            request_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            busy_timeout: Duration::from_millis(100),
            shutdown_grace: Duration::from_secs(5),
        }
    }
}

/// Server counters (monotonic).
#[derive(Debug, Default)]
pub struct ServerStats {
    pub connections_accepted: AtomicU64,
    pub connections_rejected: AtomicU64,
    pub requests: AtomicU64,
    pub bad_requests: AtomicU64,
    pub too_large: AtomicU64,
    pub busy: AtomicU64,
    pub internal_errors: AtomicU64,
    pub timeouts: AtomicU64,
}

impl ServerStats {
    fn inc(c: &AtomicU64) {
        c.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get(c: &AtomicU64) -> u64 {
        c.load(Ordering::Relaxed)
    }
}

struct Ctx {
    db: Arc<Db>,
    cfg: ServerConfig,
    limits: Limits,
    ops: Arc<Semaphore>,
    stats: Arc<ServerStats>,
}

/// Serve `listener` until `shutdown` completes, then drain and shut the
/// engine down. Returns the engine's shutdown result.
pub async fn serve(
    listener: TcpListener,
    db: Arc<Db>,
    cfg: ServerConfig,
    stats: Arc<ServerStats>,
    shutdown: impl Future<Output = ()>,
) -> crate::Result<()> {
    let opts = db.options();
    let ctx = Arc::new(Ctx {
        limits: Limits {
            max_key: opts.max_key_len,
            max_value: opts.max_value_len,
        },
        ops: Arc::new(Semaphore::new(cfg.max_inflight_ops)),
        db: Arc::clone(&db),
        cfg: cfg.clone(),
        stats,
    });
    let connections = Arc::new(Semaphore::new(cfg.max_connections));
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut tasks = JoinSet::new();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            () = &mut shutdown => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _peer)) => match Arc::clone(&connections).try_acquire_owned() {
                    Ok(permit) => {
                        ServerStats::inc(&ctx.stats.connections_accepted);
                        tasks.spawn(connection(stream, Arc::clone(&ctx), stop_rx.clone(), permit));
                    }
                    Err(_) => {
                        ServerStats::inc(&ctx.stats.connections_rejected);
                        tasks.spawn(reject(stream, cfg.write_timeout));
                    }
                },
                Err(e) => {
                    // E.g. out of file descriptors: report and back off
                    // instead of spinning.
                    eprintln!("forgekv-server: accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            },
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {}
        }
    }

    drop(listener);
    let _ = stop_tx.send(true);
    let drained = timeout(cfg.shutdown_grace, async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    drop(ctx);
    tokio::task::spawn_blocking(move || db.shutdown())
        .await
        .map_err(|_| Error::Poisoned)?
}

async fn reject(mut stream: TcpStream, write_timeout: Duration) {
    let bytes = frame(&encode_response(&Response::Busy(
        "too many connections".into(),
    )));
    let _ = timeout(write_timeout, stream.write_all(&bytes)).await;
    let _ = stream.shutdown().await;
}

/// Outcome of reading one frame.
enum Frame {
    Payload(Vec<u8>),
    /// A well-delimited frame that is invalid (respond, keep going).
    Bad(String),
    /// Cannot resynchronise (respond, close).
    TooLarge(String),
}

async fn read_frame<R: AsyncRead + Unpin>(r: &mut R, max_frame: usize) -> io::Result<Frame> {
    let mut header = [0u8; FRAME_HEADER_LEN];
    r.read_exact(&mut header).await?;
    let len = match check_frame_len(u32::from_be_bytes(header), max_frame) {
        Ok(len) => len,
        Err(DecodeError::Bad(m)) => return Ok(Frame::Bad(m)),
        Err(DecodeError::TooLarge(m)) => return Ok(Frame::TooLarge(m)),
    };
    // Grows with the data actually received, doubling but never past `len`
    // (`read_to_end` would round the capacity up to 2x the frame length).
    let mut payload = Vec::new();
    while payload.len() < len {
        if payload.len() == payload.capacity() {
            let more = payload.capacity().max(READ_CHUNK).min(len - payload.len());
            payload.reserve_exact(more);
        }
        let want = (payload.capacity().min(len) - payload.len()) as u64;
        if (&mut *r).take(want).read_buf(&mut payload).await? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
    }
    Ok(Frame::Payload(payload))
}

/// First allocation for a request payload; later growth doubles.
const READ_CHUNK: usize = 16 * 1024;

async fn connection(
    stream: TcpStream,
    ctx: Arc<Ctx>,
    mut stop: watch::Receiver<bool>,
    _permit: OwnedSemaphorePermit,
) {
    let _ = stream.set_nodelay(true);
    let (rd, mut wr) = stream.into_split();
    let mut rd = BufReader::new(rd);
    let max_frame = ctx.limits.max_frame();
    let stats = &ctx.stats;
    loop {
        if *stop.borrow() {
            break;
        }
        // Wait for the first byte of the next request (or EOF / idle / stop).
        let ready = tokio::select! {
            r = timeout(ctx.cfg.idle_timeout, rd.fill_buf()) => r,
            _ = stop.changed() => break,
        };
        match ready {
            Err(_) => {
                ServerStats::inc(&stats.timeouts);
                break;
            }
            Ok(Err(_)) => break,
            Ok(Ok([])) => break, // clean EOF between requests
            Ok(Ok(_)) => {}
        }
        // The rest of the request must arrive within `request_timeout`.
        let payload = match timeout(ctx.cfg.request_timeout, read_frame(&mut rd, max_frame)).await {
            Err(_) => {
                ServerStats::inc(&stats.timeouts);
                break;
            }
            Ok(Err(_)) => break, // disconnected mid-frame
            Ok(Ok(Frame::Payload(p))) => p,
            Ok(Ok(Frame::Bad(m))) => {
                ServerStats::inc(&stats.bad_requests);
                if respond(&mut wr, &Response::BadRequest(m), &ctx)
                    .await
                    .is_err()
                {
                    break;
                }
                continue;
            }
            Ok(Ok(Frame::TooLarge(m))) => {
                ServerStats::inc(&stats.too_large);
                let _ = respond(&mut wr, &Response::TooLarge(m), &ctx).await;
                break;
            }
        };
        ServerStats::inc(&stats.requests);
        let decoded = decode_request(&payload, &ctx.limits);
        // The request owns copies of key and value; free the raw frame now.
        drop(payload);
        let (response, close) = match decoded {
            Ok(req) => (execute(req, &ctx).await, false),
            Err(DecodeError::Bad(m)) => {
                ServerStats::inc(&stats.bad_requests);
                (Response::BadRequest(m), false)
            }
            Err(DecodeError::TooLarge(m)) => {
                ServerStats::inc(&stats.too_large);
                (Response::TooLarge(m), true)
            }
        };
        if respond(&mut wr, &response, &ctx).await.is_err() || close {
            break;
        }
    }
    let _ = wr.shutdown().await;
}

async fn respond(wr: &mut OwnedWriteHalf, r: &Response, ctx: &Ctx) -> io::Result<()> {
    let bytes = frame(&encode_response(r));
    match timeout(ctx.cfg.write_timeout, wr.write_all(&bytes)).await {
        Ok(r) => r,
        Err(_) => {
            ServerStats::inc(&ctx.stats.timeouts);
            Err(io::ErrorKind::TimedOut.into())
        }
    }
}

async fn execute(req: Request, ctx: &Ctx) -> Response {
    if req == Request::Ping {
        return Response::Ok;
    }
    let permit = match timeout(ctx.cfg.busy_timeout, Arc::clone(&ctx.ops).acquire_owned()).await {
        Ok(Ok(p)) => p,
        _ => {
            ServerStats::inc(&ctx.stats.busy);
            return Response::Busy("server busy, retry later".into());
        }
    };
    let db = Arc::clone(&ctx.db);
    // The permit moves into the blocking task, so it is held until the
    // engine call returns even if this connection goes away.
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        run(&db, req)
    })
    .await;
    match result {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => map_engine_error(e, ctx),
        Err(join) => {
            ServerStats::inc(&ctx.stats.internal_errors);
            eprintln!("forgekv-server: engine task failed: {join}");
            Response::Internal("internal error".into())
        }
    }
}

fn run(db: &Db, req: Request) -> crate::Result<Response> {
    Ok(match req {
        Request::Ping => Response::Ok,
        Request::Get { key } => match db.get(&key)? {
            Some(v) => Response::Value(v),
            None => Response::NotFound,
        },
        Request::Put { key, value } => {
            db.put(&key, &value)?;
            Response::Ok
        }
        Request::Delete { key } => {
            db.delete(&key)?;
            Response::Ok
        }
    })
}

fn map_engine_error(e: Error, ctx: &Ctx) -> Response {
    match e {
        Error::InvalidArgument(m) => {
            ServerStats::inc(&ctx.stats.bad_requests);
            Response::BadRequest(m)
        }
        Error::Closed => {
            ServerStats::inc(&ctx.stats.busy);
            Response::Busy("server is shutting down".into())
        }
        other => {
            ServerStats::inc(&ctx.stats.internal_errors);
            eprintln!("forgekv-server: storage error: {other}");
            Response::Internal("storage error".into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn framed(payload: &[u8]) -> Vec<u8> {
        frame(payload)
    }

    /// Regression (audit): `read_to_end` grew the buffer to up to twice the
    /// frame length, breaking the documented per-connection bound.
    #[tokio::test]
    async fn payload_capacity_never_exceeds_the_frame_length() {
        for len in [1usize, 100, READ_CHUNK, READ_CHUNK + 1, 600_000, 1_048_585] {
            let payload = vec![7u8; len];
            let bytes = framed(&payload);
            let mut r = &bytes[..];
            match read_frame(&mut r, 2 << 20).await.unwrap() {
                Frame::Payload(p) => {
                    assert_eq!(p, payload);
                    assert!(p.capacity() <= len, "len {len}: capacity {}", p.capacity());
                }
                _ => panic!("len {len}: not a payload"),
            }
            assert!(r.is_empty());
        }
    }

    #[tokio::test]
    async fn the_next_frame_is_not_consumed() {
        let mut bytes = framed(b"first");
        bytes.extend(framed(b"second"));
        let mut r = &bytes[..];
        for want in [&b"first"[..], b"second"] {
            match read_frame(&mut r, 64).await.unwrap() {
                Frame::Payload(p) => assert_eq!(p, want),
                _ => panic!("not a payload"),
            }
        }
    }

    #[tokio::test]
    async fn eof_inside_the_payload_is_an_error() {
        let bytes = framed(&[1u8; 100]);
        for cut in [5, 50, 103] {
            let mut r = &bytes[..cut];
            let err = read_frame(&mut r, 1024).await.err().unwrap();
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "cut {cut}");
        }
    }
}
