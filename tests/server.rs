//! Phase 6: protocol over TCP, malformed input, resource limits, timeouts,
//! concurrent clients, shutdown, and persistence across server restarts
//! (in-process and with the real `forgekv-server` binary, including kill -9).

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use forgekv::client::{Client, ClientError};
use forgekv::protocol::{self, OP_GET, OP_PING, OP_PUT, Request, Response};
use forgekv::server::{ServerConfig, ServerStats, serve};
use forgekv::{Db, Options};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);

fn opts() -> Options {
    Options {
        max_key_len: 64,
        max_value_len: 1024,
        memtable_bytes: 8 * 1024,
        l0_compaction_trigger: 3,
        ..Options::default()
    }
}

fn quick_cfg() -> ServerConfig {
    ServerConfig {
        idle_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        write_timeout: Duration::from_secs(5),
        shutdown_grace: Duration::from_secs(5),
        ..ServerConfig::default()
    }
}

struct TestServer {
    addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<forgekv::Result<()>>,
    db: Arc<Db>,
    stats: Arc<ServerStats>,
}

async fn start_with_db(db: Db, cfg: ServerConfig) -> TestServer {
    let db = Arc::new(db);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel::<()>();
    let stats = Arc::new(ServerStats::default());
    let task = tokio::spawn(serve(
        listener,
        Arc::clone(&db),
        cfg,
        Arc::clone(&stats),
        async {
            let _ = rx.await;
        },
    ));
    TestServer {
        addr,
        stop: Some(tx),
        task,
        db,
        stats,
    }
}

async fn start(dir: &Path, cfg: ServerConfig) -> TestServer {
    let dir = dir.to_path_buf();
    let db = tokio::task::spawn_blocking(move || Db::open(dir, opts()))
        .await
        .unwrap()
        .unwrap();
    start_with_db(db, cfg).await
}

impl TestServer {
    async fn stop(mut self) -> forgekv::Result<()> {
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
        timeout(T, self.task)
            .await
            .expect("server stop timed out")
            .unwrap()
    }

    async fn client(&self) -> Client {
        let mut c = Client::connect(self.addr).await.unwrap();
        c.set_max_value(opts().max_value_len);
        c
    }

    fn stat(&self, f: impl Fn(&ServerStats) -> &std::sync::atomic::AtomicU64) -> u64 {
        ServerStats::get(f(&self.stats))
    }
}

async fn raw(addr: SocketAddr) -> TcpStream {
    TcpStream::connect(addr).await.unwrap()
}

/// Read one response frame; `None` on EOF.
async fn read_response(s: &mut TcpStream, op: u8) -> Option<Response> {
    let mut h = [0u8; 4];
    match timeout(T, s.read_exact(&mut h))
        .await
        .expect("read timed out")
    {
        Ok(_) => {}
        Err(_) => return None,
    }
    let mut p = vec![0u8; u32::from_be_bytes(h) as usize];
    timeout(T, s.read_exact(&mut p)).await.unwrap().ok()?;
    Some(protocol::decode_response(op, &p, 1 << 20).unwrap())
}

/// The server closed the connection (EOF or reset) within `T`.
async fn assert_closed(s: &mut TcpStream) {
    let mut b = [0u8; 1];
    match timeout(T, s.read(&mut b))
        .await
        .expect("connection not closed")
    {
        Ok(0) | Err(_) => {}
        Ok(n) => panic!("expected close, read {n} bytes"),
    }
}

/// The server no longer accepts connections. A connect to `addr` is
/// normally refused, but once the listener is closed the OS may give the
/// port to another listener (a parallel test) or let the client connect to
/// itself, so "refused" alone is not a reliable check. What must hold is
/// that *this* server's accept counters do not move.
async fn assert_not_accepting(addr: SocketAddr, stats: &ServerStats) {
    let counts = |s: &ServerStats| {
        (
            ServerStats::get(&s.connections_accepted),
            ServerStats::get(&s.connections_rejected),
        )
    };
    let before = counts(stats);
    if let Ok(sock) = TcpStream::connect(addr).await {
        drop(sock);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        counts(stats),
        before,
        "the server accepted a connection after closing its listener"
    );
}

fn frame_of(req: &Request) -> Vec<u8> {
    protocol::frame(&protocol::encode_request(req))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn basic_operations_over_tcp() {
    let dir = TempDir::new().unwrap();
    let s = start(dir.path(), quick_cfg()).await;
    let mut c = s.client().await;
    c.ping().await.unwrap();
    assert_eq!(c.get(b"k").await.unwrap(), None);
    c.put(b"k", b"v").await.unwrap();
    assert_eq!(c.get(b"k").await.unwrap(), Some(b"v".to_vec()));
    c.put(b"k", b"").await.unwrap();
    assert_eq!(
        c.get(b"k").await.unwrap(),
        Some(vec![]),
        "empty value is not missing"
    );
    c.delete(b"k").await.unwrap();
    assert_eq!(c.get(b"k").await.unwrap(), None);
    c.delete(b"never").await.unwrap();
    s.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipelined_requests_are_answered_in_order() {
    let dir = TempDir::new().unwrap();
    let s = start(dir.path(), quick_cfg()).await;
    let mut c = s.client().await;
    let n = 200;
    let mut ops = vec![];
    for i in 0..n {
        let k = format!("k{i}").into_bytes();
        c.send(&Request::Put {
            key: k.clone(),
            value: format!("v{i}").into_bytes(),
        })
        .await
        .unwrap();
        c.send(&Request::Get { key: k }).await.unwrap();
        ops.push((OP_PUT, None));
        ops.push((OP_GET, Some(format!("v{i}").into_bytes())));
    }
    for (op, want) in ops {
        let r = c.recv(op).await.unwrap();
        match want {
            None => assert_eq!(r, Response::Ok),
            Some(v) => assert_eq!(r, Response::Value(v)),
        }
    }
    s.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_requests_get_bad_request_and_the_connection_survives() {
    let dir = TempDir::new().unwrap();
    let s = start(dir.path(), quick_cfg()).await;
    let mut sock = raw(s.addr).await;
    let cases: Vec<Vec<u8>> = vec![
        protocol::frame(&[]),                               // empty frame
        protocol::frame(&[99]),                             // unknown opcode
        protocol::frame(&[OP_PING, 0]),                     // trailing byte
        protocol::frame(&[OP_GET, 0, 0, 0, 0]),             // empty key
        protocol::frame(&[OP_GET, 0, 0, 0, 9, b'k']),       // key overruns frame
        protocol::frame(&[OP_PUT, 0, 0, 0, 1, b'k', 0, 0]), // truncated value length
    ];
    for bytes in cases {
        sock.write_all(&bytes).await.unwrap();
        let r = read_response(&mut sock, OP_GET).await.unwrap();
        assert!(matches!(r, Response::BadRequest(_)), "{bytes:?} -> {r:?}");
    }
    // Still usable.
    sock.write_all(&frame_of(&Request::Ping)).await.unwrap();
    assert_eq!(read_response(&mut sock, OP_PING).await, Some(Response::Ok));
    assert_eq!(s.stat(|x| &x.bad_requests), 6);
    s.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_frames_keys_and_values_close_the_connection() {
    let dir = TempDir::new().unwrap();
    let s = start(dir.path(), quick_cfg()).await;
    let o = opts();
    let big_key = Request::Get {
        key: vec![b'k'; o.max_key_len + 1],
    };
    let big_value = Request::Put {
        key: b"k".to_vec(),
        value: vec![0; o.max_value_len + 1],
    };
    let cases: Vec<Vec<u8>> = vec![
        // A header claiming 4 GiB; nothing else is ever sent.
        u32::MAX.to_be_bytes().to_vec(),
        // One byte over the frame limit.
        ((protocol::Limits {
            max_key: o.max_key_len,
            max_value: o.max_value_len,
        })
        .max_frame() as u32
            + 1)
        .to_be_bytes()
        .to_vec(),
        frame_of(&big_key),
        frame_of(&big_value),
    ];
    for bytes in cases {
        let mut sock = raw(s.addr).await;
        sock.write_all(&bytes).await.unwrap();
        let r = read_response(&mut sock, OP_PUT).await.unwrap();
        assert!(matches!(r, Response::TooLarge(_)), "{r:?}");
        assert_closed(&mut sock).await;
    }
    // The maximum legal request still works.
    let mut c = s.client().await;
    let k = vec![b'k'; o.max_key_len];
    let v = vec![7u8; o.max_value_len];
    c.put(&k, &v).await.unwrap();
    assert_eq!(c.get(&k).await.unwrap(), Some(v));
    assert_eq!(s.stat(|x| &x.too_large), 4);
    s.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disconnects_mid_request_do_not_affect_other_clients() {
    let dir = TempDir::new().unwrap();
    let s = start(dir.path(), quick_cfg()).await;
    // Half a header, half a payload, and a full request without reading the answer.
    let full = frame_of(&Request::Put {
        key: b"half".to_vec(),
        value: b"value".to_vec(),
    });
    for cut in [2, full.len() - 3, full.len()] {
        let mut sock = raw(s.addr).await;
        sock.write_all(&full[..cut]).await.unwrap();
        drop(sock);
    }
    let mut c = s.client().await;
    c.ping().await.unwrap();
    c.put(b"other", b"fine").await.unwrap();
    assert_eq!(c.get(b"other").await.unwrap(), Some(b"fine".to_vec()));
    // A fully sent request is executed even if the client vanished.
    let v = c.get(b"half").await.unwrap();
    assert!(v.is_none() || v == Some(b"value".to_vec()));
    let stats = Arc::clone(&s.stats);
    s.stop().await.unwrap();
    // After shutdown every connection has been handled: the two cuts inside
    // a frame are counted as partial frames; the complete request and the
    // clean EOFs between requests are not.
    assert_eq!(ServerStats::get(&stats.partial_frames), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_and_slow_clients_are_disconnected() {
    let dir = TempDir::new().unwrap();
    let cfg = ServerConfig {
        idle_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_millis(300),
        ..quick_cfg()
    };
    let s = start(dir.path(), cfg).await;
    // Idle: connects and never sends.
    let mut idle = raw(s.addr).await;
    // Slowloris: sends a header and part of the payload, then stalls.
    let mut slow = raw(s.addr).await;
    let f = frame_of(&Request::Put {
        key: b"k".to_vec(),
        value: b"value".to_vec(),
    });
    slow.write_all(&f[..6]).await.unwrap();
    let started = std::time::Instant::now();
    assert_closed(&mut idle).await;
    assert_closed(&mut slow).await;
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(s.stat(|x| &x.timeouts), 2);
    // A request abandoned by timeout is counted as a timeout, not as a
    // connection that ended mid-frame.
    assert_eq!(s.stat(|x| &x.partial_frames), 0);
    // An active client is unaffected as long as it keeps talking.
    let mut c = s.client().await;
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        c.ping().await.unwrap();
    }
    s.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_limit_is_enforced() {
    let dir = TempDir::new().unwrap();
    let cfg = ServerConfig {
        max_connections: 2,
        ..quick_cfg()
    };
    let s = start(dir.path(), cfg).await;
    let mut a = s.client().await;
    let mut b = s.client().await;
    a.ping().await.unwrap();
    b.ping().await.unwrap();
    let mut third = raw(s.addr).await;
    let r = read_response(&mut third, OP_PING).await.unwrap();
    assert!(matches!(r, Response::Busy(_)), "{r:?}");
    assert_closed(&mut third).await;
    drop(a);
    // The slot frees once the server notices the close.
    let mut ok = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut c = s.client().await;
        if c.ping().await.is_ok() {
            ok = true;
            break;
        }
    }
    assert!(ok, "slot was never released");
    b.ping().await.unwrap();
    assert!(s.stat(|x| &x.connections_rejected) >= 1);
    s.stop().await.unwrap();
}

/// Phase 8: a flood of connections beyond `max_connections` is answered
/// BUSY and closed, counted, and does not disturb admitted clients; the
/// server keeps working afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_flood_is_rejected_without_disturbing_admitted_clients() {
    let dir = TempDir::new().unwrap();
    let cfg = ServerConfig {
        max_connections: 8,
        ..quick_cfg()
    };
    let s = start(dir.path(), cfg).await;
    let mut admitted = vec![];
    for _ in 0..7 {
        let mut c = s.client().await;
        c.ping().await.unwrap();
        admitted.push(c);
    }
    // Hold one more slot with a raw connection that never sends, so every
    // flood connection is over the limit.
    let mut holder = raw(s.addr).await;
    holder.write_all(&frame_of(&Request::Ping)).await.unwrap();
    assert_eq!(
        read_response(&mut holder, OP_PING).await,
        Some(Response::Ok)
    );

    const FLOOD: usize = 200;
    let flood: Vec<_> = (0..FLOOD)
        .map(|_| {
            let addr = s.addr;
            tokio::spawn(async move {
                let mut sock = raw(addr).await;
                let r = read_response(&mut sock, OP_PING).await;
                assert_closed(&mut sock).await;
                r
            })
        })
        .collect();
    // Admitted clients keep working while the flood is processed.
    for (i, c) in admitted.iter_mut().enumerate() {
        let k = format!("admitted-{i}").into_bytes();
        c.put(&k, b"v").await.unwrap();
        assert_eq!(c.get(&k).await.unwrap(), Some(b"v".to_vec()));
    }
    for f in flood {
        let r = timeout(T, f).await.unwrap().unwrap();
        assert!(matches!(r, Some(Response::Busy(_))), "{r:?}");
    }
    assert_eq!(s.stat(|x| &x.connections_rejected), FLOOD as u64);
    assert_eq!(s.stat(|x| &x.connections_accepted), 8);
    // Freed slots are reusable.
    drop(admitted);
    drop(holder);
    let mut ok = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut c = s.client().await;
        if c.ping().await.is_ok() {
            ok = true;
            break;
        }
    }
    assert!(ok, "slots were not released after the flood");
    s.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn many_concurrent_clients() {
    let dir = TempDir::new().unwrap();
    let s = start(dir.path(), quick_cfg()).await;
    let mut tasks = vec![];
    for t in 0..32 {
        let mut c = s.client().await;
        tasks.push(tokio::spawn(async move {
            for i in 0..100 {
                let own = format!("c{t}-k{i}").into_bytes();
                c.put(&own, &own).await.unwrap();
                c.put(b"shared", format!("{t}-{i}").as_bytes())
                    .await
                    .unwrap();
                assert_eq!(c.get(&own).await.unwrap(), Some(own.clone()));
            }
        }));
    }
    for t in tasks {
        timeout(Duration::from_secs(60), t).await.unwrap().unwrap();
    }
    let mut c = s.client().await;
    for t in 0..32 {
        for i in 0..100 {
            let own = format!("c{t}-k{i}").into_bytes();
            assert_eq!(c.get(&own).await.unwrap(), Some(own.clone()));
        }
    }
    let shared = String::from_utf8(c.get(b"shared").await.unwrap().unwrap()).unwrap();
    assert!(
        shared.ends_with("-99"),
        "last write of some client wins: {shared}"
    );
    assert_eq!(s.stat(|x| &x.internal_errors), 0);
    s.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_survives_a_graceful_server_restart() {
    let dir = TempDir::new().unwrap();
    let s = start(dir.path(), quick_cfg()).await;
    let mut c = s.client().await;
    for i in 0..500 {
        c.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
            .await
            .unwrap();
    }
    c.delete(b"k7").await.unwrap();
    drop(c);
    s.stop().await.unwrap();

    let s = start(dir.path(), quick_cfg()).await;
    let mut c = s.client().await;
    for i in 0..500 {
        let want = (i != 7).then(|| format!("v{i}").into_bytes());
        assert_eq!(c.get(format!("k{i}").as_bytes()).await.unwrap(), want);
    }
    s.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_closes_idle_connections_refuses_new_ones_and_closes_the_engine() {
    let dir = TempDir::new().unwrap();
    let s = start(dir.path(), quick_cfg()).await;
    let addr = s.addr;
    let db = Arc::clone(&s.db);
    let stats = Arc::clone(&s.stats);
    let mut idle = raw(addr).await;
    let mut c = s.client().await;
    c.put(b"k", b"v").await.unwrap();
    s.stop().await.unwrap();
    assert_closed(&mut idle).await;
    assert_not_accepting(addr, &stats).await;
    assert!(matches!(db.get(b"k"), Err(forgekv::Error::Closed)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_errors_map_to_statuses() {
    let dir = TempDir::new().unwrap();
    let s = start(dir.path(), quick_cfg()).await;
    let mut c = s.client().await;
    // Engine shut down underneath the server: requests get BUSY, not a crash.
    let db = Arc::clone(&s.db);
    tokio::task::spawn_blocking(move || db.shutdown())
        .await
        .unwrap()
        .unwrap();
    match c.put(b"k", b"v").await {
        Err(ClientError::Status(Response::Busy(_))) => {}
        other => panic!("{other:?}"),
    }
    c.ping().await.unwrap();
    s.stop().await.unwrap();
}

// ---------------------------------------------------------------- binary

mod binary {
    use super::*;
    use std::io::{BufRead, BufReader};
    use std::process::{Child, Command, Stdio};

    fn spawn(dir: &Path) -> (Child, SocketAddr) {
        spawn_with(dir, &[])
    }

    fn spawn_with(dir: &Path, extra: &[&str]) -> (Child, SocketAddr) {
        let mut child = Command::new(env!("CARGO_BIN_EXE_forgekv-server"))
            .args(["--dir", dir.to_str().unwrap(), "--addr", "127.0.0.1:0"])
            .args(extra)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let addr = line
            .trim()
            .strip_prefix("listening on ")
            .unwrap_or_else(|| panic!("unexpected output {line:?}"))
            .parse()
            .unwrap();
        (child, addr)
    }

    async fn write_keys(addr: SocketAddr, range: std::ops::Range<usize>) {
        let mut c = Client::connect(addr).await.unwrap();
        for i in range {
            c.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
                .await
                .unwrap();
        }
    }

    async fn check_keys(addr: SocketAddr, range: std::ops::Range<usize>) {
        let mut c = Client::connect(addr).await.unwrap();
        for i in range {
            assert_eq!(
                c.get(format!("k{i}").as_bytes()).await.unwrap(),
                Some(format!("v{i}").into_bytes()),
                "acknowledged write k{i} lost"
            );
        }
    }

    fn wait_exit(child: &mut Child) -> std::process::ExitStatus {
        for _ in 0..200 {
            if let Some(st) = child.try_wait().unwrap() {
                return st;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = child.kill();
        panic!("server did not exit");
    }

    /// Every write acknowledged before SIGKILL is there after a restart.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn kill_dash_nine_loses_no_acknowledged_write() {
        let dir = TempDir::new().unwrap();
        for round in 0..3 {
            let (mut child, addr) = spawn(dir.path());
            write_keys(addr, round * 300..(round + 1) * 300).await;
            child.kill().unwrap(); // SIGKILL
            child.wait().unwrap();
        }
        let (mut child, addr) = spawn(dir.path());
        check_keys(addr, 0..900).await;
        child.kill().unwrap();
        child.wait().unwrap();
    }

    /// Phase 8: SIGKILL while 4 clients have writes in flight, alternating
    /// `--sync always` and `--sync never`. Every acknowledged write must be
    /// there after a restart. Process crash only (the page cache survives).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn kill_dash_nine_during_concurrent_writes_loses_no_acknowledged_write() {
        let dir = TempDir::new().unwrap();
        let mut acked: Vec<(Vec<u8>, Vec<u8>)> = vec![];
        for round in 0..6u64 {
            let sync = if round % 2 == 0 { "always" } else { "never" };
            let (mut child, addr) = spawn_with(dir.path(), &["--sync", sync]);
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let mut writers = vec![];
            for c in 0..4 {
                let tx = tx.clone();
                writers.push(tokio::spawn(async move {
                    let mut cl = Client::connect(addr).await.unwrap();
                    for i in 0.. {
                        let k = format!("r{round}-c{c}-k{i}").into_bytes();
                        let v = format!("value-{round}-{c}-{i}").into_bytes();
                        // An error means the server is gone: stop.
                        if cl.put(&k, &v).await.is_err() || tx.send((k, v)).is_err() {
                            break;
                        }
                    }
                }));
            }
            drop(tx);
            tokio::time::sleep(Duration::from_millis(60 + 50 * round)).await;
            child.kill().unwrap(); // SIGKILL, requests in flight
            child.wait().unwrap();
            for w in writers {
                w.await.unwrap();
            }
            while let Ok(kv) = rx.try_recv() {
                acked.push(kv);
            }
        }
        assert!(acked.len() > 20, "only {} writes acknowledged", acked.len());
        let (mut child, addr) = spawn(dir.path());
        let mut c = Client::connect(addr).await.unwrap();
        for (k, v) in &acked {
            assert_eq!(
                c.get(k).await.unwrap().as_ref(),
                Some(v),
                "acknowledged write {} lost",
                String::from_utf8_lossy(k)
            );
        }
        child.kill().unwrap();
        child.wait().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sigterm_shuts_down_gracefully() {
        let dir = TempDir::new().unwrap();
        let (mut child, addr) = spawn(dir.path());
        write_keys(addr, 0..200).await;
        let st = Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .unwrap();
        assert!(st.success());
        let exit = wait_exit(&mut child);
        assert!(exit.success(), "{exit:?}");
        let (mut child, addr) = spawn(dir.path());
        check_keys(addr, 0..200).await;
        child.kill().unwrap();
        child.wait().unwrap();
    }

    /// Regression (audit): the signal handlers were installed only after
    /// `listening on` was printed, so a SIGTERM sent right after readiness
    /// killed the process (about 25% of runs) instead of shutting it down
    /// gracefully. The window is microseconds wide, so the signal is sent
    /// with `kill(2)` directly; spawning `kill(1)` or a shell is too slow.
    #[test]
    fn sigterm_right_after_readiness_is_graceful() {
        use rustix::process::{Pid, Signal, kill_process};
        for round in 0..30 {
            let dir = TempDir::new().unwrap();
            let (mut child, _addr) = spawn(dir.path());
            let pid = Pid::from_raw(child.id() as i32).unwrap();
            kill_process(pid, Signal::TERM).unwrap();
            let exit = wait_exit(&mut child);
            assert!(exit.success(), "round {round}: {exit:?}");
        }
    }

    #[test]
    fn bad_arguments_exit_with_usage() {
        let out = Command::new(env!("CARGO_BIN_EXE_forgekv-server"))
            .args(["--bogus"])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(2));
        assert!(String::from_utf8_lossy(&out.stderr).contains("usage"));
    }
}

// ------------------------------------------- deterministic (pause points)

#[cfg(feature = "failpoints")]
mod paused {
    use super::*;
    use forgekv::FailPoints;

    async fn start_paused(dir: &Path, cfg: ServerConfig) -> (TestServer, FailPoints) {
        let fp = FailPoints::new();
        let (d, f) = (dir.to_path_buf(), fp.clone());
        let db = tokio::task::spawn_blocking(move || Db::open_with_failpoints(d, opts(), f))
            .await
            .unwrap()
            .unwrap();
        (start_with_db(db, cfg).await, fp)
    }

    /// With the only engine permit held by a stuck GET, other requests are
    /// answered BUSY (and PING still works: it needs no engine permit).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn in_flight_engine_operations_are_bounded() {
        let dir = TempDir::new().unwrap();
        let cfg = ServerConfig {
            max_inflight_ops: 1,
            busy_timeout: Duration::from_millis(100),
            ..quick_cfg()
        };
        let (s, fp) = start_paused(dir.path(), cfg).await;
        fp.pause("read:after_snapshot");
        let mut stuck = s.client().await;
        let held = tokio::spawn(async move { stuck.get(b"k").await });
        let fp2 = fp.clone();
        assert!(
            tokio::task::spawn_blocking(move || fp2.wait_paused("read:after_snapshot", T))
                .await
                .unwrap()
        );
        let mut c = s.client().await;
        match c.put(b"x", b"y").await {
            Err(ClientError::Status(Response::Busy(_))) => {}
            other => panic!("{other:?}"),
        }
        c.ping().await.unwrap();
        fp.release("read:after_snapshot");
        assert_eq!(held.await.unwrap().unwrap(), None);
        c.put(b"x", b"y").await.unwrap();
        assert!(s.stat(|x| &x.busy) >= 1);
        s.stop().await.unwrap();
    }

    /// On a single-threaded runtime, an engine call frozen inside the
    /// storage layer must not stall other connections: storage work runs on
    /// the blocking pool, never on the async worker thread.
    #[tokio::test(flavor = "current_thread")]
    async fn a_stuck_engine_call_does_not_block_the_runtime() {
        let dir = TempDir::new().unwrap();
        let (s, fp) = start_paused(dir.path(), quick_cfg()).await;
        fp.pause("read:after_snapshot");
        // If the runtime thread itself were blocked, no async timeout could
        // fire. This OS-thread watchdog releases the pause after 3 s and
        // records that it had to, turning a hang into a clean failure.
        let rescued = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let watchdog = {
            let (fp, rescued) = (fp.clone(), Arc::clone(&rescued));
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_secs(3));
                if fp.wait_paused("read:after_snapshot", Duration::ZERO) {
                    rescued.store(true, std::sync::atomic::Ordering::SeqCst);
                    fp.release("read:after_snapshot");
                }
            })
        };
        let mut stuck = s.client().await;
        let held = tokio::spawn(async move { stuck.get(b"k").await });
        let fp2 = fp.clone();
        assert!(
            tokio::task::spawn_blocking(move || fp2.wait_paused("read:after_snapshot", T))
                .await
                .unwrap()
        );
        let mut c = s.client().await;
        let ping = timeout(Duration::from_secs(2), c.ping()).await;
        fp.release("read:after_snapshot");
        assert_eq!(held.await.unwrap().unwrap(), None);
        watchdog.join().unwrap();
        assert!(
            !rescued.load(std::sync::atomic::Ordering::SeqCst),
            "runtime thread was blocked by an engine call"
        );
        ping.expect("ping timed out").unwrap();
        s.stop().await.unwrap();
    }

    /// A request in progress when shutdown starts is completed and answered;
    /// new connections are refused meanwhile; the engine closes last.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_lets_the_in_flight_request_finish() {
        let dir = TempDir::new().unwrap();
        let (s, fp) = start_paused(dir.path(), quick_cfg()).await;
        let mut c = s.client().await;
        c.put(b"k", b"v").await.unwrap();
        fp.pause("read:after_snapshot");
        let in_flight = tokio::spawn(async move { c.get(b"k").await });
        let fp2 = fp.clone();
        assert!(
            tokio::task::spawn_blocking(move || fp2.wait_paused("read:after_snapshot", T))
                .await
                .unwrap()
        );
        let addr = s.addr;
        let db = Arc::clone(&s.db);
        let stats = Arc::clone(&s.stats);
        let stopping = tokio::spawn(s.stop());
        // Shutdown is waiting for the in-flight request.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!stopping.is_finished());
        assert_not_accepting(addr, &stats).await;
        fp.release("read:after_snapshot");
        assert_eq!(in_flight.await.unwrap().unwrap(), Some(b"v".to_vec()));
        timeout(T, stopping).await.unwrap().unwrap().unwrap();
        assert!(matches!(db.get(b"k"), Err(forgekv::Error::Closed)));
    }
}
