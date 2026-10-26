//! ForgeKV TCP server.
//!
//! ```text
//! forgekv-server --dir PATH [--addr 127.0.0.1:7878] [--sync always|never]
//!                [--max-connections N] [--max-inflight N]
//!                [--idle-timeout-ms N] [--request-timeout-ms N]
//! ```
//! Prints `listening on <addr>` once ready. SIGINT/SIGTERM start a graceful
//! shutdown; a second signal exits immediately (acknowledged writes are
//! already in the WAL).

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use forgekv::server::{ServerConfig, ServerStats, serve};
use forgekv::{Db, Options, SyncMode};
use tokio::net::TcpListener;
use tokio::signal::unix::{Signal, SignalKind, signal};

struct Args {
    dir: PathBuf,
    addr: String,
    opts: Options,
    cfg: ServerConfig,
}

fn usage() -> String {
    "usage: forgekv-server --dir PATH [--addr HOST:PORT] [--sync always|never] \
     [--max-connections N] [--max-inflight N] [--idle-timeout-ms N] [--request-timeout-ms N]"
        .into()
}

fn parse(mut it: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut dir = None;
    let mut addr = "127.0.0.1:7878".to_string();
    let mut opts = Options::default();
    let mut cfg = ServerConfig::default();
    while let Some(flag) = it.next() {
        let mut value = || it.next().ok_or_else(|| format!("{flag} needs a value"));
        let number = |v: String| {
            v.parse::<u64>()
                .map_err(|_| format!("{flag}: not a number: {v}"))
        };
        match flag.as_str() {
            "--dir" => dir = Some(PathBuf::from(value()?)),
            "--addr" => addr = value()?,
            "--sync" => {
                opts.sync_mode = match value()?.as_str() {
                    "always" => SyncMode::Always,
                    "never" => SyncMode::Never,
                    other => return Err(format!("--sync: expected always|never, got {other}")),
                }
            }
            "--max-connections" => cfg.max_connections = number(value()?)? as usize,
            "--max-inflight" => cfg.max_inflight_ops = number(value()?)? as usize,
            "--idle-timeout-ms" => cfg.idle_timeout = Duration::from_millis(number(value()?)?),
            "--request-timeout-ms" => {
                cfg.request_timeout = Duration::from_millis(number(value()?)?)
            }
            "--help" | "-h" => return Err(usage()),
            other => return Err(format!("unknown argument {other}\n{}", usage())),
        }
    }
    let dir = dir.ok_or_else(usage)?;
    if cfg.max_connections == 0 || cfg.max_inflight_ops == 0 {
        return Err("--max-connections and --max-inflight must be > 0".into());
    }
    Ok(Args {
        dir,
        addr,
        opts,
        cfg,
    })
}

/// SIGTERM/SIGINT handlers. Installed before the server reports readiness:
/// until then a signal would still have its default action (kill the
/// process), skipping the graceful shutdown.
struct Signals {
    term: Signal,
    int: Signal,
}

impl Signals {
    fn install() -> std::io::Result<Signals> {
        Ok(Signals {
            term: signal(SignalKind::terminate())?,
            int: signal(SignalKind::interrupt())?,
        })
    }
}

/// Resolves on the first SIGINT/SIGTERM; a second one exits the process.
async fn shutdown_signal(signals: Signals) {
    let Signals { mut term, mut int } = signals;
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
    eprintln!("forgekv-server: shutting down (signal again to exit immediately)");
    tokio::spawn(async move {
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
        eprintln!("forgekv-server: second signal, exiting now");
        std::process::exit(130);
    });
}

async fn run(args: Args) -> Result<(), String> {
    let Args {
        dir,
        addr,
        opts,
        cfg,
    } = args;
    let db = tokio::task::spawn_blocking(move || Db::open(&dir, opts))
        .await
        .map_err(|e| format!("open task failed: {e}"))?
        .map_err(|e| format!("cannot open database: {e}"))?;
    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("cannot bind {addr}: {e}"))?;
    let local = listener
        .local_addr()
        .map_err(|e| format!("local address: {e}"))?;
    let signals = Signals::install().map_err(|e| format!("cannot install signal handlers: {e}"))?;
    println!("listening on {local}");
    serve(
        listener,
        Arc::new(db),
        cfg,
        Arc::new(ServerStats::default()),
        shutdown_signal(signals),
    )
    .await
    .map_err(|e| format!("shutdown error: {e}"))
}

fn main() -> ExitCode {
    let args = match parse(std::env::args().skip(1)) {
        Ok(a) => a,
        Err(msg) => {
            eprintln!("{msg}");
            return ExitCode::from(2);
        }
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("cannot start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("forgekv-server: {msg}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(s: &str) -> Result<Args, String> {
        parse(s.split_whitespace().map(String::from))
    }

    #[test]
    fn parses_flags() {
        let a = args("--dir /tmp/x --addr 0.0.0.0:1 --sync never --max-connections 3 --max-inflight 2 --idle-timeout-ms 5").unwrap();
        assert_eq!(a.dir, PathBuf::from("/tmp/x"));
        assert_eq!(a.addr, "0.0.0.0:1");
        assert_eq!(a.opts.sync_mode, SyncMode::Never);
        assert_eq!(a.cfg.max_connections, 3);
        assert_eq!(a.cfg.max_inflight_ops, 2);
        assert_eq!(a.cfg.idle_timeout, Duration::from_millis(5));
    }

    #[test]
    fn rejects_bad_flags() {
        for bad in [
            "",
            "--addr x",
            "--dir d --sync sometimes",
            "--dir d --max-inflight 0",
            "--dir d --bogus",
            "--dir",
        ] {
            assert!(args(bad).is_err(), "{bad:?}");
        }
    }
}
