//! Machine and build metadata recorded with every result file. Values are
//! read from `/proc`, `/sys` and a few commands; anything unavailable is
//! recorded as "unknown" rather than guessed.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::json::J;

pub struct Env {
    pub fields: Vec<(String, J)>,
    /// The benchmark directory is on a RAM-backed filesystem.
    pub ram_fs: bool,
}

const UNKNOWN: &str = "unknown";

fn read(path: impl AsRef<Path>) -> Option<String> {
    fs::read_to_string(path).ok().map(|s| s.trim().to_string())
}

fn command(cmd: &str, args: &[&str], dir: &Path) -> Option<String> {
    let out = Command::new(cmd)
        .args(args)
        .current_dir(dir)
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn field_after(text: &str, prefix: &str) -> Option<String> {
    text.lines()
        .find_map(|l| l.strip_prefix(prefix))
        .map(|v| v.trim_start_matches([' ', '\t', ':']).trim().to_string())
}

/// Mount point, filesystem type and source device holding `path`
/// (longest matching mount point in `/proc/self/mountinfo`).
fn mount_of(path: &Path) -> Option<(String, String, String)> {
    let path = fs::canonicalize(path).ok()?;
    let info = read("/proc/self/mountinfo")?;
    let mut best: Option<(String, String, String)> = None;
    for line in info.lines() {
        let (pre, post) = line.split_once(" - ")?;
        let mount_point = unescape(pre.split(' ').nth(4)?);
        let mut post = post.split(' ');
        let (fstype, source) = (post.next()?.to_string(), post.next()?.to_string());
        if path.starts_with(&mount_point)
            && best.as_ref().is_none_or(|b| mount_point.len() >= b.0.len())
        {
            best = Some((mount_point, fstype, source));
        }
    }
    best
}

/// mountinfo escapes space, tab, newline and backslash as `\ooo`.
fn unescape(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\' && i + 3 < b.len() && b[i + 1..i + 4].iter().all(u8::is_ascii_digit) {
            let v = (b[i + 1] - b'0') * 64 + (b[i + 2] - b'0') * 8 + (b[i + 3] - b'0');
            out.push(v);
            i += 4;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Model and rotational flag of the whole disk behind a `/dev/...` source.
fn device_info(source: &str) -> (String, String) {
    let unknown = || (UNKNOWN.to_string(), UNKNOWN.to_string());
    let Some(name) = source.strip_prefix("/dev/") else {
        return unknown();
    };
    let class = PathBuf::from("/sys/class/block").join(name);
    let disk = if class.join("partition").exists() {
        fs::canonicalize(&class).ok().and_then(|p| {
            p.parent()
                .and_then(|d| d.file_name())
                .map(|n| n.to_string_lossy().into_owned())
        })
    } else {
        Some(name.to_string())
    };
    let Some(disk) = disk else {
        return unknown();
    };
    let sys = PathBuf::from("/sys/block").join(&disk);
    let model = read(sys.join("device/model")).unwrap_or_else(|| UNKNOWN.into());
    let rotational = match read(sys.join("queue/rotational")).as_deref() {
        Some("0") => "no (SSD/NVMe)".to_string(),
        Some("1") => "yes".to_string(),
        _ => UNKNOWN.to_string(),
    };
    (format!("{disk}: {model}"), rotational)
}

/// CRC-32 over the crate's sources, so a result can be matched to the exact
/// code even when it was not committed.
fn source_fingerprint() -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = vec![root.join("Cargo.toml"), root.join("Cargo.lock")];
    let mut stack = vec![root.join("src")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            return UNKNOWN.into();
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else {
                files.push(p);
            }
        }
    }
    files.sort();
    let mut h = crc32fast::Hasher::new();
    for f in &files {
        let Ok(bytes) = fs::read(f) else {
            return UNKNOWN.into();
        };
        h.update(
            f.strip_prefix(root)
                .unwrap_or(f)
                .to_string_lossy()
                .as_bytes(),
        );
        h.update(&bytes);
    }
    format!("crc32:{:08x} over {} files", h.finalize(), files.len())
}

/// `YYYY-MM-DDTHH:MM:SSZ` and a compact `YYYYMMDDTHHMMSSZ` form.
pub fn utc_now() -> (String, String) {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    utc(secs)
}

fn utc(secs: u64) -> (String, String) {
    // Civil-from-days (H. Hinnant), proleptic Gregorian calendar.
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    let (hh, mm, ss) = (rem / 3600, rem % 3600 / 60, rem % 60);
    (
        format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}Z"),
        format!("{y:04}{m:02}{d:02}T{hh:02}{mm:02}{ss:02}Z"),
    )
}

pub fn collect(bench_dir: &Path) -> Env {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let s = |v: Option<String>| J::str(v.unwrap_or_else(|| UNKNOWN.into()));
    let cpuinfo = read("/proc/cpuinfo").unwrap_or_default();
    let meminfo = read("/proc/meminfo").unwrap_or_default();
    let (mount_point, fstype, source) =
        mount_of(bench_dir).unwrap_or_else(|| (UNKNOWN.into(), UNKNOWN.into(), UNKNOWN.into()));
    let (device, rotational) = device_info(&source);
    let ram_fs = matches!(fstype.as_str(), "tmpfs" | "ramfs");
    let git_head = command("git", &["rev-parse", "HEAD"], root);
    let git_dirty = command("git", &["status", "--porcelain"], root).map(|o| !o.is_empty());
    let fields = vec![
        ("cpu_model", s(field_after(&cpuinfo, "model name"))),
        (
            "logical_cpus",
            J::opt(std::thread::available_parallelism().ok(), |n| {
                J::Int(n.get() as u64)
            }),
        ),
        ("mem_total", s(field_after(&meminfo, "MemTotal"))),
        (
            "os",
            s(read("/etc/os-release")
                .and_then(|t| field_after(&t, "PRETTY_NAME="))
                .map(|v| v.trim_matches('"').to_string())),
        ),
        ("kernel", s(read("/proc/sys/kernel/osrelease"))),
        (
            "cpu_governor",
            s(read(
                "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor",
            )),
        ),
        ("load_average_at_start", s(read("/proc/loadavg"))),
        (
            "bench_dir",
            J::str(
                fs::canonicalize(bench_dir)
                    .unwrap_or(bench_dir.to_path_buf())
                    .display()
                    .to_string(),
            ),
        ),
        ("filesystem", J::str(fstype)),
        ("mount_point", J::str(mount_point)),
        ("mount_source", J::str(source)),
        ("storage_device", J::str(device)),
        ("rotational", J::str(rotational)),
        ("rustc", s(command("rustc", &["-V"], root))),
        (
            "build_profile",
            J::str(if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            }),
        ),
        ("git_head", s(git_head.or(Some("none (no commits)".into())))),
        ("git_dirty", J::opt(git_dirty, J::Bool)),
        ("source_fingerprint", J::str(source_fingerprint())),
        (
            "command_line",
            J::str(std::env::args().collect::<Vec<_>>().join(" ")),
        ),
    ];
    Env {
        fields: fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
        ram_fs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_formatting() {
        assert_eq!(utc(0).0, "1970-01-01T00:00:00Z");
        assert_eq!(utc(1_700_000_000).0, "2023-11-14T22:13:20Z");
        assert_eq!(utc(951_782_400).1, "20000229T000000Z");
    }

    #[test]
    fn mountinfo_unescaping() {
        assert_eq!(unescape(r"/mnt/a\040b"), "/mnt/a b");
        assert_eq!(unescape(r"/plain"), "/plain");
        assert_eq!(unescape(r"/end\04"), r"/end\04");
    }

    #[test]
    fn the_current_directory_has_a_mount() {
        let (mp, fstype, _) = mount_of(Path::new(".")).unwrap();
        assert!(mp.starts_with('/') && !fstype.is_empty());
    }
}
