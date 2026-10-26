//! Deterministic workload generation: a seeded PRNG per client, fixed-width
//! keys, and the operation mix of each workload.
//!
//! For a given `(seed, client index)` the operation sequence is always the
//! same; how far along it a run gets depends on its duration.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Workload {
    /// PUT keys 0, 1, 2, ... (clients interleave), wrapping at `keys`.
    FillSeq,
    /// PUT uniformly random keys in `0..keys`.
    FillRandom,
    /// GET uniformly random keys of a preloaded dataset.
    ReadRandom,
    /// `read_pct`% GET, the rest PUT, uniform keys, preloaded dataset.
    ReadWrite,
    /// 20% GET, 40% PUT, 40% DELETE, uniform keys, preloaded dataset.
    DeleteHeavy,
}

impl Workload {
    pub fn parse(s: &str) -> Option<Workload> {
        Some(match s {
            "fillseq" => Workload::FillSeq,
            "fillrandom" => Workload::FillRandom,
            "readrandom" => Workload::ReadRandom,
            "readwrite" => Workload::ReadWrite,
            "deleteheavy" => Workload::DeleteHeavy,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Workload::FillSeq => "fillseq",
            Workload::FillRandom => "fillrandom",
            Workload::ReadRandom => "readrandom",
            Workload::ReadWrite => "readwrite",
            Workload::DeleteHeavy => "deleteheavy",
        }
    }

    /// Whether the dataset is loaded (untimed) before the measured run.
    pub fn needs_preload(self) -> bool {
        !matches!(self, Workload::FillSeq | Workload::FillRandom)
    }
}

/// SplitMix64: small, fast, and good enough for choosing keys.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n` (multiply-shift; bias is negligible for n << 2^64).
    pub fn below(&mut self, n: u64) -> u64 {
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Get(u64),
    Put(u64),
    Delete(u64),
}

/// Index into per-kind arrays: GET, PUT, DELETE.
pub const KINDS: [&str; 3] = ["get", "put", "delete"];

impl Op {
    pub fn key(self) -> u64 {
        match self {
            Op::Get(k) | Op::Put(k) | Op::Delete(k) => k,
        }
    }

    pub fn kind(self) -> usize {
        match self {
            Op::Get(_) => 0,
            Op::Put(_) => 1,
            Op::Delete(_) => 2,
        }
    }
}

/// The operation stream of one client.
#[derive(Debug, Clone)]
pub struct Gen {
    workload: Workload,
    read_pct: u64,
    keys: u64,
    rng: Rng,
    seq_next: u64,
    seq_step: u64,
}

impl Gen {
    pub fn new(
        workload: Workload,
        read_pct: u64,
        keys: u64,
        seed: u64,
        client: u64,
        clients: u64,
    ) -> Gen {
        // Distinct, reproducible streams per client.
        let mut mix = Rng::new(seed ^ client.wrapping_mul(0xD1B5_4A32_D192_ED03));
        Gen {
            workload,
            read_pct,
            keys,
            rng: Rng::new(mix.next_u64()),
            seq_next: client,
            seq_step: clients.max(1),
        }
    }

    pub fn next_op(&mut self) -> Op {
        let random = |rng: &mut Rng| rng.below(self.keys);
        match self.workload {
            Workload::FillSeq => {
                let k = self.seq_next % self.keys;
                self.seq_next += self.seq_step;
                Op::Put(k)
            }
            Workload::FillRandom => Op::Put(random(&mut self.rng)),
            Workload::ReadRandom => Op::Get(random(&mut self.rng)),
            Workload::ReadWrite => {
                let roll = self.rng.below(100);
                let k = random(&mut self.rng);
                if roll < self.read_pct {
                    Op::Get(k)
                } else {
                    Op::Put(k)
                }
            }
            Workload::DeleteHeavy => {
                let roll = self.rng.below(100);
                let k = random(&mut self.rng);
                match roll {
                    0..20 => Op::Get(k),
                    20..60 => Op::Put(k),
                    _ => Op::Delete(k),
                }
            }
        }
    }
}

/// Decimal digits needed for the largest key index.
pub fn min_key_size(keys: u64) -> usize {
    keys.saturating_sub(1).max(1).to_string().len()
}

/// Write key `i` into `buf` as a zero-padded decimal filling the whole
/// buffer (so keys sort numerically). `buf` must be at least
/// `min_key_size(keys)` long.
pub fn write_key(buf: &mut [u8], mut i: u64) {
    for b in buf.iter_mut().rev() {
        *b = b'0' + (i % 10) as u8;
        i /= 10;
    }
}

/// Value bytes: pseudo-random (incompressible), slightly different per key.
pub struct Values {
    buf: Vec<u8>,
    size: usize,
}

const VALUE_WINDOW: usize = 4096;

impl Values {
    pub fn new(size: usize, seed: u64) -> Values {
        let mut rng = Rng::new(seed ^ 0x05EE_D0F7_A1E5);
        let buf = (0..size + VALUE_WINDOW)
            .map(|_| rng.next_u64() as u8)
            .collect();
        Values { buf, size }
    }

    pub fn get(&self, key: u64) -> &[u8] {
        let off = (key as usize).wrapping_mul(31) % VALUE_WINDOW;
        &self.buf[off..off + self.size]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ops(g: &mut Gen, n: usize) -> Vec<Op> {
        (0..n).map(|_| g.next_op()).collect()
    }

    #[test]
    fn same_seed_same_sequence_and_clients_differ() {
        let mk = |client| Gen::new(Workload::ReadWrite, 50, 1000, 42, client, 4);
        assert_eq!(ops(&mut mk(0), 500), ops(&mut mk(0), 500));
        assert_ne!(ops(&mut mk(0), 500), ops(&mut mk(1), 500));
        let other_seed = Gen::new(Workload::ReadWrite, 50, 1000, 43, 0, 4);
        assert_ne!(ops(&mut mk(0), 500), ops(&mut other_seed.clone(), 500));
    }

    #[test]
    fn mixes_and_key_ranges() {
        let mut g = Gen::new(Workload::ReadWrite, 90, 1000, 7, 0, 1);
        let v = ops(&mut g, 20_000);
        let reads = v.iter().filter(|o| matches!(o, Op::Get(_))).count();
        assert!((17_500..18_500).contains(&reads), "{reads}");
        assert!(v.iter().all(|o| o.key() < 1000));

        let mut g = Gen::new(Workload::DeleteHeavy, 0, 50, 7, 0, 1);
        let v = ops(&mut g, 20_000);
        let dels = v.iter().filter(|o| matches!(o, Op::Delete(_))).count();
        assert!((7_500..8_500).contains(&dels), "{dels}");

        let mut g = Gen::new(Workload::ReadRandom, 0, 10, 7, 0, 1);
        assert!(
            ops(&mut g, 100)
                .iter()
                .all(|o| matches!(o, Op::Get(k) if *k < 10))
        );
    }

    #[test]
    fn fillseq_interleaves_clients_and_wraps() {
        let mut a = Gen::new(Workload::FillSeq, 0, 5, 1, 0, 2);
        let mut b = Gen::new(Workload::FillSeq, 0, 5, 1, 1, 2);
        assert_eq!(
            ops(&mut a, 4),
            [Op::Put(0), Op::Put(2), Op::Put(4), Op::Put(1)]
        );
        assert_eq!(ops(&mut b, 3), [Op::Put(1), Op::Put(3), Op::Put(0)]);
    }

    #[test]
    fn keys_are_fixed_width_and_sort_numerically() {
        let mut k = [0u8; 8];
        write_key(&mut k, 42);
        assert_eq!(&k, b"00000042");
        let mut a = [0u8; 4];
        let mut b = [0u8; 4];
        write_key(&mut a, 9);
        write_key(&mut b, 10);
        assert!(a < b);
        assert_eq!(min_key_size(1), 1);
        assert_eq!(min_key_size(1000), 3);
        assert_eq!(min_key_size(1001), 4);
    }

    #[test]
    fn values_have_the_requested_size() {
        let v = Values::new(100, 1);
        assert_eq!(v.get(0).len(), 100);
        assert_eq!(v.get(u64::MAX).len(), 100);
        assert_ne!(v.get(0), v.get(1));
        assert_eq!(Values::new(100, 1).get(5), v.get(5));
    }
}
