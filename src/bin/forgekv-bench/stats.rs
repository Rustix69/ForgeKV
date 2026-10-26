//! Exact latency percentiles and run-to-run spread.
//!
//! Every operation's latency is kept (nanoseconds), sorted, and indexed:
//! no histogram, so no bucketing error. Percentiles use the nearest-rank
//! definition: the p-th percentile of n sorted samples is the sample at
//! 1-based rank `ceil(p / 100 * n)`.

/// Nearest-rank percentile of an ascending, non-empty slice; `p` in (0, 100].
pub fn percentile(sorted: &[u64], p: f64) -> u64 {
    debug_assert!(!sorted.is_empty() && p > 0.0 && p <= 100.0);
    let n = sorted.len();
    let rank = ((p / 100.0) * n as f64).ceil() as usize;
    sorted[rank.clamp(1, n) - 1]
}

/// Latency distribution of one kind of operation in one run (nanoseconds).
#[derive(Debug, Clone, PartialEq)]
pub struct Lat {
    pub count: u64,
    pub mean: f64,
    pub min: u64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
}

/// Summarize raw samples; `None` if there are none.
pub fn latency(mut samples: Vec<u64>) -> Option<Lat> {
    if samples.is_empty() {
        return None;
    }
    samples.sort_unstable();
    let sum: u128 = samples.iter().map(|&x| u128::from(x)).sum();
    Some(Lat {
        count: samples.len() as u64,
        mean: sum as f64 / samples.len() as f64,
        min: samples[0],
        p50: percentile(&samples, 50.0),
        p95: percentile(&samples, 95.0),
        p99: percentile(&samples, 99.0),
        p999: percentile(&samples, 99.9),
        max: samples[samples.len() - 1],
    })
}

/// Median, minimum and maximum of a per-run metric.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Spread {
    pub median: f64,
    pub min: f64,
    pub max: f64,
}

/// `None` if `xs` is empty. The median of an even count is the mean of the
/// two middle values.
pub fn spread(xs: &[f64]) -> Option<Spread> {
    if xs.is_empty() {
        return None;
    }
    let mut v = xs.to_vec();
    v.sort_by(f64::total_cmp);
    let n = v.len();
    let median = if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    };
    Some(Spread {
        median,
        min: v[0],
        max: v[n - 1],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_percentiles_of_known_arrays() {
        let v: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&v, 50.0), 50);
        assert_eq!(percentile(&v, 95.0), 95);
        assert_eq!(percentile(&v, 99.0), 99);
        assert_eq!(percentile(&v, 99.9), 100);
        assert_eq!(percentile(&v, 100.0), 100);
        assert_eq!(percentile(&v, 0.5), 1);
        assert_eq!(percentile(&[7], 50.0), 7);
        assert_eq!(percentile(&[7], 99.0), 7);
        // n = 10: p95 -> rank ceil(9.5) = 10; p50 -> rank 5.
        let v: Vec<u64> = (10..20).collect();
        assert_eq!(percentile(&v, 50.0), 14);
        assert_eq!(percentile(&v, 95.0), 19);
        assert_eq!(percentile(&v, 90.0), 18);
    }

    #[test]
    fn latency_sorts_and_summarizes() {
        let l = latency(vec![5, 1, 3, 2, 4]).unwrap();
        assert_eq!(l.count, 5);
        assert_eq!((l.min, l.p50, l.max), (1, 3, 5));
        assert_eq!(l.mean, 3.0);
        assert_eq!((l.p95, l.p99, l.p999), (5, 5, 5));
        assert!(latency(vec![]).is_none());
    }

    #[test]
    fn spread_of_odd_and_even_counts() {
        let s = spread(&[3.0, 1.0, 2.0]).unwrap();
        assert_eq!((s.median, s.min, s.max), (2.0, 1.0, 3.0));
        let s = spread(&[4.0, 1.0, 3.0, 2.0]).unwrap();
        assert_eq!((s.median, s.min, s.max), (2.5, 1.0, 4.0));
        assert!(spread(&[]).is_none());
    }
}
