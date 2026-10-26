# Benchmark session 20261026T173115Z

Raw results: `bench-results/20261026T173115Z/*.json`; full output: `bench-results/20261026T173115Z/log.txt`.
Latency cells are the median across runs of each run's p50 / p95 / p99.
Throughput is the median across runs (min–max in parentheses).
Write stalls (waits for memtable room; in parentheses those that began during a
compaction) and total stall time are medians across runs of the measured phase.

| name | mode | workload | sync | threads/clients | runs | ops/s median (min–max) | GET p50 / p95 / p99 | PUT p50 / p95 / p99 | DELETE p50 / p95 / p99 | worst max latency | write stalls (during compaction), stall time | errors / busy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| fsync-probe-start | fsync | append 100 B + fdatasync | always | 1 | 5 | 335 (285–426) | - | 3.08 ms / 4.08 ms / 4.67 ms | - | 9.63 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-fillseq-never-c1 | engine | fillseq | never | 1 | 5 | 416920 (414468–425740) | - | 864 ns / 2.0 µs / 3.2 µs | - | 143.96 ms | 41 (41), 3.40 s | 0 / 0 |
| engine-fillrandom-never-c1 | engine | fillrandom | never | 1 | 5 | 383643 (381304–407399) | - | 932 ns / 1.8 µs / 2.9 µs | - | 152.03 ms | 36 (35), 3.97 s | 0 / 0 |
| engine-fillrandom-never-c4 | engine | fillrandom | never | 4 | 5 | 236323 (185663–242003) | - | 14.2 µs / 19.2 µs / 23.9 µs | - | 50.90 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-fillrandom-always-c1 | engine | fillrandom | always | 1 | 5 | 306 (287–744) | - | 3.43 ms / 3.88 ms / 4.20 ms | - | 8.93 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-fillrandom-always-c4 | engine | fillrandom | always | 4 | 5 | 287 (249–306) | - | 13.69 ms / 16.50 ms / 18.35 ms | - | 28.22 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-fillrandom-never-c4-v1000 | engine | fillrandom | never | 4 | 5 | 23673 (22829–23821) | - | 21.5 µs / 35.2 µs / 66.3 µs | - | 687.05 ms | 16 (15), 7.95 s | 0 / 0 |
| engine-readrandom-always-c1 | engine | readrandom | always | 1 | 5 | 669668 (640137–695049) | 1.4 µs / 1.8 µs / 2.3 µs | - | - | 1.03 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-readrandom-always-c4 | engine | readrandom | always | 4 | 5 | 2255094 (1948064–2325036) | 1.7 µs / 2.1 µs / 2.6 µs | - | - | 1.04 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-readrandom-always-c8 | engine | readrandom | always | 8 | 5 | 2822684 (2592780–3149390) | 2.6 µs / 3.9 µs / 4.5 µs | - | - | 6.96 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-readrandom-always-c4-k1M | engine | readrandom | always | 4 | 5 | 1764758 (1691864–1849222) | 2.1 µs / 2.9 µs / 3.6 µs | - | - | 4.05 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-readwrite90-always-c4 | engine | readwrite 90% get | always | 4 | 5 | 2580 (2495–3748) | 11.0 µs / 31.2 µs / 49.1 µs | 15.26 ms / 17.05 ms / 18.33 ms | - | 27.29 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-readwrite90-never-c4 | engine | readwrite 90% get | never | 4 | 5 | 998745 (944297–1040210) | 3.6 µs / 7.7 µs / 10.6 µs | 1.6 µs / 6.0 µs / 10.1 µs | - | 42.80 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-readwrite50-always-c4 | engine | readwrite 50% get | always | 4 | 5 | 529 (506–840) | 15.0 µs / 46.9 µs / 67.5 µs | 14.72 ms / 17.16 ms / 18.97 ms | - | 26.93 ms | 0 (0), 0.00 s | 0 / 0 |
| engine-readwrite50-never-c4 | engine | readwrite 50% get | never | 4 | 5 | 493845 (471098–521487) | 5.4 µs / 11.6 µs / 14.8 µs | 7.8 µs / 15.0 µs / 19.5 µs | - | 60.33 ms | 6 (1), 0.12 s | 0 / 0 |
| engine-deleteheavy-never-c4 | engine | deleteheavy | never | 4 | 5 | 305238 (286445–336770) | 5.5 µs / 13.3 µs / 17.8 µs | 13.3 µs / 20.2 µs / 25.6 µs | 13.2 µs / 20.0 µs / 25.2 µs | 18.20 ms | 0 (0), 0.00 s | 0 / 0 |
| server-readwrite50-never-c1 | server | readwrite 50% get | never | 1 | 5 | 47129 (46169–47808) | 20.4 µs / 37.1 µs / 48.0 µs | 17.3 µs / 33.6 µs / 41.6 µs | - | 19.37 ms | 0 (0), 0.00 s | 0 / 0 |
| server-readwrite50-never-c8 | server | readwrite 50% get | never | 8 | 5 | 160928 (159887–177884) | 41.1 µs / 97.2 µs / 153.0 µs | 39.6 µs / 94.1 µs / 156.2 µs | - | 26.13 ms | 0 (0), 0.00 s | 0 / 0 |
| server-readwrite50-never-c32 | server | readwrite 50% get | never | 32 | 5 | 125746 (122713–135508) | 83.5 µs / 351.1 µs / 540.1 µs | 353.2 µs / 658.5 µs / 1.14 ms | - | 30.38 ms | 0 (0), 0.00 s | 0 / 0 |
| server-readwrite50-always-c8 | server | readwrite 50% get | always | 8 | 5 | 582 (496–1939) | 411.4 µs / 689.1 µs / 814.6 µs | 25.81 ms / 32.68 ms / 33.68 ms | - | 40.78 ms | 0 (0), 0.00 s | 0 / 0 |
| server-readrandom-always-c8 | server | readrandom | always | 8 | 5 | 220159 (218282–221040) | 34.3 µs / 50.6 µs / 94.6 µs | - | - | 2.57 ms | 0 (0), 0.00 s | 0 / 0 |
| fsync-probe-end | fsync | append 100 B + fdatasync | always | 1 | 5 | 296 (261–326) | - | 3.65 ms / 4.15 ms / 4.39 ms | - | 8.68 ms | 0 (0), 0.00 s | 0 / 0 |

| name | records written | sync | memtable MiB | WAL MiB before open | SSTs before open | runs | open ms median (min–max) | records replayed | flushed on open |
|---|---|---|---|---|---|---|---|---|---|
| recovery-k100000-m67108864 | 100000 | always | 64.0 | 13.1 | 0 | 5 | 92.2 (90.8–94.6) | 100000 | true |
| recovery-k300000-m67108864 | 300000 | always | 64.0 | 39.2 | 0 | 5 | 312.9 (297.6–317.5) | 300000 | true |
| recovery-k1000000-m4194304 | 1000000 | always | 4.0 | 2.0 | 39 | 5 | 33.3 (27.9–36.7) | 15362 | true |
