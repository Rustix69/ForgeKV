# Benchmark session 20261026T160449Z

Raw results: `bench-results/20261026T160449Z/*.json`; full output: `bench-results/20261026T160449Z/log.txt`.
Latency cells are the median across runs of each run's p50 / p95 / p99.
Throughput is the median across runs (min–max in parentheses).

| name | mode | workload | sync | threads/clients | runs | ops/s median (min–max) | GET p50 / p95 / p99 | PUT p50 / p95 / p99 | DELETE p50 / p95 / p99 | worst max latency | errors / busy |
|---|---|---|---|---|---|---|---|---|---|---|---|
| fsync-probe-start | fsync | append 100 B + fdatasync | always | 1 | 5 | 354 (348–829) | - | 3.03 ms / 3.83 ms / 4.14 ms | - | 18.47 ms | 0 / 0 |
| engine-fillseq-never-c1 | engine | fillseq | never | 1 | 5 | 420515 (403649–449909) | - | 845 ns / 2.1 µs / 3.3 µs | - | 150.18 ms | 0 / 0 |
| engine-fillrandom-never-c1 | engine | fillrandom | never | 1 | 5 | 394289 (381967–430678) | - | 892 ns / 1.7 µs / 2.7 µs | - | 179.45 ms | 0 / 0 |
| engine-fillrandom-never-c4 | engine | fillrandom | never | 4 | 5 | 334418 (319056–342945) | - | 2.0 µs / 23.5 µs / 38.5 µs | - | 109.46 ms | 0 / 0 |
| engine-fillrandom-always-c1 | engine | fillrandom | always | 1 | 5 | 282 (273–317) | - | 3.59 ms / 4.18 ms / 4.56 ms | - | 19.10 ms | 0 / 0 |
| engine-fillrandom-always-c4 | engine | fillrandom | always | 4 | 5 | 283 (273–324) | - | 3.47 ms / 4.14 ms / 6.15 ms | - | 10.01 s | 0 / 0 |
| engine-fillrandom-never-c4-v1000 | engine | fillrandom | never | 4 | 5 | 23953 (22804–25071) | - | 4.1 µs / 53.3 µs / 107.1 µs | - | 719.18 ms | 0 / 0 |
| engine-readrandom-always-c1 | engine | readrandom | always | 1 | 5 | 648724 (571604–675593) | 1.4 µs / 1.9 µs / 2.6 µs | - | - | 276.5 µs | 0 / 0 |
| engine-readrandom-always-c4 | engine | readrandom | always | 4 | 5 | 2236751 (2145333–2292108) | 1.7 µs / 2.2 µs / 2.9 µs | - | - | 1.77 ms | 0 / 0 |
| engine-readrandom-always-c8 | engine | readrandom | always | 8 | 5 | 2966063 (2872771–3081835) | 2.4 µs / 3.7 µs / 4.2 µs | - | - | 2.71 ms | 0 / 0 |
| engine-readrandom-always-c4-k1M | engine | readrandom | always | 4 | 5 | 1733647 (1531631–1818341) | 2.1 µs / 3.0 µs / 3.9 µs | - | - | 3.03 ms | 0 / 0 |
| engine-readwrite90-always-c4 | engine | readwrite 90% get | always | 4 | 5 | 3232 (2917–3614) | 5.8 µs / 17.2 µs / 31.7 µs | 10.07 ms / 39.79 ms / 63.99 ms | - | 174.20 ms | 0 / 0 |
| engine-readwrite90-never-c4 | engine | readwrite 90% get | never | 4 | 5 | 955921 (927882–981122) | 3.7 µs / 8.0 µs / 11.4 µs | 1.5 µs / 3.5 µs / 7.1 µs | - | 26.32 ms | 0 / 0 |
| engine-readwrite50-always-c4 | engine | readwrite 50% get | always | 4 | 5 | 590 (520–715) | 14.6 µs / 43.6 µs / 59.3 µs | 3.51 ms / 46.98 ms / 258.03 ms | - | 2.47 s | 0 / 0 |
| engine-readwrite50-never-c4 | engine | readwrite 50% get | never | 4 | 5 | 612788 (588507–622581) | 5.5 µs / 11.3 µs / 14.5 µs | 2.2 µs / 10.5 µs / 16.1 µs | - | 100.29 ms | 0 / 0 |
| engine-deleteheavy-never-c4 | engine | deleteheavy | never | 4 | 5 | 536164 (521784–580554) | 5.7 µs / 13.9 µs / 18.4 µs | 2.5 µs / 17.5 µs / 26.7 µs | 2.3 µs / 17.3 µs / 26.4 µs | 44.80 ms | 0 / 0 |
| server-readwrite50-never-c1 | server | readwrite 50% get | never | 1 | 5 | 36353 (30144–48811) | 23.3 µs / 52.5 µs / 82.1 µs | 19.8 µs / 46.4 µs / 72.8 µs | - | 18.91 ms | 0 / 0 |
| server-readwrite50-never-c8 | server | readwrite 50% get | never | 8 | 5 | 178034 (163529–183846) | 40.2 µs / 81.9 µs / 133.2 µs | 36.8 µs / 75.5 µs / 128.7 µs | - | 21.69 ms | 0 / 0 |
| server-readwrite50-never-c32 | server | readwrite 50% get | never | 32 | 5 | 196150 (192961–207835) | 130.3 µs / 316.3 µs / 490.2 µs | 130.3 µs / 321.0 µs / 509.2 µs | - | 23.68 ms | 0 / 0 |
| server-readwrite50-always-c8 | server | readwrite 50% get | always | 8 | 5 | 512 (498–917) | 448.6 µs / 724.5 µs / 829.7 µs | 30.79 ms / 33.70 ms / 34.94 ms | - | 56.81 ms | 0 / 0 |
| server-readrandom-always-c8 | server | readrandom | always | 8 | 5 | 221258 (217540–221349) | 34.2 µs / 49.9 µs / 89.1 µs | - | - | 2.86 ms | 0 / 0 |
| fsync-probe-end | fsync | append 100 B + fdatasync | always | 1 | 5 | 262 (259–537) | - | 3.83 ms / 4.13 ms / 4.34 ms | - | 9.69 ms | 0 / 0 |

| name | records written | sync | memtable MiB | WAL MiB before open | SSTs before open | runs | open ms median (min–max) | records replayed | flushed on open |
|---|---|---|---|---|---|---|---|---|---|
| recovery-k100000-m67108864 | 100000 | always | 64.0 | 13.1 | 0 | 5 | 95.0 (94.4–95.6) | 100000 | true |
| recovery-k300000-m67108864 | 300000 | always | 64.0 | 39.2 | 0 | 5 | 324.9 (316.5–328.4) | 300000 | true |
| recovery-k1000000-m4194304 | 1000000 | always | 4.0 | 2.0 | 39 | 5 | 31.4 (29.4–33.4) | 15362 | true |
