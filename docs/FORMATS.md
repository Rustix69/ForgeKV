# ForgeKV on-disk and wire formats

This is a consolidated reference. The authoritative specifications are the
module docs they cite; golden-byte tests pin each format, and
`tests/golden.rs` checks that the current code still writes a stored
database (`tests/data/golden-db/`) byte for byte and can read it.

Conventions: all on-disk integers are **little-endian**; all wire-protocol
integers are **big-endian**. Every checksum is **CRC-32/IEEE** (the
`crc32fast` crate, polynomial 0x04C11DB7 reflected), not CRC-32C.

## Database directory

```
<db>/
  LOCK               advisory exclusive lock, held while a Db is open
  MANIFEST           record log of version edits (the source of truth)
  MANIFEST.tmp       transient while a new MANIFEST is written; deleted on open
  wal-<n>.log        write-ahead log number n (6+ digits, zero-padded)
  <n>.sst            SSTable number n
  *.tmp              transient; deleted on open
```

WALs and SSTables share one increasing file-number counter, persisted in
the MANIFEST. On open, an SSTable not named by the MANIFEST, a WAL numbered
below the MANIFEST's `log_number`, and any `*.tmp` file are orphans and are
deleted. Files with other names are left alone. A directory that contains
WAL or SST files but no MANIFEST is refused as `Corruption`.

## Record log (WAL and MANIFEST) — `src/logfile.rs`, format version 2

```
file header (16 B) = magic[8] "FKVLOG\0\1" | kind u8 (1 WAL, 2 MANIFEST) | version u8 = 2 | reserved[6] = 0
frame              = len u32 | header_crc u32 | payload_crc u32 | payload[len]
  header_crc  = CRC-32 of the 4 bytes of len
  payload_crc = CRC-32 of len (4 bytes) || payload
  1 <= len <= max record size
```

Reading (deterministic):

| Situation | Result |
|---|---|
| Newest WAL shorter than the header, and what exists is a prefix of the expected header | creation was interrupted: treated as an empty log (for a MANIFEST or an older WAL this is `Corruption`) |
| Wrong magic or kind, non-zero reserved bytes | `Corruption` |
| Other version | `UnsupportedVersion` |
| File ends inside a frame and nothing read so far is wrong (fewer than 8 header bytes, or a verified header with a short payload) | **incomplete final frame** (torn write): discarded |
| `header_crc` wrong, `len` 0 or over the limit, `payload_crc` wrong (including the **last** frame), or an undecodable payload | `Corruption`; the file is not modified |

Only the newest WAL may end in an incomplete frame; in an older (sealed)
WAL it is `Corruption`. "Discarded" means ignored: recovery never truncates
a file. `RecoveryMode::SalvagePrefix` (opt-in) drops everything from the
first invalid frame of the newest WAL and reports it; it never applies to
the MANIFEST.

Writers assemble a frame in memory and write it with a write-all loop; any
write or sync error poisons the writer (no further appends, so damage can
only be at the end).

## WAL record payload — `src/wal.rs`

```
type u8 (1 Put, 2 Delete) | key_len u32 | key | [val_len u32 | value]   (value fields only for Put)
```

Keys are 1..=`max_key_len` bytes, values 0..=`max_value_len` bytes. A
record with a valid checksum but an unknown type or bad lengths is
`Corruption`.

## MANIFEST record — `src/manifest.rs`

```
record = edit_count u32 | edit{edit_count}          one record = one atomic version change
edit   = 1 SetNextFileNumber next u64
       | 2 SetLogNumber      log u64                 oldest WAL still needed
       | 3 AddFile           level u8 | file_no u64 | size u64 |
                             smallest_len u32 | smallest | largest_len u32 | largest
       | 4 DeleteFile        level u8 | file_no u64
```

Levels are 0 and 1. Every open writes a fresh single-record snapshot
(`MANIFEST.tmp` → fsync → rename → fsync directory); so does a flush or
compaction once the file exceeds `Options::manifest_max_bytes`. Replay
rejects inconsistent edits (duplicate add, delete of an unknown file or at
the wrong level, log number going backwards, a file number not below
`next`, overlapping L1 files) as `Corruption`. Every table the MANIFEST
names must exist, open cleanly and match the recorded size and key range.

## SSTable — `src/sstable/mod.rs`, format version 1

```
[data block 0] ... [data block n-1] [index block] [footer]

data block  = entry* | block_crc u32                         (CRC over the entries)
entry       = kind u8 (1 Value, 2 Tombstone) | key_len u32 | val_len u32 | key | value
index block = count u32 | smallest_key_len u32 | smallest_key |
              { last_key_len u32 | last_key | block_offset u64 | block_len u32 | block_entries u32 }{count} |
              index_crc u32                                  (CRC over everything before it)
footer (56 B) = index_offset u64 | index_len u64 | bloom_offset u64 | bloom_len u64 |
                entry_count u64 | version u32 = 1 | magic[8] "FKVSST\0\1" | footer_crc u32
                (CRC over the preceding 52 bytes; bloom_* reserved, must be 0)
```

Keys are non-empty and strictly increasing within and across blocks; a
tombstone has `val_len = 0`. The reader validates the footer before reading
the index, the index before reading blocks, and each block when it is read
(checksum, every length bounds-checked, key order, agreement with the
index). No allocation is sized from an unchecked field (hard caps: block
16 MiB, index 1 GiB). There is no compression, prefix compression or Bloom
filter.

## Wire protocol — `src/protocol.rs`

```
frame    = len u32 (big-endian) | payload[len]          1 <= len <= max_frame
request  = op u8 | body
  1 PING    (empty)
  2 GET     key_len u32 | key
  3 PUT     key_len u32 | key | val_len u32 | value
  4 DELETE  key_len u32 | key
response = status u8 | body
  0 OK          GET: val_len u32 | value; PING/PUT/DELETE: empty
  1 NOT_FOUND   (GET only)
  2 BAD_REQUEST utf-8 message; the connection stays open
  3 TOO_LARGE   utf-8 message; the server closes the connection
  4 BUSY        utf-8 message; the connection stays open
  5 INTERNAL    utf-8 message (generic; details are logged by the server)
```

`max_frame = 1 + 4 + max_key_len + 4 + max_value_len`. A frame length above
it is TOO_LARGE (checked before anything is read or allocated) and the
connection is closed; a zero-length frame, unknown opcode, short body,
overrunning length or trailing bytes is BAD_REQUEST and the connection
continues; a key or value over its limit inside a frame is TOO_LARGE.
Responses come in request order; clients may pipeline. There is no version
handshake in v1.
