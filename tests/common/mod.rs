//! Independent SSTable encoder used to hand-craft valid and malformed files
//! straight from the format specification in `src/sstable/mod.rs`. It does
//! not call the crate's own encoder, so it also checks the spec itself.
#![allow(dead_code)]

pub const MAGIC: [u8; 8] = *b"FKVSST\0\x01";

pub fn u32le(x: u32) -> [u8; 4] {
    x.to_le_bytes()
}
pub fn u64le(x: u64) -> [u8; 8] {
    x.to_le_bytes()
}
pub fn crc(b: &[u8]) -> u32 {
    crc32fast::hash(b)
}

/// A well-formed entry.
pub fn entry(kind: u8, key: &[u8], value: &[u8]) -> Vec<u8> {
    entry_raw(kind, key.len() as u32, value.len() as u32, key, value)
}

/// An entry whose length fields may lie about the bytes that follow.
pub fn entry_raw(kind: u8, klen: u32, vlen: u32, key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut b = vec![kind];
    b.extend(u32le(klen));
    b.extend(u32le(vlen));
    b.extend(key);
    b.extend(value);
    b
}

/// Append a valid block checksum to `body`.
pub fn block(body: &[u8]) -> Vec<u8> {
    let mut b = body.to_vec();
    b.extend(u32le(crc(body)));
    b
}

/// One index entry with every field overridable: (key_len field, key, offset, block_len, entries).
pub type IdxEnt<'a> = (u32, &'a [u8], u64, u32, u32);

pub fn index_raw(count: u32, smallest_len: u32, smallest: &[u8], ents: &[IdxEnt]) -> Vec<u8> {
    let mut b = vec![];
    b.extend(u32le(count));
    b.extend(u32le(smallest_len));
    b.extend(smallest);
    for (klen, key, off, len, n) in ents {
        b.extend(u32le(*klen));
        b.extend(*key);
        b.extend(u64le(*off));
        b.extend(u32le(*len));
        b.extend(u32le(*n));
    }
    let c = crc(&b);
    b.extend(u32le(c));
    b
}

pub fn index_ok(smallest: &[u8], ents: &[IdxEnt]) -> Vec<u8> {
    index_raw(ents.len() as u32, smallest.len() as u32, smallest, ents)
}

/// Footer with a valid checksum over arbitrary field values.
pub fn footer_raw(
    index_off: u64,
    index_len: u64,
    bloom_off: u64,
    bloom_len: u64,
    entries: u64,
    version: u32,
    magic: [u8; 8],
) -> Vec<u8> {
    let mut b = vec![];
    for x in [index_off, index_len, bloom_off, bloom_len, entries] {
        b.extend(u64le(x));
    }
    b.extend(u32le(version));
    b.extend(magic);
    let c = crc(&b);
    b.extend(u32le(c));
    assert_eq!(b.len(), 56);
    b
}

/// blocks + index + a footer that correctly describes where the index sits.
pub fn assemble(blocks: &[Vec<u8>], index: &[u8], entry_count: u64) -> Vec<u8> {
    let mut f: Vec<u8> = blocks.concat();
    let off = f.len() as u64;
    f.extend(index);
    f.extend(footer_raw(
        off,
        index.len() as u64,
        0,
        0,
        entry_count,
        1,
        MAGIC,
    ));
    f
}

/// Baseline: one block holding a=1, b=2.
pub fn baseline_block() -> Vec<u8> {
    block(&[entry(1, b"a", b"1"), entry(1, b"b", b"2")].concat())
}

pub fn baseline() -> Vec<u8> {
    let blk = baseline_block();
    let idx = index_ok(b"a", &[(1, b"b", 0, blk.len() as u32, 2)]);
    assemble(&[blk], &idx, 2)
}

/// A file whose footer and index are consistent with `body` (checksummed
/// correctly) but whose *content* is whatever `body` says.
fn block_case(body: &[u8], declared_n: u32, smallest: &[u8], last_key: &[u8]) -> Vec<u8> {
    let blk = block(body);
    let idx = index_ok(
        smallest,
        &[(
            last_key.len() as u32,
            last_key,
            0,
            blk.len() as u32,
            declared_n,
        )],
    );
    assemble(&[blk], &idx, u64::from(declared_n))
}

pub struct Case {
    pub name: &'static str,
    pub bytes: Vec<u8>,
    /// Substring that must appear in the error message.
    pub expect: &'static str,
}

fn case(name: &'static str, bytes: Vec<u8>, expect: &'static str) -> Case {
    Case {
        name,
        bytes,
        expect,
    }
}

/// Malformed files that carry *valid checksums* (so the checksum alone cannot
/// reject them) and try to provoke huge allocations or out-of-bounds reads.
/// Every one must fail `SstReader::open` + `verify` with the given message.
pub fn malicious_cases() -> Vec<Case> {
    let mut v = vec![];
    let blk = baseline_block();
    let bl = blk.len() as u32;
    let good_idx = index_ok(b"a", &[(1, b"b", 0, bl, 2)]);
    let data_len = blk.len() as u64;
    let il = good_idx.len() as u64;
    let ft = |io, ilen, bo, bl_, n, ver, magic| {
        let mut f = blk.clone();
        f.extend(&good_idx);
        f.extend(footer_raw(io, ilen, bo, bl_, n, ver, magic));
        f
    };

    // --- footer-level
    v.push(case(
        "index_len=u64::MAX",
        ft(data_len, u64::MAX, 0, 0, 2, 1, MAGIC),
        "invalid index length",
    ));
    v.push(case(
        "index_len=1<<40",
        ft(data_len, 1 << 40, 0, 0, 2, 1, MAGIC),
        "invalid index length",
    ));
    v.push(case(
        "index_len=1",
        ft(data_len, 1, 0, 0, 2, 1, MAGIC),
        "invalid index length",
    ));
    v.push(case(
        "index_len=MAX_INDEX_LEN",
        ft(data_len, 1 << 30, 0, 0, 2, 1, MAGIC),
        "does not end",
    ));
    v.push(case(
        "index_offset=u64::MAX",
        ft(u64::MAX, il, 0, 0, 2, 1, MAGIC),
        "does not end",
    ));
    v.push(case(
        "offset+len overflows",
        ft(u64::MAX - 1, 100, 0, 0, 2, 1, MAGIC),
        "does not end",
    ));
    v.push(case(
        "index_len one long",
        ft(data_len, il + 1, 0, 0, 2, 1, MAGIC),
        "does not end",
    ));
    v.push(case(
        "bloom_offset set",
        ft(data_len, il, 1, 0, 2, 1, MAGIC),
        "reserved bloom",
    ));
    v.push(case(
        "bloom_len set",
        ft(data_len, il, 0, 7, 2, 1, MAGIC),
        "reserved bloom",
    ));
    v.push(case(
        "entry_count=0",
        ft(data_len, il, 0, 0, 0, 1, MAGIC),
        "no entries",
    ));
    v.push(case(
        "entry_count mismatch",
        ft(data_len, il, 0, 0, 3, 1, MAGIC),
        "entry counts sum",
    ));
    v.push(case(
        "bad magic",
        ft(data_len, il, 0, 0, 2, 1, *b"NOTANSST"),
        "bad sstable magic",
    ));

    // --- index-level (checksums valid, footer consistent)
    let one = |ents: &[IdxEnt], smallest: &[u8]| {
        assemble(std::slice::from_ref(&blk), &index_ok(smallest, ents), 2)
    };
    v.push(case(
        "count=u32::MAX",
        assemble(
            std::slice::from_ref(&blk),
            &index_raw(u32::MAX, 1, b"a", &[(1, b"b", 0, bl, 2)]),
            2,
        ),
        "implausible index entry count",
    ));
    v.push(case(
        "count=0",
        assemble(
            std::slice::from_ref(&blk),
            &index_raw(0, 1, b"a", &[(1, b"b", 0, bl, 2)]),
            2,
        ),
        "implausible index entry count",
    ));
    v.push(case(
        "smallest_len=u32::MAX",
        assemble(
            std::slice::from_ref(&blk),
            &index_raw(1, u32::MAX, b"a", &[(1, b"b", 0, bl, 2)]),
            2,
        ),
        "invalid smallest key length",
    ));
    v.push(case(
        "smallest_len=0",
        assemble(
            std::slice::from_ref(&blk),
            &index_raw(1, 0, b"a", &[(1, b"b", 0, bl, 2)]),
            2,
        ),
        "invalid smallest key length",
    ));
    v.push(case(
        "last_key_len=u32::MAX",
        one(&[(u32::MAX, b"b", 0, bl, 2)], b"a"),
        "invalid key length",
    ));
    v.push(case(
        "last_key_len=0",
        one(&[(0, b"b", 0, bl, 2)], b"a"),
        "invalid key length",
    ));
    v.push(case(
        "block_len=0",
        one(&[(1, b"b", 0, 0, 2)], b"a"),
        "invalid block length",
    ));
    v.push(case(
        "block_len=u32::MAX",
        one(&[(1, b"b", 0, u32::MAX, 2)], b"a"),
        "invalid block length",
    ));
    v.push(case(
        "block_len=MAX+1",
        one(&[(1, b"b", 0, 16 * 1024 * 1024 + 1, 2)], b"a"),
        "invalid block length",
    ));
    v.push(case(
        "first offset != 0",
        one(&[(1, b"b", 1, bl, 2)], b"a"),
        "block offset",
    ));
    v.push(case(
        "block past data area",
        one(&[(1, b"b", 0, bl + 100, 2)], b"a"),
        "past the data area",
    ));
    v.push(case(
        "entries=u32::MAX",
        one(&[(1, b"b", 0, bl, u32::MAX)], b"a"),
        "invalid entry count",
    ));
    v.push(case(
        "entries=0",
        one(&[(1, b"b", 0, bl, 0)], b"a"),
        "invalid entry count",
    ));
    v.push(case(
        "entries implausibly many",
        one(&[(1, b"b", 0, bl, 1000)], b"a"),
        "invalid entry count",
    ));
    v.push(case(
        "blocks do not fill data area",
        one(&[(1, b"b", 0, bl - 2, 2)], b"a"),
        "do not fill",
    ));
    v.push(case(
        "entries sum != footer",
        one(&[(1, b"b", 0, bl, 1)], b"a"),
        "entry counts sum",
    ));
    v.push(case(
        "smallest > last key",
        one(&[(1, b"b", 0, bl, 2)], b"z"),
        "smallest key is greater",
    ));
    {
        let mut idx = index_ok(b"a", &[(1, b"b", 0, bl, 2)]);
        idx.truncate(idx.len() - 4); // drop crc
        idx.extend([0, 0, 0]); // 3 stray bytes inside the checksummed region
        let c = crc(&idx);
        idx.extend(u32le(c));
        v.push(case(
            "trailing bytes in index",
            assemble(std::slice::from_ref(&blk), &idx, 2),
            "trailing bytes in index",
        ));
    }
    {
        // Two blocks whose index last-keys go backwards.
        let b1 = block(&entry(1, b"m", b"1"));
        let b2 = block(&entry(1, b"c", b"1"));
        let idx = index_ok(
            b"m",
            &[
                (1, b"m", 0, b1.len() as u32, 1),
                (1, b"c", b1.len() as u64, b2.len() as u32, 1),
            ],
        );
        v.push(case(
            "index keys decreasing",
            assemble(&[b1, b2], &idx, 2),
            "not strictly increasing",
        ));
    }

    // --- block-level (open succeeds; reading the block must fail)
    let ab = |a: Vec<u8>, b: Vec<u8>| [a, b].concat();
    v.push(case(
        "entry key_len=u32::MAX",
        block_case(&entry_raw(1, u32::MAX, 1, b"a", b"1"), 1, b"a", b"a"),
        "invalid key length",
    ));
    v.push(case(
        "entry key_len=0",
        block_case(&entry_raw(1, 0, 1, b"", b"1"), 1, b"a", b"a"),
        "invalid key length",
    ));
    v.push(case(
        "entry val_len=u32::MAX",
        block_case(&entry_raw(1, 1, u32::MAX, b"a", b"1"), 1, b"a", b"a"),
        "exceeds block",
    ));
    v.push(case(
        "unknown kind",
        block_case(&entry(9, b"a", b"1"), 1, b"a", b"a"),
        "unknown entry kind",
    ));
    v.push(case(
        "tombstone with value",
        block_case(&entry(2, b"a", b"1"), 1, b"a", b"a"),
        "tombstone carries a value",
    ));
    v.push(case(
        "unsorted keys",
        block_case(
            &ab(entry(1, b"b", b"1"), entry(1, b"a", b"1")),
            2,
            b"a",
            b"a",
        ),
        "not strictly increasing",
    ));
    v.push(case(
        "duplicate keys",
        block_case(
            &ab(entry(1, b"a", b"1"), entry(1, b"a", b"2")),
            2,
            b"a",
            b"a",
        ),
        "not strictly increasing",
    ));
    v.push(case(
        "more entries than index declares",
        block_case(
            &ab(entry(1, b"a", b"1"), entry(1, b"b", b"2")),
            1,
            b"a",
            b"b",
        ),
        "index declares",
    ));
    v.push(case(
        "fewer entries than index declares",
        block_case(&entry(1, b"a", &[7u8; 20]), 2, b"a", b"a"),
        "index declares",
    ));
    v.push(case(
        "trailing partial entry",
        block_case(
            &[entry(1, b"a", b"1"), vec![1, 2, 3]].concat(),
            1,
            b"a",
            b"a",
        ),
        "need",
    ));
    v.push(case(
        "index last key differs from block",
        block_case(
            &ab(entry(1, b"a", b"1"), entry(1, b"b", b"2")),
            2,
            b"a",
            b"c",
        ),
        "last key differs",
    ));
    v.push(case(
        "recorded smallest differs from first key",
        block_case(
            &ab(entry(1, b"a", b"1"), entry(1, b"b", b"2")),
            2,
            b"0",
            b"b",
        ),
        "smallest key",
    ));
    {
        let mut blk = baseline_block();
        let n = blk.len();
        blk[n - 1] ^= 1; // wrong block crc
        let idx = index_ok(b"a", &[(1, b"b", 0, blk.len() as u32, 2)]);
        v.push(case(
            "bad block crc",
            assemble(&[blk], &idx, 2),
            "block checksum mismatch",
        ));
    }
    v
}
