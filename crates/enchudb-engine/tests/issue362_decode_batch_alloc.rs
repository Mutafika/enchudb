//! #362: `decode_batch` は、 相手から届いた bytes の先頭 4 byte (件数) をそのまま `Vec::with_capacity` に
//! 渡していた。 大きな件数だと record を 1 件も読む前に 「件数 × record の大きさ」 を確保しようとし、
//! Linux では確保に失敗してプロセスごと abort する (CI の `fuzz_like_parsing` で
//! `memory allocation of 180490557696 bytes failed`)。 macOS は大きな確保を触るまで実体化しないので通る —
//! 手元でしか回していなかったので見えなかった。
//!
//! OS によらず見えるように、 確保の**要求の大きさ**を数える (1 回の要求の最大)。 この file の test は
//! 1 本だけ (他の test の確保が混ざらないように)。

use enchudb_engine::transport::{decode_batch, encode_batch, WireDecodeError, WireRecord};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::Hlc;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

/// 1 回の確保の要求の最大 (byte)。
static MAX_REQUEST: AtomicUsize = AtomicUsize::new(0);

struct Watching;

unsafe impl GlobalAlloc for Watching {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        MAX_REQUEST.fetch_max(l.size(), Ordering::Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
        MAX_REQUEST.fetch_max(new_size, Ordering::Relaxed);
        unsafe { System.realloc(p, l, new_size) }
    }
}

#[global_allocator]
static ALLOC: Watching = Watching;

/// `f` の間の、 1 回の確保の要求の最大。
fn max_request_during<R>(f: impl FnOnce() -> R) -> (R, usize) {
    MAX_REQUEST.store(0, Ordering::Relaxed);
    let r = f();
    (r, MAX_REQUEST.load(Ordering::Relaxed))
}

#[test]
fn decode_batch_allocation_is_bounded_by_the_input_length() {
    let rec_size = std::mem::size_of::<WireRecord>();

    // 件数だけ大きく、 中身の無い入力: 何も確保せずに Truncated
    for count in [u32::MAX, 1 << 30, 1_000_000] {
        let mut buf = count.to_le_bytes().to_vec();
        buf.extend_from_slice(&[0xAB; 16]);
        let (r, max) = max_request_during(|| decode_batch(&buf));
        assert!(matches!(r, Err(WireDecodeError::Truncated)), "count {count}");
        assert!(max <= 4096, "count {count}: 入力 20 byte に対して {max} byte を要求した");
    }

    // 件数だけ大きく、 乱数の中身 16 KB (CI で落ちた proptest の形): 確保は入力に入りうる件数まで
    let mut buf = u32::MAX.to_le_bytes().to_vec();
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    for _ in 0..16 * 1024 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        buf.push(x as u8);
    }
    let (r, max) = max_request_during(|| decode_batch(&buf));
    assert!(r.is_err());
    let bound = (buf.len() / 100 + 1) * rec_size;
    assert!(max <= bound, "入力 {} byte に対して {max} byte を要求した (上限 {bound})", buf.len());

    // 正しい batch は今までどおり読める (件数が入力に入りきる時は、 その件数ぶんを 1 回で確保する)
    let records: Vec<WireRecord> = (0..1000u64)
        .map(|i| {
            WireRecord::unsigned(
                Hlc { wall: 1000 + i, logical: 0, peer: 7 },
                7,
                DecodedOp::Tie { eid: i, himo_id: 3, value: i * 2 },
            )
        })
        .collect();
    let batch = encode_batch(&records);
    let (decoded, max) = max_request_during(|| decode_batch(&batch).unwrap());
    assert_eq!(decoded.len(), 1000);
    for (d, o) in decoded.iter().zip(&records) {
        assert_eq!(format!("{:?}", d.op), format!("{:?}", o.op));
        assert_eq!((d.hlc.wall, d.author_peer), (o.hlc.wall, o.author_peer));
    }
    assert_eq!(max, 1000 * rec_size, "正しい batch は件数ぶんを 1 回で確保する (伸ばし直さない)");

    // 件数が実際より多い batch (途中で切れた応答): 読めた分を捨てて Truncated、 確保は入力の範囲
    let mut cut = batch.clone();
    cut[0..4].copy_from_slice(&5000u32.to_le_bytes());
    let (r, max) = max_request_during(|| decode_batch(&cut));
    assert!(matches!(r, Err(WireDecodeError::Truncated)));
    assert!(max <= (cut.len() / 100 + 1) * rec_size);
}
