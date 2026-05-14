//! 「fuzz ライク」な parser robustness テスト。cargo-fuzz (libFuzzer、nightly 必須)
//! 抜きで proptest でランダム bytes を食わせて panic/UB しないことを検証する。
//!
//! カバー対象(TEST_DESIGN.md P2-8):
//! - WireRecord::decode — v32 sync で相手 peer から受け取る bytes
//! - decode_batch — 複数 record 並びの framing
//! - WAL の iter_committed — 破損 tail を含む WAL ファイルの走査
//!
//! 期待: 任意の bytes 入力で panic/abort せず、Err か空 Vec を返す。
//! 「成功するかどうか」は気にしない(むしろランダムは大半 Err になる)、
//! **サニタイザが抜ける = パニック無し** だけ確認。

#![cfg(feature = "v32")]

use enchudb::transport::{decode_batch, WireRecord};
use proptest::prelude::*;

// ─────────────────────────────────────────────────────────────
// WireRecord::decode
// ─────────────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig { cases: 2048, .. ProptestConfig::default() })]

    #[test]
    fn wire_record_decode_does_not_panic(data in prop::collection::vec(any::<u8>(), 0..8192)) {
        // 任意 bytes に対して Err を返すだけ。panic しないこと。
        let _ = WireRecord::decode(&data);
    }

    #[test]
    fn decode_batch_does_not_panic(data in prop::collection::vec(any::<u8>(), 0..16384)) {
        let _ = decode_batch(&data);
    }

    #[test]
    fn decode_batch_with_count_prefix(
        count in 0u32..1024,
        tail in prop::collection::vec(any::<u8>(), 0..16384)
    ) {
        // count prefix は指定 + 後ろにランダム bytes。極端な count も拒否される。
        let mut buf = Vec::with_capacity(4 + tail.len());
        buf.extend_from_slice(&count.to_le_bytes());
        buf.extend_from_slice(&tail);
        let _ = decode_batch(&buf);
    }
}

// ─────────────────────────────────────────────────────────────
// encode/decode roundtrip は破綻しない
// ─────────────────────────────────────────────────────────────

use enchudb_wal::wal::DecodedOp;
use enchudb_wal::Hlc;

proptest! {
    #[test]
    fn wire_record_encode_then_decode_roundtrip(
        wall in any::<u64>(),
        logical in any::<u32>(),
        peer in any::<u32>(),
        value in any::<u32>(),
        eid in any::<u64>(),
        himo_id in any::<u16>(),
    ) {
        let orig = WireRecord::unsigned(
            Hlc { wall, logical, peer },
            peer,
            DecodedOp::Tie { eid, himo_id, value },
        );
        let enc = orig.encode();
        let (dec, _) = WireRecord::decode(&enc).expect("encode -> decode should roundtrip");
        prop_assert_eq!(dec.hlc.wall, wall);
        prop_assert_eq!(dec.hlc.logical, logical);
        prop_assert_eq!(dec.author_peer, peer);
        // op は Debug 文字列で比較(DecodedOp に PartialEq 無し)
        prop_assert_eq!(format!("{:?}", dec.op), format!("{:?}", orig.op));
    }
}

// ─────────────────────────────────────────────────────────────
// WAL iter_committed の robustness
// ─────────────────────────────────────────────────────────────
//
// WAL ファイルをランダム bytes で作って iter_committed を呼ぶ。
// panic 無し、empty Vec を返すか、途中で切って返るかのいずれか。

use enchudb_wal::wal::Wal;

fn write_wal_with_body(path: &std::path::Path, body: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path)?;
    // WAL file header 32B: magic "EWAL" + version(u32=2) + head(u64=HEADER_SIZE+body.len())
    //                       + checkpoint(u64=HEADER_SIZE) + capacity(u64=body.len()+HEADER_SIZE)
    const HEADER_SIZE: u64 = 32;
    let total = HEADER_SIZE + body.len() as u64;
    f.write_all(b"EWAL")?;
    f.write_all(&2u32.to_le_bytes())?;
    f.write_all(&total.to_le_bytes())?;         // head
    f.write_all(&HEADER_SIZE.to_le_bytes())?;   // checkpoint(最小)
    f.write_all(&total.to_le_bytes())?;          // capacity
    f.write_all(body)?;
    Ok(())
}

fn tmp_wal(tag: &str) -> std::path::PathBuf {
    let p = format!(
        "/tmp/enchudb-fuzz-wal-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    std::path::PathBuf::from(p)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, .. ProptestConfig::default() })]

    #[test]
    fn wal_iter_committed_on_random_body_does_not_panic(
        body in prop::collection::vec(any::<u8>(), 0..2048)
    ) {
        let path = tmp_wal("random_body");
        write_wal_with_body(&path, &body).unwrap();
        // Wal::open は header を validate。破損してなければ iter_committed で panic なく
        // (空 or 途中切り) 帰ってくること。
        if let Ok(wal) = Wal::open(&path) {
            let _ = wal.iter_committed();
        }
        let _ = std::fs::remove_file(&path);
    }
}
