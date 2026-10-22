//! `.eidmap` の追記 (#560)。
//!
//! `eidmap` (翻訳の写像 + 削除の記録、 `Engine::eidmap_entries_with_tombstones`) は pull で何か当てるたびに永続する。 前は毎回
//! まるごと組み直して書き直していたので、 写像が 100 万件で 1 回 40 ms・27 MB の書き直しになった。 変わった key の今の中身だけを
//! 横の `eidmap.log` に追記し、 log が大きくなった時ときれいに閉じる時にまとめ直す (`eidmap` を書き直して log を空にする)。
//!
//! `eidmap` の形は変えない (読まない旧 binary は `eidmap` だけを読む。 きれいに閉じた後は log が空なので全部ある)。
//!
//! key = (peer, foreign_local) ごとに 2 つの面がある:
//!
//! - 写像の面: 逆写像の組 (local → key) と、 その local の削除の版数
//! - 退避した削除の面: slot を持たない削除の版数 (`NO_LOCAL_SLOT`、 増えるだけ)
//!
//! file: header = magic "EIDL"(4) + version u32 + 続く `eidmap` の長さ u64 + その FNV-1a 64 (header が今の `eidmap` と合わなければ
//! log は読まない: まとめ直しの途中で落ちた跡)。 続いて record (32 B) の列: peer u32, foreign_local u32, local u32, 版数 wall u64 /
//! logical u32 / peer u32, その 28 B の FNV-1a 32。 local は普通の slot なら写像の面を置き換え、 `NO_LOCAL_SLOT` なら退避した削除
//! (大きい方)、 `MAPPING_GONE` なら写像の面を外す。 読む時は、 切れた / 壊れた record から後を捨てる (追記の途中で落ちた跡。 その時は
//! 次の書き出しでまとめ直す。 切れた跡の後ろに追記すると、 次に読む時にそこで止まって失う)。

use std::collections::HashMap;

use enchudb_oplog::{Hlc, PeerId};

/// `eidmap` の entry (peer, foreign_local, local, 削除の版数)。 engine の `EidmapEntry` と同じ。
pub(crate) type Entry = (PeerId, u32, u32, Hlc);

const MAGIC: &[u8; 4] = b"EIDL";
const VERSION: u32 = 1;
pub(crate) const HEADER_LEN: usize = 24;
pub(crate) const RECORD_LEN: usize = 32;
/// `eidmap` の 「写像を持たない削除記録」 の local (engine の `NO_LOCAL_SLOT` と同じ)。
pub(crate) const NO_LOCAL_SLOT: u32 = u32::MAX;
/// log の record で 「この key の写像の面を外す」。 slot は `max_entities` 未満なので重ならない。
pub(crate) const MAPPING_GONE: u32 = u32::MAX - 1;
/// まとめ直す目安: log の record の数が、 生きている記録の 2 倍とこの数の和を越えたら。
pub(crate) const REWRITE_SLACK: u64 = 1024;

/// log の今の様子 (engine が lock の下で持つ)。
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub(crate) struct LogState {
    /// log にある record の数。
    pub records: u64,
    /// log が今の `eidmap` の続きとして信用できるか。 false なら次の書き出しでまとめ直す (log が無い / header が合わない /
    /// 切れた record がある / 前の書き出しが失敗した)。
    pub ok: bool,
}

/// `eidmap` の bytes `snapshot` の続きの、 record の無い log。
pub(crate) fn header(snapshot: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&(snapshot.len() as u64).to_le_bytes());
    out.extend_from_slice(&crate::change_log::fnv1a64(snapshot).to_le_bytes());
    out
}

/// record を 1 つ足す。
pub(crate) fn push_record(out: &mut Vec<u8>, (peer, foreign_local, local, tomb): Entry) {
    let start = out.len();
    out.extend_from_slice(&peer.to_le_bytes());
    out.extend_from_slice(&foreign_local.to_le_bytes());
    out.extend_from_slice(&local.to_le_bytes());
    out.extend_from_slice(&tomb.wall.to_le_bytes());
    out.extend_from_slice(&tomb.logical.to_le_bytes());
    out.extend_from_slice(&tomb.peer.to_le_bytes());
    let sum = crate::integrity::fnv1a_region(&out[start..]);
    out.extend_from_slice(&sum.to_le_bytes());
}

/// log `buf` を読む。 `snapshot` (今の `eidmap` の bytes) の続きでなければ None。 続きなら (record の列, 切れた / 壊れた record が
/// あったか)。
pub(crate) fn read(buf: &[u8], snapshot: &[u8]) -> Option<(Vec<Entry>, bool)> {
    if buf.len() < HEADER_LEN || &buf[..4] != MAGIC || u32::from_le_bytes(buf[4..8].try_into().unwrap()) != VERSION {
        return None;
    }
    if buf[..HEADER_LEN] != header(snapshot)[..] {
        return None;
    }
    let mut out = Vec::with_capacity((buf.len() - HEADER_LEN) / RECORD_LEN);
    let mut off = HEADER_LEN;
    while off + RECORD_LEN <= buf.len() {
        let r = &buf[off..off + RECORD_LEN];
        if crate::integrity::fnv1a_region(&r[..28]) != u32::from_le_bytes(r[28..32].try_into().unwrap()) {
            return Some((out, true));
        }
        let u32_at = |i: usize| u32::from_le_bytes(r[i..i + 4].try_into().unwrap());
        let wall = u64::from_le_bytes(r[12..20].try_into().unwrap());
        out.push((u32_at(0), u32_at(4), u32_at(8), Hlc { wall, logical: u32_at(20), peer: u32_at(24) }));
        off += RECORD_LEN;
    }
    Some((out, off != buf.len()))
}

/// `eidmap` の entry の列に log の record を当てる (key ごとに写像の面は後の record が勝ち、 退避した削除は大きい方)。 写像の面を
/// 先に、 key の順で並べる。
pub(crate) fn merge(snapshot: Vec<Entry>, log: &[Entry]) -> Vec<Entry> {
    let mut mapped: HashMap<(PeerId, u32), (u32, Hlc)> = HashMap::with_capacity(snapshot.len());
    let mut tombs: HashMap<(PeerId, u32), Hlc> = HashMap::new();
    let mut apply = |(peer, fl, local, tomb): Entry| match local {
        NO_LOCAL_SLOT => {
            let t = tombs.entry((peer, fl)).or_insert(Hlc::ZERO);
            *t = (*t).max(tomb);
        }
        MAPPING_GONE => {
            mapped.remove(&(peer, fl));
        }
        _ => {
            mapped.insert((peer, fl), (local, tomb));
        }
    };
    for e in snapshot {
        apply(e);
    }
    for &e in log {
        apply(e);
    }
    let mut out: Vec<Entry> = mapped.into_iter().map(|((p, f), (l, t))| (p, f, l, t)).collect();
    out.sort_unstable();
    let mut orphan: Vec<Entry> = tombs.into_iter().map(|((p, f), t)| (p, f, NO_LOCAL_SLOT, t)).collect();
    orphan.sort_unstable();
    out.extend(orphan);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hlc(w: u64) -> Hlc {
        Hlc { wall: w, logical: 0, peer: 9 }
    }

    #[test]
    fn records_round_trip_and_merge_in_order() {
        let snap = b"EIDM-snapshot".to_vec();
        let mut log = header(&snap);
        for e in [(2, 1, 10, Hlc::ZERO), (2, 2, 11, Hlc::ZERO), (2, 1, MAPPING_GONE, Hlc::ZERO), (2, 3, NO_LOCAL_SLOT, hlc(5)), (2, 2, 11, hlc(7))] {
            push_record(&mut log, e);
        }
        let (recs, torn) = read(&log, &snap).unwrap();
        assert!(!torn);
        assert_eq!(recs.len(), 5);
        let merged = merge(vec![(2, 1, 10, Hlc::ZERO), (2, 3, NO_LOCAL_SLOT, hlc(9)), (2, 4, 12, Hlc::ZERO)], &recs);
        assert_eq!(merged, vec![(2, 2, 11, hlc(7)), (2, 4, 12, Hlc::ZERO), (2, 3, NO_LOCAL_SLOT, hlc(9))]);
    }

    #[test]
    fn a_log_for_another_snapshot_is_not_read() {
        let mut log = header(b"old");
        push_record(&mut log, (1, 1, 1, Hlc::ZERO));
        assert!(read(&log, b"new").is_none(), "まとめ直した後の eidmap に前の log を当てた");
        assert!(read(&log, b"old").is_some());
    }

    #[test]
    fn a_torn_or_corrupt_record_ends_the_log() {
        let snap = b"s".to_vec();
        let mut log = header(&snap);
        push_record(&mut log, (1, 1, 1, Hlc::ZERO));
        push_record(&mut log, (1, 2, 2, Hlc::ZERO));
        let (recs, torn) = read(&log[..log.len() - 3], &snap).unwrap();
        assert_eq!((recs.len(), torn), (1, true), "切れた record を読んだ");
        let mut bad = log.clone();
        bad[HEADER_LEN + RECORD_LEN + 5] ^= 1;
        let (recs, torn) = read(&bad, &snap).unwrap();
        assert_eq!((recs.len(), torn), (1, true), "壊れた record を読んだ");
    }
}
