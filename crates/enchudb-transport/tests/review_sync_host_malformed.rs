//! review (0.37.4): ホストに、 形は正しいが中身のおかしい record を送る (`POST /v2/publish?peer=..`、 誰でも送れる)。 当てる途中で
//! panic すると、 その接続の thread が client ごとの lock (`HostInner::client_locks`) を握ったまま死に、 lock が毒になる = その
//! peer id の以後の publish は全部 `lock().unwrap()` で panic する。 `peer=` は確かめないので、 他人の peer id を毒にできる。
//!
//! 1 つずつ送り、 その後で同じ peer id の正しい publish が通るか (返事が来るか) を見る。

use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::transport::{WireRecord, encode_batch};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId, make_eid};
use enchudb_transport::host::HttpHost;

const HOST: PeerId = 100;

fn db(path: &str, peer: PeerId) -> Arc<Engine> {
    {
        let mut e = Engine::create_with_cell_version(path, 65_536).unwrap();
        e.define_table("notes", 4096).unwrap();
        e.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "t", ValueType::Tag, 0).unwrap();
        e.define_himo_in("notes", "r", ValueType::Ref, 0).unwrap();
        let k = e.himo_id("notes.k").unwrap() as u16;
        e.set_table_pk("notes", k).unwrap();
        e.enable_sync_tables().unwrap();
        e.flush().unwrap();
    }
    let e = Engine::open_concurrent_with_oplog(path, 16 << 20).unwrap();
    e.set_peer_id(peer);
    e
}

/// 戻り値: (status, body)。 接続が切れたら status 0。
fn post(addr: std::net::SocketAddr, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
    let Ok(mut s) = std::net::TcpStream::connect(addr) else { return (0, Vec::new()) };
    let _ = s.set_read_timeout(Some(std::time::Duration::from_secs(20)));
    let head = format!("POST {path} HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
    if s.write_all(head.as_bytes()).is_err() || s.write_all(body).is_err() {
        return (0, Vec::new());
    }
    let mut out = Vec::new();
    let _ = s.read_to_end(&mut out);
    let text = String::from_utf8_lossy(&out);
    let status = text.split(' ').nth(1).and_then(|c| c.parse().ok()).unwrap_or(0);
    let body = out.windows(4).position(|w| w == b"\r\n\r\n").map(|p| out[p + 4..].to_vec()).unwrap_or_default();
    (status, body)
}

#[test]
fn odd_records_do_not_poison_a_peer_id() {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("review-host-malformed-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let host_eng = db(dir.join("host").to_str().unwrap(), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let addr = host.addr();
    let (k, v, t, r) = (
        host_eng.himo_id("notes.k").unwrap() as u16,
        host_eng.himo_id("notes.v").unwrap() as u16,
        host_eng.himo_id("notes.t").unwrap() as u16,
        host_eng.himo_id("notes.r").unwrap() as u16,
    );
    let mut wall = 1_900_000_000_000u64;
    let mut hlc = |peer: PeerId| {
        wall += 1;
        Hlc { wall, logical: 0, peer }
    };
    let cases: Vec<(&str, PeerId, Vec<DecodedOp>)> = vec![
        ("himo_id が範囲の外", 7, vec![DecodedOp::Tie { eid: make_eid(7, 0), himo_id: 60_000, value: 1 }]),
        ("himo_id = u16::MAX の Tie", 8, vec![DecodedOp::Tie { eid: make_eid(8, 0), himo_id: u16::MAX, value: 1 }]),
        ("local が u32::MAX の eid", 9, vec![DecodedOp::Tie { eid: make_eid(9, u32::MAX), himo_id: v, value: 1 }]),
        ("u32 の列に u64::MAX", 10, vec![DecodedOp::Tie { eid: make_eid(10, 0), himo_id: v, value: u64::MAX }]),
        ("名前に u64::MAX", 11, vec![DecodedOp::Tie { eid: make_eid(11, 0), himo_id: k, value: u64::MAX }]),
        ("辞書に無い番号の Tag", 12, vec![DecodedOp::Tie { eid: make_eid(12, 0), himo_id: t, value: 4_000_000_000 }]),
        ("Ref の値が u64::MAX", 13, vec![DecodedOp::Tie { eid: make_eid(13, 0), himo_id: r, value: u64::MAX }]),
        ("範囲の外の himo の Untie", 14, vec![DecodedOp::Untie { eid: make_eid(14, 0), himo_id: 50_000 }]),
        ("sync の内部の表の himo への Tie", 15, vec![DecodedOp::Tie { eid: make_eid(15, 0), himo_id: host_eng.himo_id("_sync_ops.lsn").map(|h| h as u16).unwrap_or(4), value: 1 }]),
        ("種類のおかしい TieNamed", 16, vec![DecodedOp::TieNamed { eid: make_eid(16, 0), himo_name: "notes.v".into(), himo_kind: 250, value: 1 }]),
        ("既にある列を Leaf として名乗る TieLeaf", 17, vec![DecodedOp::TieLeaf { eid: make_eid(17, 0), himo_name: "notes.k".into(), himo_kind: ValueType::Leaf as u8, bytes: vec![1, 2, 3] }]),
        ("内部の表の名前の TieNamed", 18, vec![DecodedOp::TieNamed { eid: make_eid(18, 0), himo_name: "_sync_ops.lsn".into(), himo_kind: ValueType::Number as u8, value: 7 }]),
        ("空の名前の TieNamed", 19, vec![DecodedOp::TieNamed { eid: make_eid(19, 0), himo_name: String::new(), himo_kind: ValueType::Number as u8, value: 7 }]),
        ("ホスト自身の eid の Delete", 20, vec![DecodedOp::Delete { eid: make_eid(HOST, 0) }]),
        ("自分を指す Alias", 21, vec![DecodedOp::Alias { eid: make_eid(21, 0), same_as: make_eid(21, 0) }]),
        ("ホストの行と結ぶ Alias", 22, vec![DecodedOp::Tie { eid: make_eid(22, 0), himo_id: k, value: 5 }, DecodedOp::Alias { eid: make_eid(22, 0), same_as: make_eid(HOST, 1) }]),
        ("peer 0 の eid", 23, vec![DecodedOp::Tie { eid: make_eid(0, 3), himo_id: v, value: 1 }]),
        ("巨大な Vocab", 24, vec![DecodedOp::Vocab { vid: 1, bytes: vec![b'x'; 3 << 20] }]),
    ];
    let mut poisoned = Vec::new();
    for (name, peer, ops) in cases {
        let records: Vec<WireRecord> = ops.into_iter().map(|op| WireRecord::unsigned(hlc(peer), peer, op)).collect();
        let (status, _) = post(addr, &format!("/v2/publish?peer={peer}"), &encode_batch(&records));
        // 同じ peer id の、 正しい publish (名前 900+peer の行)
        let good = vec![
            WireRecord::unsigned(hlc(peer), peer, DecodedOp::Tie { eid: make_eid(peer, 77), himo_id: k, value: 900 + peer as u64 }),
            WireRecord::unsigned(hlc(peer), peer, DecodedOp::Tie { eid: make_eid(peer, 77), himo_id: v, value: 1 }),
        ];
        let (after, _) = post(addr, &format!("/v2/publish?peer={peer}"), &encode_batch(&good));
        let landed = host_eng.pull_raw("notes.k", 900 + peer).len();
        println!("[malformed] {name}: 返事 {status} → 同じ peer の次の publish: 返事 {after}、 行 {landed}");
        if after != 200 || landed != 1 {
            poisoned.push(name);
        }
    }
    let hello = {
        let mut s = std::net::TcpStream::connect(addr).unwrap();
        s.write_all(b"GET /v2/hello HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").unwrap();
        let mut out = Vec::new();
        let _ = s.read_to_end(&mut out);
        String::from_utf8_lossy(&out).starts_with("HTTP/1.1 200")
    };
    let broken = host_eng.verify_invariants().violations;
    println!("[malformed] ホストはまだ応える: {hello}、 不変量の違反: {broken:?}");
    drop(host);
    drop(host_eng);
    let _ = std::fs::remove_dir_all(&dir);
    assert!(poisoned.is_empty(), "この record の後、 同じ peer id の publish が通らない: {poisoned:?}");
    assert!(hello && broken.is_empty(), "ホストが壊れた: {broken:?}");
}
