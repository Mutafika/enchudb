//! fuzz の種 (`seeds/<target>/`、 repo に置く) を作る。 libFuzzer が見つけた入力は `corpus/<target>/` (置かない) に溜まる。 `cargo run --release --bin make_seeds` (fuzz/ の中で)。
//! oplog に全部の op の種類を書いて読み戻し、 その file (`oplog_file`)・record ごとの `_sync_ops` の payload
//! (`sync_payload`)・wire の batch (`wire_decode`) を置く。 SQL は文の並びを数本 (`sql_exec`)。
use enchudb_engine::transport::{encode_batch, WireRecord};
use enchudb_oplog::oplog::{Op, OpLog};
use enchudb_oplog::Hlc;
use std::path::Path;

fn put(target: &str, name: &str, bytes: &[u8]) {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("seeds").join(target);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(name), bytes).unwrap();
}

fn main() {
    let tmp = std::env::temp_dir().join(format!("enchudb-seeds-{}.oplog", std::process::id()));
    let _ = std::fs::remove_file(&tmp);
    {
        let log = OpLog::create(&tmp, 4096).unwrap();
        log.set_peer_id(7);
        let eid = enchudb_oplog::make_eid(7, 3);
        let ops = [
            Op::Vocab { vid: 5, bytes: b"tokyo" },
            Op::Tie { eid, himo_id: 2, value: 5 },
            Op::Tie { eid, himo_id: 3, value: 1 << 40 },
            Op::Untie { eid, himo_id: 2 },
            Op::Content { eid, key: "memo", data: b"hello" },
            Op::TieNamed { eid, himo_name: "_c_memo", himo_kind: 1, value: 9 },
            Op::TieNamed { eid, himo_name: "users.at", himo_kind: 4, value: 1 << 41 },
            Op::TieLeaf { eid, himo_name: "users.name", himo_kind: 3, bytes: b"alice" },
            Op::TieRef { eid, himo_id: 4, target: enchudb_oplog::make_eid(9, 1) },
            Op::Delete { eid },
        ];
        for op in ops {
            log.append(op).unwrap();
        }
        log.append(Op::Commit).unwrap();
        let recs = log.recover();
        assert_eq!(recs.len(), 10, "種の record が全部読めない");
        let mut wire = Vec::new();
        for (i, r) in recs.iter().enumerate() {
            let mut payload = Vec::new();
            payload.extend_from_slice(&r.signature);
            payload.extend_from_slice(&r.pubkey_fp);
            payload.extend_from_slice(&r.signed_bytes);
            put("sync_payload", &format!("rec{i}"), &payload);
            let w = WireRecord {
                hlc: r.hlc,
                author_peer: r.author_peer,
                op: r.op.clone(),
                signature: r.signature,
                pubkey_fp: r.pubkey_fp,
                signed_bytes: r.signed_bytes.clone(),
            };
            put("wire_decode", &format!("one{i}"), &encode_batch(std::slice::from_ref(&w)));
            wire.push(w);
        }
        put("wire_decode", "all", &encode_batch(&wire));
        let _ = Hlc::ZERO;
    }
    put("oplog_file", "ops", &std::fs::read(&tmp).unwrap());
    let _ = std::fs::remove_file(&tmp);

    let sql = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER); INSERT INTO t VALUES (1, 'a', 30); \
         INSERT INTO t VALUES (2, 'b', 40); SELECT * FROM t WHERE age > 20 ORDER BY age DESC LIMIT 1",
        "CREATE TABLE n (key TEXT PRIMARY KEY, at INTEGER); INSERT OR REPLACE INTO n VALUES ('k', 1715174400); \
         UPDATE n SET at = 0 WHERE key = 'k'; DELETE FROM n WHERE key = 'k'; SELECT key, at FROM n WHERE at IS NOT NULL",
        "CREATE TABLE u (id INTEGER PRIMARY KEY, v BIGINT); INSERT INTO u VALUES (-5, -9000000000000000000); \
         SELECT * FROM u WHERE v < 0 AND id IN (-5, 3); DROP TABLE u",
    ];
    for (i, s) in sql.iter().enumerate() {
        put("sql_exec", &format!("s{i}.sql"), s.as_bytes());
    }
    put("packed_open", "empty", b"");
    println!("seeds written");
}
