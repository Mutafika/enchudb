//! 壊れた packed DB (`Engine::from_bytes`、 relay の bootstrap / wasm の入口) を開いて読む。 panic も UB もしない。
//!
//! 毎回ゼロから作った bytes では header の検査ではじかれて先へ進まないので、 小さな正しい DB (列の型ごとに数行) を
//! 1 度だけ作って packed にし、 入力をその上の書き換えの並びとして当てる: `[位置 u32 LE][長さ u8][bytes]` の繰り返し
//! (位置は DB の長さで割った余り)。 当てた後に header の CRC (`[0..64)` の FNV-1a を 64..68) を付け直す (header を
//! 書き換えても layout の読みまで届くように — SQLite の dbsqlfuzz が checksum を外すのと同じ)。 開けたら、 全部の列の
//! 値を引き、 範囲で引き、 entity ごとに読む。
#![no_main]
use enchudb_engine::{Engine, GrowableOptions, ValueType};
use libfuzzer_sys::fuzz_target;
use std::sync::OnceLock;

static BASE: OnceLock<Vec<u8>> = OnceLock::new();

fn build() -> Vec<u8> {
    let dir = std::env::temp_dir().join(format!("enchudb-fuzz-packed-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let path = dir.to_str().unwrap().to_string();
    let opts = GrowableOptions {
        max_entities: 256,
        max_himos: 8,
        vocab_data_size: 4096,
        content_data_size: Some(4096),
        cyl_max_values: 64,
        leaf_data_size: Some(4096),
        vocab_max_entries: Some(64),
        column_pad: Some(false),
        ..Default::default()
    };
    {
        let mut eng = Engine::create_growable_opts(&path, opts).unwrap();
        eng.define_himo("n", ValueType::Number, 16);
        eng.define_himo("t", ValueType::Tag, 0);
        eng.define_himo("l", ValueType::Leaf, 0);
        eng.define_himo("r", ValueType::Ref, 0);
        eng.define_himo("w", ValueType::Number64, 0);
        let es: Vec<u64> = (0..24).map(|_| eng.entity().unwrap()).collect();
        for (i, &e) in es.iter().enumerate() {
            eng.tie(e, "n", (i % 5) as u32);
            eng.tie_text(e, "t", ["a", "b", "c"][i % 3]);
            eng.tie_text(e, "l", &format!("leaf-{i}"));
            eng.tie(e, "r", enchudb_oplog::eid_local(es[(i + 1) % es.len()]));
            eng.tie(e, "w", (i as u64) << 40);
            if i % 4 == 0 {
                eng.content(e, "memo", format!("memo {i}").as_bytes());
            }
        }
        eng.untie(es[3], "n");
        eng.delete(es[7]);
        eng.flush().unwrap();
    }
    let packed = dir.with_extension("packed");
    Engine::pack_dir(&path, &packed).unwrap();
    let bytes = std::fs::read(&packed).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_file(&packed);
    bytes
}

fn fix_header_crc(buf: &mut [u8]) {
    if buf.len() < 68 {
        return;
    }
    let mut h: u32 = 0x811c9dc5;
    for &b in &buf[0..64] {
        h ^= b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    buf[64..68].copy_from_slice(&h.to_le_bytes());
}

fuzz_target!(|data: &[u8]| {
    let mut buf = BASE.get_or_init(build).clone();
    let mut p = 0;
    while p + 5 <= data.len() {
        let off = u32::from_le_bytes(data[p..p + 4].try_into().unwrap()) as usize % buf.len();
        let n = (data[p + 4] as usize).min(data.len() - p - 5).min(buf.len() - off);
        p += 5;
        buf[off..off + n].copy_from_slice(&data[p..p + n]);
        p += n;
    }
    fix_header_crc(&mut buf);
    let Ok(eng) = Engine::from_bytes(buf) else { return };
    let names: Vec<String> = eng.himo_names().to_vec();
    for name in &names {
        for v in 0..6u32 {
            let _ = eng.pull_raw(name, v);
        }
        let _ = eng.pull_range(name, 0u32, 1000u32);
        let _ = eng.himo_cardinality(name);
    }
    for local in 0..eng.entity_count().min(64) {
        let e = enchudb_oplog::make_eid(eng.peer_id(), local);
        for (hid, name) in names.iter().enumerate() {
            let _ = eng.get_by_id(e, hid as u16);
            let _ = eng.get_text_owned(e, name);
        }
        let _ = eng.get_content_owned(e, "memo");
    }
    let _ = eng.vocab_usage();
});
