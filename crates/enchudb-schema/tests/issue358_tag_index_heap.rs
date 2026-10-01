//! #358: Tag 列の索引 (`LockFreeCylinder`) の dense 配列が、 その列にある値の数ではなく **DB 全体の辞書 ID の
//! 範囲**まで伸び、 途中の全要素に空の bucket (`Arc<AppendBucket>` + `Buf`) を作っていた。 辞書は DB で 1 つ
//! なので、 表の多い DB ほどヒープが 「Tag 列の数 × 辞書の大きさ」 で増えた。
//!
//! issue の再現手順そのまま: T 個の表 (`id` tag PK、 `kind` tag) を作り、 各表で 1 回引いて索引を組んでから、
//! 表を順番に回しながら一意な `id` の行を入れる。 行の総数は固定で、 表の数だけ変える。 ヒープの増分は
//! 数えるだけの `GlobalAlloc` で測る (要求した byte 数の合計、 allocator の管理分は入らない)。
//!
//! 64,000 行での実測 (表 1 / 4 / 8 / 16 個):
//!
//! - 直す前 (0.28.2): 5.5 / 22.8 / 48.7 / 99.4 MB (16 個で 1 行 1,553 B)
//! - 直した後: 5.4 / 7.6 / 10.8 / 17.1 MB (16 個で 1 行 267 B)
//!
//! 空の要素に bucket を作らなくなった分が消えた。 残る増え方 (表 1 個につき約 0.8 MB) は配列そのもの —
//! 空の要素 1 つ 8 B が辞書 ID の範囲まで並ぶ (配列は倍々で伸びるので、 最大でその 2 倍)。
//!
//! `ENCHU_358_REPORT=1 cargo test --release -p enchudb-schema --test issue358_tag_index_heap -- --nocapture`
//! で表の数ごとの増分を出す。

use enchudb_schema::Database;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};

/// 生きている確保の byte 数 (要求した大きさの合計)。
static LIVE: AtomicIsize = AtomicIsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            LIVE.fetch_add(l.size() as isize, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
        LIVE.fetch_sub(l.size() as isize, Ordering::Relaxed);
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, l, new_size) };
        if !q.is_null() {
            LIVE.fetch_add(new_size as isize - l.size() as isize, Ordering::Relaxed);
        }
        q
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue358-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path); // v10: DB は directory
    for suf in ["", ".tables", ".oplog", ".crc", ".db.lock", ".eidmap", ".vocabmap", ".positions"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

/// 表 `tables` 個に合計 `rows` 行を入れた時の (ヒープの増分 (byte)、 入れるのに掛かった秒)。
fn heap_growth(tables: usize, rows: usize) -> (isize, f64) {
    let path = tmp(&format!("t{tables}"));
    cleanup(&path);
    let mut b = Database::create_growable_with_capacity(&path, 4_000_000).unwrap();
    for t in 0..tables {
        b.table(&format!("t{t}")).tag("id").tag("kind").primary_key("id").build().unwrap();
    }
    let db = b.finish_with_oplog(64 << 20).unwrap();
    // 索引を組む (サーバーは id で引くので常に組まれている)
    for t in 0..tables {
        let tb = db.get_table(&format!("t{t}")).unwrap();
        tb.insert().set("id", format!("seed{t}")).set("kind", "k").commit().unwrap();
        tb.where_eq("id", format!("seed{t}")).find_one().unwrap();
        tb.where_eq("kind", "k").find().unwrap();
    }

    let before = LIVE.load(Ordering::Relaxed);
    let t0 = std::time::Instant::now();
    for i in 0..rows / tables {
        for t in 0..tables {
            db.get_table(&format!("t{t}"))
                .unwrap()
                .insert()
                .set("id", format!("t{t}_r{i}"))
                .set("kind", "k")
                .commit()
                .unwrap();
        }
    }
    let secs = t0.elapsed().as_secs_f64();
    let grown = LIVE.load(Ordering::Relaxed) - before;

    // 入れた行が索引から引ける (索引を組んでいない列を測っていないことの確認)
    for t in 0..tables {
        let tb = db.get_table(&format!("t{t}")).unwrap();
        let last = rows / tables - 1;
        assert!(tb.where_eq("id", format!("t{t}_r{last}")).find_one().unwrap().is_some());
        assert_eq!(tb.where_eq("kind", "k").count().unwrap(), rows / tables + 1);
    }
    drop(db);
    cleanup(&path);
    (grown, secs)
}

/// 行の総数が同じなら、 表の数を増やしてもヒープの増分はほとんど増えない。
#[test]
fn tag_index_heap_does_not_scale_with_table_count() {
    const ROWS: usize = 64_000;
    let report = std::env::var("ENCHU_358_REPORT").is_ok();
    let counts: &[usize] = if report { &[1, 4, 8, 16] } else { &[1, 16] };
    let mut grown = Vec::new();
    for &t in counts {
        let (g, secs) = heap_growth(t, ROWS);
        eprintln!(
            "tables {t:>2}: heap +{:.1} MB ({} B / row), insert {:.0} rows/s",
            g as f64 / 1e6,
            g / ROWS as isize,
            ROWS as f64 / secs
        );
        grown.push(g);
    }
    let (one, sixteen) = (grown[0], *grown.last().unwrap());
    // 直す前は表 1 個 5.5 MB / 16 個 99.4 MB (18 倍)。 今は 5.4 MB / 17.1 MB (3.2 倍)。
    // 残りは配列そのもの: 空の要素 1 つ 8 B が辞書 ID の範囲まで並ぶので、 表 1 個につき約 0.8 MB 増える。
    assert!(
        sixteen < one * 6,
        "表 16 個のヒープの増分 {sixteen} B が、 表 1 個 {one} B の 6 倍以上"
    );
}
