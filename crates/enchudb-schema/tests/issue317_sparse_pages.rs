//! #317: 書いたページは書き出しで本当にディスクを食う (16 KB)。 初めて触るページを数えないと、 使える空きを超えても
//! 気付けない。 使える空き 2 MB で新しい Tag の値を書き続ける: 空きを超える前に commit が Err になり、 開き直すと
//! Ok の row は全部読める。 プロセス全体の合計を見るので 1 file 1 テスト。
//!
//! 0.28.5 までは辞書の索引の slot が領域全体に散り、 新しい値ごとに別のページを触ったので、 この試験は 「値 1 つ = 高々
//! 1 ページ」 を上限に断るまでを見ていた。 #374 から索引は語数に合わせて伸び、 ページは値ごとには増えない (先に列や
//! 辞書の data が空きを使い切る、 実測 2 万行前後)。 索引の表を書く時 (伸ばす / 開いた時に作り直す、 同じ
//! `write_gen`) の空きの数え方は `issue327_rebuild_space` が見る。
//!
//! 使える空きは 「走り始めのディスクの空き − 2 MB」 を線にして作るので、 走っている間にほかの process がディスクの空きを
//! 動かすと (file を消す・build が書く)、 予算が崩れる (空きが増えると断られずに入り続け、 減ると早く断られる)。 その回は
//! やり直す: 1000 行ごとに空きを見て、 走り始めより 1 MB を超えて増えていたら、 または 100 行以内で断られた時に 4 MB を
//! 超えて減っていたら、 DB を消して作り直す (5 回まで)。

use enchudb_schema::{Database, SchemaError, Value};

/// panic しても DB を消す (変異試験で落とすたびに /tmp に数百 MB 残っていた)。
struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn name(i: i64) -> String {
    format!("tag-{i:08}")
}

const USABLE: u64 = 2 << 20;
/// 走り始めより空きがこれを超えて増えたら、 ほかの process が空けた (予算が広がった)。
const GREW: u64 = 1 << 20;
/// 早く断られた時に、 走り始めより空きがこれを超えて減っていたら、 ほかの process が使った (予算が縮んだ)。
const SHRANK: u64 = 4 << 20;

/// 1 回分: 使える空き 2 MB で入れ続け、 断られた行数を返す (DB は書き出しておく)。 走っている間にほかの process で
/// ディスクの空きが動いた時は None (呼び手がやり直す)。
fn attempt(path: &str) -> Option<i64> {
    let mut db = Database::create(path).unwrap();
    db.table("tags").number("id").tag("name").primary_key("id").build().unwrap();
    let t = db.get_table("tags").unwrap();
    let eng = db.engine();
    let free = eng.disk_free_bytes().expect("growable backing");
    eng.set_space_margin(free.saturating_sub(USABLE));
    let moved = |grew_only: bool| {
        let now = eng.disk_free_bytes().unwrap_or(free);
        now > free + GREW || (!grew_only && now + SHRANK < free)
    };
    let mut ok = 0i64;
    let n = loop {
        if ok % 1000 == 0 && moved(true) {
            eprintln!("{ok} 行目: ディスクの空きが走り始めより増えた (ほかの process)。 やり直す");
            return None;
        }
        // 1 行は数十 B。 100 万行入っても断らなければ、 書いたページを空きから引いていない
        assert!(ok < 1_000_000, "使える空き {USABLE} B で 100 万行入った (書いたページを空きから引いていない)");
        match t.insert().set("id", ok).set("name", name(ok).as_str()).commit() {
            Ok(_) => ok += 1,
            Err(SchemaError::WriteRejected(_)) => break ok,
            Err(e) => panic!("row {ok}: {e}"),
        }
    };
    if n <= 100 && moved(false) {
        eprintln!("{n} 行で拒否: ディスクの空きが走り始めより減った (ほかの process)。 やり直す");
        return None;
    }
    eprintln!("{n} 行で拒否");
    assert!(n > 100, "早すぎる拒否 ({n} 行)");
    eng.set_space_margin(0);
    drop(t);
    db.engine_mut().expect("単独所有").flush().unwrap();
    Some(n)
}

#[test]
fn new_index_pages_are_counted() {
    let base = format!("/tmp/enchudb-issue317-pages-{}", std::process::id());
    let mut tried = 0;
    let (path, n, _cleanup) = loop {
        assert!(tried < 5, "5 回ともディスクの空きが走っている間に動いた (ほかの process)");
        let path = format!("{base}-{tried}.db");
        let _ = std::fs::remove_dir_all(&path);
        let cleanup = Cleanup(path.clone());
        tried += 1;
        if let Some(n) = attempt(&path) {
            break (path, n, cleanup);
        }
    };
    let db = Database::open(&path).unwrap();
    let t = db.get_table("tags").unwrap();
    for i in 0..n {
        let e = t.where_eq("id", i).find_one().unwrap().unwrap_or_else(|| panic!("row {i} が無い"));
        assert_eq!(t.entity(e).get("name"), Some(Value::Text(name(i))), "row {i}");
    }
    drop(t);
    drop(db);
    let _ = std::fs::remove_dir_all(&path);
}
