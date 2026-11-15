//! issue #61: `.schema` sidecar の serialize は table/column 名を区切り文字
//! `|` `;` `:` 改行・ relation `->` で連結するため、 名前にこれらが入ると
//! round-trip で schema が壊れる。 build 時に弾くことを確認する。

use enchudb_schema::Database;

/// 試験の DB の path。 pid / 連番 / 時刻を入れて、 並べて回した別の process の試験と取り合わない (#906)。
fn tmp(name: &str) -> String {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let p = format!("/tmp/enchudb_issue61_{name}_{}_{seq}_{nanos}.db", std::process::id());
    TMP_PATHS.with(|t| t.0.borrow_mut().push(p.clone()));
    let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
    let _ = std::fs::remove_file(&p);
    let _ = std::fs::remove_file(format!("{p}.schema"));
    let _ = std::fs::remove_file(format!("{p}.tables"));
    p
}

/// [`tmp`] が渡した path。 試験の thread の終わりに消す (名前が run ごとに変わるので、 試験の中で消さない分が積もらないように)。
struct TmpPaths(std::cell::RefCell<Vec<String>>);

impl Drop for TmpPaths {
    fn drop(&mut self) {
        for p in self.0.get_mut().drain(..) {
            let _ = std::fs::remove_dir_all(&p);
            let _ = std::fs::remove_file(&p);
        }
    }
}

thread_local! {
    static TMP_PATHS: TmpPaths = const { TmpPaths(std::cell::RefCell::new(Vec::new())) };
}

#[test]
fn table_name_with_pipe_rejected() {
    let path = tmp("tbl_pipe");
    let mut db = Database::create(&path).unwrap();
    let r = db.table("users|posts").number("id").primary_key("id").build();
    assert!(r.is_err(), "table name with '|' must be rejected");
    let msg = format!("{}", r.err().unwrap());
    assert!(msg.contains("reserved"), "got: {msg}");
}

#[test]
fn column_name_with_colon_rejected() {
    let path = tmp("col_colon");
    let mut db = Database::create(&path).unwrap();
    let r = db.table("t").number("a:b").primary_key("a:b").build();
    assert!(r.is_err(), "column name with ':' must be rejected");
}

#[test]
fn column_name_with_semicolon_rejected() {
    let path = tmp("col_semi");
    let mut db = Database::create(&path).unwrap();
    let r = db.table("t").tag("name;drop").build();
    assert!(r.is_err(), "column name with ';' must be rejected");
}

#[test]
fn name_with_arrow_rejected() {
    let path = tmp("arrow");
    let mut db = Database::create(&path).unwrap();
    let r = db.table("a->b").number("id").primary_key("id").build();
    assert!(r.is_err(), "name with '->' must be rejected");
}

#[test]
fn clean_names_still_build() {
    // 通常の名前は従来どおり通ること (regression を作らない)。
    let path = tmp("clean");
    let mut db = Database::create(&path).unwrap();
    let r = db.table("users").number("id").tag("name").primary_key("id").build();
    assert!(r.is_ok(), "ordinary names must still build: {:?}", r.err());
}
