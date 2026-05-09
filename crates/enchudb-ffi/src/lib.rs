//! EnchuDB C ABI — 全言語から叩ける凍結 layer。
//!
//! ## 設計方針
//! - SQLite 風の関数命名と return code (0 = OK, 非 0 = error)
//! - opaque handle (`*mut enchudb_db`, `*mut enchudb_result`)
//! - 文字列は NUL 終端 C string、UTF-8 想定
//! - 失敗時は handle が NULL or rc が非 0、`enchudb_last_error` で詳細
//! - heap alloc は handle 寿命にバインド、call 毎の alloc は最小化
//!
//! ## C 側使用例
//!
//! see crates/enchudb-ffi/examples/demo.c
//!
//!
//! ```c
//! enchudb_db* db;
//! if (enchudb_open("/tmp/x.db", &db) != 0) { /* error */ }
//! enchudb_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
//! enchudb_exec(db, "INSERT INTO t VALUES (1, 'alice')");
//!
//! enchudb_result* r;
//! enchudb_query(db, "SELECT * FROM t WHERE id = 1", &r);
//! for (size_t i = 0; i < enchudb_result_rows(r); i++) {
//!     int64_t id = enchudb_result_int(r, i, 0);
//!     const char* name = enchudb_result_text(r, i, 1);
//!     printf("%lld %s\n", (long long)id, name);
//! }
//! enchudb_result_free(r);
//! enchudb_close(db);
//! ```

#![allow(non_camel_case_types)]

use enchudb_sql::{Database, Output, Value};
use std::ffi::{CStr, CString, c_char};
use std::ptr;

// ────── return codes ──────

pub const ENCHUDB_OK: i32 = 0;
pub const ENCHUDB_ERROR: i32 = 1;
pub const ENCHUDB_INVALID_ARG: i32 = 2;
pub const ENCHUDB_INVALID_UTF8: i32 = 3;
pub const ENCHUDB_NO_RESULT: i32 = 4;

// ────── opaque handles ──────

pub struct enchudb_db {
    inner: Database,
    last_error: CString,
}

pub struct enchudb_result {
    columns: Vec<CString>,
    rows: Vec<Vec<CellRepr>>,
}

enum CellRepr {
    Null,
    Integer(i64),
    Text(CString),
}

// ────── helpers ──────

unsafe fn cstr_to_str<'a>(p: *const c_char) -> Result<&'a str, i32> {
    if p.is_null() { return Err(ENCHUDB_INVALID_ARG); }
    unsafe { CStr::from_ptr(p) }.to_str().map_err(|_| ENCHUDB_INVALID_UTF8)
}

fn set_error(db: &mut enchudb_db, msg: impl AsRef<str>) {
    db.last_error = CString::new(msg.as_ref()).unwrap_or_default();
}

fn make_result(out: Output) -> enchudb_result {
    match out {
        Output::Rows { columns, rows } => {
            let cols: Vec<CString> = columns.into_iter()
                .map(|c| CString::new(c).unwrap_or_default())
                .collect();
            let rs: Vec<Vec<CellRepr>> = rows.into_iter().map(|row| {
                row.into_iter().map(|v| match v {
                    Value::Null => CellRepr::Null,
                    Value::Integer(n) => CellRepr::Integer(n),
                    Value::Text(s) => CellRepr::Text(CString::new(s).unwrap_or_default()),
                }).collect()
            }).collect();
            enchudb_result { columns: cols, rows: rs }
        }
        _ => enchudb_result { columns: Vec::new(), rows: Vec::new() },
    }
}

// ────── DB lifecycle ──────

/// DB を開く（既存なら open、無ければ create）。
/// 成功時 *out_db に handle が入り 0 を返す。失敗時 *out_db = NULL、非 0 rc。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn enchudb_open(
    path: *const c_char,
    out_db: *mut *mut enchudb_db,
) -> i32 {
    if out_db.is_null() { return ENCHUDB_INVALID_ARG; }
    unsafe { *out_db = ptr::null_mut(); }
    let path = match unsafe { cstr_to_str(path) } {
        Ok(s) => s,
        Err(rc) => return rc,
    };

    let result = if std::path::Path::new(path).exists() {
        Database::open(path)
    } else {
        Database::create(path)
    };
    match result {
        Ok(inner) => {
            let boxed = Box::new(enchudb_db { inner, last_error: CString::default() });
            unsafe { *out_db = Box::into_raw(boxed); }
            ENCHUDB_OK
        }
        Err(_) => ENCHUDB_ERROR,
    }
}

/// 明示 create（既存ファイルがあると Engine 側でエラー）。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn enchudb_create(
    path: *const c_char,
    out_db: *mut *mut enchudb_db,
) -> i32 {
    if out_db.is_null() { return ENCHUDB_INVALID_ARG; }
    unsafe { *out_db = ptr::null_mut(); }
    let path = match unsafe { cstr_to_str(path) } {
        Ok(s) => s,
        Err(rc) => return rc,
    };
    match Database::create(path) {
        Ok(inner) => {
            let boxed = Box::new(enchudb_db { inner, last_error: CString::default() });
            unsafe { *out_db = Box::into_raw(boxed); }
            ENCHUDB_OK
        }
        Err(_) => ENCHUDB_ERROR,
    }
}

/// DB handle を破棄。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn enchudb_close(db: *mut enchudb_db) -> i32 {
    if db.is_null() { return ENCHUDB_INVALID_ARG; }
    unsafe { drop(Box::from_raw(db)); }
    ENCHUDB_OK
}

// ────── exec / query ──────

/// SQL を実行（結果不要 / DDL / INSERT / UPDATE / DELETE 用）。
/// SELECT も流せるが結果は捨てられる。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn enchudb_exec(db: *mut enchudb_db, sql: *const c_char) -> i32 {
    if db.is_null() { return ENCHUDB_INVALID_ARG; }
    let db = unsafe { &mut *db };
    let sql = match unsafe { cstr_to_str(sql) } {
        Ok(s) => s,
        Err(rc) => return rc,
    };
    match db.inner.execute(sql) {
        Ok(_) => ENCHUDB_OK,
        Err(e) => { set_error(db, e.to_string()); ENCHUDB_ERROR }
    }
}

/// SQL を実行して結果を `*out_result` に返す。SELECT 用。
/// 結果を読み終えたら `enchudb_result_free` で解放すること。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn enchudb_query(
    db: *mut enchudb_db,
    sql: *const c_char,
    out_result: *mut *mut enchudb_result,
) -> i32 {
    if db.is_null() || out_result.is_null() { return ENCHUDB_INVALID_ARG; }
    unsafe { *out_result = ptr::null_mut(); }
    let db = unsafe { &mut *db };
    let sql = match unsafe { cstr_to_str(sql) } {
        Ok(s) => s,
        Err(rc) => return rc,
    };
    match db.inner.execute(sql) {
        Ok(out) => {
            let r = make_result(out);
            unsafe { *out_result = Box::into_raw(Box::new(r)); }
            ENCHUDB_OK
        }
        Err(e) => { set_error(db, e.to_string()); ENCHUDB_ERROR }
    }
}

/// 直近 enchudb_exec / enchudb_query が失敗したときのエラーメッセージ。
/// 返ったポインタは次の API call まで有効、call をまたいで保持しないこと。
/// 成功直後 / エラー無しなら空文字列を返す。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn enchudb_last_error(db: *mut enchudb_db) -> *const c_char {
    if db.is_null() { return ptr::null(); }
    unsafe { (*db).last_error.as_ptr() }
}

// ────── result accessors ──────

#[unsafe(no_mangle)]
pub unsafe extern "C" fn enchudb_result_rows(r: *const enchudb_result) -> usize {
    if r.is_null() { return 0; }
    unsafe { (*r).rows.len() }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn enchudb_result_cols(r: *const enchudb_result) -> usize {
    if r.is_null() { return 0; }
    unsafe { (*r).columns.len() }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn enchudb_result_col_name(
    r: *const enchudb_result,
    col: usize,
) -> *const c_char {
    if r.is_null() { return ptr::null(); }
    let r = unsafe { &*r };
    if col >= r.columns.len() { return ptr::null(); }
    r.columns[col].as_ptr()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn enchudb_result_is_null(
    r: *const enchudb_result,
    row: usize,
    col: usize,
) -> i32 {
    if r.is_null() { return 1; }
    let r = unsafe { &*r };
    if row >= r.rows.len() || col >= r.columns.len() { return 1; }
    matches!(r.rows[row][col], CellRepr::Null) as i32
}

/// INTEGER として値を取得。NULL / TEXT セルは 0 を返す（is_null で先に確認推奨）。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn enchudb_result_int(
    r: *const enchudb_result,
    row: usize,
    col: usize,
) -> i64 {
    if r.is_null() { return 0; }
    let r = unsafe { &*r };
    if row >= r.rows.len() || col >= r.columns.len() { return 0; }
    match &r.rows[row][col] {
        CellRepr::Integer(n) => *n,
        _ => 0,
    }
}

/// TEXT として値を取得。NULL / INTEGER セルは NULL ポインタを返す。
/// 返ったポインタは result が free されるまで有効。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn enchudb_result_text(
    r: *const enchudb_result,
    row: usize,
    col: usize,
) -> *const c_char {
    if r.is_null() { return ptr::null(); }
    let r = unsafe { &*r };
    if row >= r.rows.len() || col >= r.columns.len() { return ptr::null(); }
    match &r.rows[row][col] {
        CellRepr::Text(s) => s.as_ptr(),
        _ => ptr::null(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn enchudb_result_free(r: *mut enchudb_result) -> i32 {
    if r.is_null() { return ENCHUDB_INVALID_ARG; }
    unsafe { drop(Box::from_raw(r)); }
    ENCHUDB_OK
}

// ────── version ──────

#[unsafe(no_mangle)]
pub unsafe extern "C" fn enchudb_version() -> *const c_char {
    static VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "\0");
    VERSION.as_ptr() as *const c_char
}

// ────── tests ──────

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    fn cstr(s: &str) -> CString { CString::new(s).unwrap() }

    fn fresh(name: &str) -> *mut enchudb_db {
        let path = format!("/tmp/enchudb_ffi_{name}.db");
        let _ = std::fs::remove_file(&path);
        let cpath = cstr(&path);
        let mut db: *mut enchudb_db = ptr::null_mut();
        let rc = unsafe { enchudb_open(cpath.as_ptr(), &mut db as *mut _) };
        assert_eq!(rc, ENCHUDB_OK);
        assert!(!db.is_null());
        db
    }

    #[test]
    fn open_and_close() {
        let db = fresh("open_close");
        let rc = unsafe { enchudb_close(db) };
        assert_eq!(rc, ENCHUDB_OK);
    }

    #[test]
    fn exec_create_insert() {
        let db = fresh("exec");
        let create = cstr("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        assert_eq!(unsafe { enchudb_exec(db, create.as_ptr()) }, ENCHUDB_OK);
        let ins = cstr("INSERT INTO t VALUES (1, 'alice')");
        assert_eq!(unsafe { enchudb_exec(db, ins.as_ptr()) }, ENCHUDB_OK);
        unsafe { enchudb_close(db); }
    }

    #[test]
    fn query_returns_rows() {
        let db = fresh("query");
        for s in &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO t VALUES (1, 'alice')",
            "INSERT INTO t VALUES (2, 'bob')",
        ] {
            let c = cstr(s);
            assert_eq!(unsafe { enchudb_exec(db, c.as_ptr()) }, ENCHUDB_OK);
        }

        let q = cstr("SELECT id, name FROM t WHERE id = 2");
        let mut r: *mut enchudb_result = ptr::null_mut();
        let rc = unsafe { enchudb_query(db, q.as_ptr(), &mut r as *mut _) };
        assert_eq!(rc, ENCHUDB_OK);
        assert!(!r.is_null());

        unsafe {
            assert_eq!(enchudb_result_rows(r), 1);
            assert_eq!(enchudb_result_cols(r), 2);

            let id = enchudb_result_int(r, 0, 0);
            assert_eq!(id, 2);

            let name_ptr = enchudb_result_text(r, 0, 1);
            assert!(!name_ptr.is_null());
            let name = CStr::from_ptr(name_ptr).to_str().unwrap();
            assert_eq!(name, "bob");

            let col0_name = CStr::from_ptr(enchudb_result_col_name(r, 0)).to_str().unwrap();
            assert_eq!(col0_name, "id");

            assert_eq!(enchudb_result_free(r), ENCHUDB_OK);
            assert_eq!(enchudb_close(db), ENCHUDB_OK);
        }
    }

    #[test]
    fn error_propagates() {
        let db = fresh("err");
        let bad = cstr("SELECT * FROM nope");
        let mut r: *mut enchudb_result = ptr::null_mut();
        let rc = unsafe { enchudb_query(db, bad.as_ptr(), &mut r as *mut _) };
        assert_eq!(rc, ENCHUDB_ERROR);
        assert!(r.is_null());
        unsafe {
            let msg_ptr = enchudb_last_error(db);
            assert!(!msg_ptr.is_null());
            let msg = CStr::from_ptr(msg_ptr).to_str().unwrap();
            assert!(msg.contains("nope"), "got: {msg}");
            enchudb_close(db);
        }
    }

    #[test]
    fn null_args_rejected() {
        unsafe {
            assert_eq!(enchudb_exec(ptr::null_mut(), ptr::null()), ENCHUDB_INVALID_ARG);
            assert_eq!(enchudb_close(ptr::null_mut()), ENCHUDB_INVALID_ARG);
            let mut db: *mut enchudb_db = ptr::null_mut();
            assert_eq!(enchudb_open(ptr::null(), &mut db as *mut _), ENCHUDB_INVALID_ARG);
        }
    }

    #[test]
    fn version_returns_valid_string() {
        unsafe {
            let v = enchudb_version();
            assert!(!v.is_null());
            let s = CStr::from_ptr(v).to_str().unwrap();
            assert!(!s.is_empty());
        }
    }

    #[test]
    fn matcha_use_case_via_ffi() {
        let db = fresh("matcha_ffi");
        for s in &[
            "CREATE TABLE notif_state (key TEXT PRIMARY KEY, dismissed_at INTEGER)",
            "INSERT INTO notif_state VALUES ('uuid-1', 1715174400)",
            "INSERT OR REPLACE INTO notif_state VALUES ('uuid-1', 1715180000)",
        ] {
            let c = cstr(s);
            assert_eq!(unsafe { enchudb_exec(db, c.as_ptr()) }, ENCHUDB_OK);
        }

        let q = cstr("SELECT key, dismissed_at FROM notif_state WHERE key = 'uuid-1'");
        let mut r: *mut enchudb_result = ptr::null_mut();
        unsafe {
            assert_eq!(enchudb_query(db, q.as_ptr(), &mut r as *mut _), ENCHUDB_OK);
            assert_eq!(enchudb_result_rows(r), 1);
            assert_eq!(enchudb_result_int(r, 0, 1), 1715180000);
            enchudb_result_free(r);
            enchudb_close(db);
        }
    }
}
