# enchudb-ffi

EnchuDB の **C ABI 凍結 layer**。 全言語から叩けるよう 12 関数を公開。

## なにこれ

- `cdylib` / `staticlib` / `rlib` ビルド対応
- SQLite 風の C 関数命名 + return code (0 = OK、 非 0 = error)
- opaque handle (`enchudb_db*` / `enchudb_result*`)
- ヘッダ + 動く C デモ同梱
- 内部実装は `enchudb-sql` の `Database` を呼ぶ → SQL の schema 永続化がそのまま乗る

## ビルド

```bash
cargo build --release -p enchudb-ffi
# → target/release/libenchudb_ffi.{dylib,so,a}
```

## 公開関数

```
// lifecycle
enchudb_open(path, **db) -> int
enchudb_create(path, **db) -> int
enchudb_close(*db) -> int

// SQL 実行
enchudb_exec(*db, sql) -> int                       // 結果不要 (DDL / DML)
enchudb_query(*db, sql, **result) -> int            // 結果あり (SELECT)
enchudb_last_error(*db) -> *const char

// 結果アクセス
enchudb_result_rows(*r) -> size_t
enchudb_result_cols(*r) -> size_t
enchudb_result_col_name(*r, col) -> *const char
enchudb_result_is_null(*r, row, col) -> int
enchudb_result_int(*r, row, col) -> int64_t
enchudb_result_text(*r, row, col) -> *const char
enchudb_result_free(*r) -> int

// misc
enchudb_version() -> *const char
```

ヘッダ: `include/enchudb.h`、 動く C デモ: `examples/demo.c`。

## 使い方 (C)

```c
#include "enchudb.h"

enchudb_db* db = NULL;
if (enchudb_open("/tmp/x.db", &db) != ENCHUDB_OK) { /* error */ }

enchudb_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
enchudb_exec(db, "INSERT OR REPLACE INTO t VALUES (1, 'alice')");

enchudb_result* r = NULL;
if (enchudb_query(db, "SELECT id, name FROM t", &r) == ENCHUDB_OK) {
    for (size_t i = 0; i < enchudb_result_rows(r); i++) {
        int64_t id = enchudb_result_int(r, i, 0);
        const char* name = enchudb_result_text(r, i, 1);
        printf("%lld | %s\n", (long long)id, name);
    }
    enchudb_result_free(r);
}
enchudb_close(db);
```

## ビルド + リンク (実例)

```bash
cargo build --release -p enchudb-ffi
cc -I crates/enchudb-ffi/include \
   -L target/release \
   -lenchudb_ffi \
   crates/enchudb-ffi/examples/demo.c -o /tmp/enchudb_demo
DYLD_LIBRARY_PATH=target/release /tmp/enchudb_demo
```

## なぜ「凍結 layer」 なのか

C ABI は一度公開すると wrapper が破壊的変更で全部壊れる。 内部実装が枯れるまで関数を増やさない方針:

- 現状 **12 関数** だけ、 prepared statement は `enchudb_query` 一括取得で代替
- 実ユースで足りないと判明したら追加
- `enchudb_db` / `enchudb_result` は opaque、 内部リファクタを許容
- 文字列は NUL 終端 UTF-8 (length-prefixed は使わない、 C エコシステム互換)

## なぜ SQLite 風 API なのか

SQLite が defacto になった本質的理由は技術じゃなく **C ABI で全言語から叩ける** こと。 全 wrapper / app が SQLite の関数命名を知ってる。 EnchuDB の C ABI もこれに倣えば既存 binding 文化を再利用しやすい。

## 言語 wrapper (今後)

`enchudb-ffi` の上に薄い層を被せて Python / Node / Swift から叩く。

- Python: `cffi` または `ctypes` で `libenchudb_ffi` をロード
- Node: `N-API` / `napi-rs` で薄く wrap
- Swift: import 直接
- Go: `cgo` で direct call

これらは別 repo / `bindings/` 配下に切り出す予定。

## meta crate での opt-in

```toml
enchudb = { path = "..", features = ["ffi"] }   # sql feature を自動引き込む
```
