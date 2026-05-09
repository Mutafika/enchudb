/**
 * EnchuDB C example.
 *
 * Build & run:
 *   cargo build --release -p enchudb-ffi
 *   cc -I crates/enchudb-ffi/include \
 *      -L target/release \
 *      -lenchudb_ffi \
 *      crates/enchudb-ffi/examples/demo.c -o /tmp/enchudb_demo
 *   DYLD_LIBRARY_PATH=target/release /tmp/enchudb_demo
 */

#include <stdio.h>
#include <stdlib.h>
#include "enchudb.h"

static void die(enchudb_db* db, const char* what) {
    fprintf(stderr, "%s: %s\n", what, enchudb_last_error(db));
    if (db) enchudb_close(db);
    exit(1);
}

int main(void) {
    printf("enchudb %s\n", enchudb_version());

    remove("/tmp/enchudb_demo.db");
    enchudb_db* db = NULL;
    if (enchudb_open("/tmp/enchudb_demo.db", &db) != ENCHUDB_OK) {
        fprintf(stderr, "open failed\n");
        return 1;
    }

    if (enchudb_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)") != ENCHUDB_OK)
        die(db, "create");
    if (enchudb_exec(db, "INSERT INTO t VALUES (1, 'alice')") != ENCHUDB_OK) die(db, "insert");
    if (enchudb_exec(db, "INSERT INTO t VALUES (2, 'bob')") != ENCHUDB_OK) die(db, "insert");
    if (enchudb_exec(db, "INSERT OR REPLACE INTO t VALUES (1, 'alice2')") != ENCHUDB_OK)
        die(db, "upsert");

    enchudb_result* r = NULL;
    if (enchudb_query(db, "SELECT id, name FROM t", &r) != ENCHUDB_OK) die(db, "query");

    size_t rows = enchudb_result_rows(r);
    size_t cols = enchudb_result_cols(r);
    printf("%zu rows, %zu cols:\n", rows, cols);

    for (size_t c = 0; c < cols; c++) {
        printf("  %s%s", enchudb_result_col_name(r, c), c + 1 < cols ? " | " : "\n");
    }
    for (size_t i = 0; i < rows; i++) {
        printf("  ");
        for (size_t c = 0; c < cols; c++) {
            if (enchudb_result_is_null(r, i, c)) {
                printf("NULL");
            } else {
                const char* txt = enchudb_result_text(r, i, c);
                if (txt) printf("%s", txt);
                else     printf("%lld", (long long)enchudb_result_int(r, i, c));
            }
            printf("%s", c + 1 < cols ? " | " : "\n");
        }
    }

    enchudb_result_free(r);
    enchudb_close(db);
    return 0;
}
