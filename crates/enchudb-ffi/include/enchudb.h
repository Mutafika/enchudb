/**
 * EnchuDB C ABI — frozen layer for cross-language access.
 *
 * Build:
 *   cargo build --release -p enchudb-ffi
 *   → target/release/libenchudb_ffi.{dylib,so,a}
 *
 * Link:
 *   cc your_app.c -lenchudb_ffi -o your_app
 *
 * Return codes: 0 = OK, non-zero = error. See ENCHUDB_* constants below.
 */

#ifndef ENCHUDB_H
#define ENCHUDB_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* return codes */
#define ENCHUDB_OK            0
#define ENCHUDB_ERROR         1
#define ENCHUDB_INVALID_ARG   2
#define ENCHUDB_INVALID_UTF8  3
#define ENCHUDB_NO_RESULT     4

/* opaque handles */
typedef struct enchudb_db enchudb_db;
typedef struct enchudb_result enchudb_result;

/* DB lifecycle */
int32_t enchudb_open(const char* path, enchudb_db** out_db);
int32_t enchudb_create(const char* path, enchudb_db** out_db);
int32_t enchudb_close(enchudb_db* db);

/* exec / query */
int32_t enchudb_exec(enchudb_db* db, const char* sql);
int32_t enchudb_query(enchudb_db* db, const char* sql, enchudb_result** out_result);
const char* enchudb_last_error(enchudb_db* db);

/* result accessors */
size_t      enchudb_result_rows(const enchudb_result* r);
size_t      enchudb_result_cols(const enchudb_result* r);
const char* enchudb_result_col_name(const enchudb_result* r, size_t col);
int32_t     enchudb_result_is_null(const enchudb_result* r, size_t row, size_t col);
int64_t     enchudb_result_int(const enchudb_result* r, size_t row, size_t col);
const char* enchudb_result_text(const enchudb_result* r, size_t row, size_t col);
int32_t     enchudb_result_free(enchudb_result* r);

/* misc */
const char* enchudb_version(void);

#ifdef __cplusplus
}
#endif

#endif /* ENCHUDB_H */
