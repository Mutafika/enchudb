# 2026-10-20 の総点検 (v0.33.0〜v0.35.0) の再現 harness

issue #544〜#563 を見つけた時に使った再現 crate。 **test ではない** (答えを決め打ちせず、 観測を print
するだけ)。 test にするなら、 前提を手渡さない形 (直しを外すと落ちる形) に書き直すこと。

どの crate も root の workspace の外 (`[workspace]` を空で持つ)。 engine は path で `../../..` を見る。
DB は `std::env::temp_dir()` の下に作り、 終わったら消す。

```sh
cd scripts/review-2026-10-20/<crate>
CARGO_TARGET_DIR=../../../target cargo run -q [--release] [-- <args>]
```

| crate | issue | 何をするか | 2026-10-20 の master (46352b7) での観測 |
|---|---|---|---|
| `pk_tomb_repro` | #545 (S1) / #546 (S3) / #547 (S4) / #544 (S5, S6) | 3 peer を `InMemoryTransport` でつなぎ、 pull の順を変えて行の集合を比べる | S1 DIVERGED (slot の使い回しの後の Delete)。 S3 は `set_emit_alias(true)` で DIVERGED、 false なら same。 S4 は zombie 行に同じ主キーを upsert すると `entity_count` 1 → 2。 S5 は主キーを後に `set` すると受け手に同じ主キーの行が 2 つ (先に set すると 1 つ)。 S6 は #513 の形で B の z (A の削除より古い) が全 peer に残る (C と D は同じ答えなので DIVERGED とは出ない。 消えるべきものが消えていない) |
| `ravnprefix` | #556 | peer_id = 7 の DB で `follow` / `reverse_follow` と輪の `bfs` | `follow` が peer の頭なし、 `bfs` が 4 段 (期待 2 段) |
| `schemasem` | #548 / #559 | 主キー無しの row 1 つを混ぜ、 count / min / order_by を経路ごとに比べる | `where_range(age).count() = 11` vs `all().count() = 10`、 `Table::min = 1` vs `all().min = 7`、 Run 経路の order_by が `[1..9, 0]` |
| `walexp` | #553 (`over`) / #552 (`many` / `many-sync`) / #554 #560 (`stalled`) | WAL の空け作りと changefeed | `over`: 100 KiB を 64 KiB の WAL に書くと 5.0 s 止まり、 consumer を約 19.5k 回起こし、 dropped = 1。 `many 32 200000 65536` (release) で 1,651 件落ち、 1 回の書き込みの最大は 1〜5 ms。 `stalled`: 2 s 止まる listener で `oplog_sync` 3.8 s、 Drop 2.0 s |

`walexp many` / `many-sync` は `--release` で測ること (debug は timing が違う)。 引数は
`many [writers] [writes] [wal_bytes]` (既定 8 / 200000 / 262144)。
