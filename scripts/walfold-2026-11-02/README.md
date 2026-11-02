# #691: wal_full_fold の試験が背景の consumer と競って落ちる (2026-11-02)

`crates/enchudb-engine/tests/wal_full_fold.rs` の試験は WAL (8 KiB、 1 区画) と bridge を手で動かすが、 背景の consumer も同じ WAL を
書き出し・bridge・空け作りで触る。 1 回だけ見る判定が、 その途中の値を拾って落ちていた (master a38102e、 12 本並べて 144 回中 5 回、
24 本並べて 192 回中 3 回)。

| 判定 | 落ちる形 (確かめた形) | 直し |
|---|---|---|
| Phase 1 を抜ける `!wal_fold_safe()` (= ring 満杯のつもり) | consumer が畳んでいる最中は一瞬 false。 一時の probe: 落ちた回は 1 周目 (v = 30) で `_sync_ops` の空き 94 (満杯でない) のまま抜けた。 その時の head は周 2 の頭、 bridge の位置は巻き戻す前の `0x20`、 直後の `wal_fold_safe()` は true。 通った 67 回は v = 150 (149)、 空き 0 で抜けた | ring の満杯 (`table_eid_usage("_sync_ops").free == 0`) も見て抜ける |
| Phase 2 の 「満杯のはずの WAL に Commit が入った」 | 上の続き: ring に空きがあるので、 満杯の WAL に積む Commit を空け作りが bridge して畳み (`wal_room_folds` 0 → 1、 周 2 → 3)、 Commit が新しい周に入った | Phase 1 の直しで消える |
| Phase 3 の 「孤児 group が fold を恒久ブロック」 | consumer がその場で畳む途中: `wal_fold_safe` false (畳んだ周の head と巻き戻す前の bridge の位置) かつ `append_dead` true、 50 ms 後には safe / 満杯でない。 見直しの loop の後にもう一度見ると、 また途中を読む | 最大 2 秒の間に一度でも通るか (`eventually`)、 その結果で判定 |
| `uncommitted_tail_with_room_blocks_fold` の 「commit 後も fold 可能にならない」 | 1 回だけ見ると false (どの途中かは見ていない) | `eventually` |
| 「fold 後の新規 write が bridge に乗らない」 (試験の transfer が 0) | consumer が先に bridge した (後の `_sync_ops` に現れるかは通る形) | どちらが運んでも `_sync_ops` に現れるかを `eventually` で |

## 確かめ

- `results_ab.txt`: master と直しの test binary を交互に。 24 本並べて 8 回ずつ: master 192 回中 3 回、 直し 0 回。 12 本並べて 12 回ずつ: master 144 回中 5 回
  (3 種類)、 直し 0 回
- `mut_1.log` (`mutations.json`): 見直す形にしても本来の誤りを落とす。 「満杯の WAL の孤児 group を畳んでよいとしない (brick に戻す)」 は Phase 3 の
  「恒久ブロック」 で、 「wal_fold_safe が常に false」 は 「commit 後も fold 可能にならない」 で落ちる (どの判定で落ちたかは run ごとの出力で見た)

engine の誤りではない: 畳む途中を 「畳めない」 と答えるのは保守的な側で、 ring に空きがある時に空け作りが畳むのは作りどおり。
