# #683 の測り: 書き手が速い時、 記録のある DB が区画ごとに止まる (2026-11-01)

測りの program は `scripts/fold-stall-2026-10-29/` の `foldstall` に、 2 ms を越えた `commit()` を段ごとの時刻に足す 3 行を
加えた `foldstall_probe.rs` (書き手 1 本の upsert + `commit()`、 表 10 万行、 WAL 64 MiB、 記録 64 MiB、 全力 4 秒)。

## 段ごとの時刻

`probe.patch` (master 6374044 に当てる、 commit しない) は engine / oplog の段 (consumer の写しの読み・符号化・記録に足す・
crc・書き出し、 畳む時に lock を持つ時間、 書き手の待ちなど) の区間を控え、 `ENCHU_T683=<file>` の時だけ engine の Drop で
書き出す。 1 行 = `開始 µs  thread (c = consumer / w = 書き手)  段  区間 µs  数`。 `stages.py <file>` が段ごとの合計、
`busy.py <file>...` が consumer の段の合計と 1 件あたりの ns。

組み方は `scripts/fold-stall-2026-10-29/README.md` と同じ (版ごとに別の directory、 `Cargo.toml` の `path` を測る版の checkout に。
`enchudb-oplog` も同じ checkout の path で足す)。 比べる版にはどちらも `probe.patch` を当てる (置き場所の差をそろえる)。

## 分かったこと (Apple M4 Max、 書き手が速い状態 = changefeed なしで 4 秒 1,900 万件ほど)

1. 記録のある DB の WAL は、 畳むたびに append_lock を持ったまま WAL 全体を fsync していた (`set_sync_on_reset`)。 2 区画の形では
   書き手がもう一方の区画に積んだ分も書くので、 1 回 4〜5 ms 書き手を止めた (畳む時に lock を持つ時間 p50 4,050 µs)。
   区画の header の page だけ書き出すと 26〜46 µs
2. consumer が WAL の record を記録に写す仕事が 1 件約 245 ns (WAL を読む 90〜98、 印と符号化 54、 記録に足す 96〜100)。
   書き手は記録ありで 1 秒 350 万件ほど積むので、 写しが追いつかず空け作りに落ち、 書き手が 30〜60 ms 待った
   (下の `f1` の 1 run で、 4 秒に 25 回・計 411 ms)。 印 (wal_id = 署名の範囲の FNV) を並べると印と符号化が 37 ns になり、 写しが追いつく。
   ただし consumer は 4 秒のうち約 3.5 秒ふさがっていて、 余裕は薄い

## 結果

- `results_steps.jsonl`: 記録あり (`log`)、 6 seed、 版 3 つを seed ごとに順を回して (負荷 3.7〜5.3)
  - `base` = master + probe、 `f1` = + 畳んだ印を header の page だけ、 `fix` = + 印を並べる

  | 版 | 書けた数の中央値 (万) | 最大 (ms) | 10 ms 超えの計 (6 seed) | 空け作り |
  |---|---|---|---|---|
  | base | 1375 | 37.3〜56.8 | 2051 ms | 21〜26 |
  | f1 | 1398 | 47.6〜72.3 | 2425 ms | 23〜27 |
  | fix | 1521 | 3.0〜17.7 (4 seed は 3.2 以下) | 31 ms | 0〜4 |

- `results_ab.jsonl`: `base` と `fix`、 形 `none` / `log`、 6 seed 交互 (負荷 1.9〜4.2)。 `log` は書けた数の比の中央値 1.104、
  最大 37.1〜48.1 → 2.9〜31.3 ms (4 seed は 3.0 以下)、 10 ms 超えの計 1870 → 90 ms、 空け作り 21〜26 → 0〜9。 `none` は比 0.992
  (揺れの範囲、 直しは記録のある DB の道だけ)

`sum_ab.py results_ab.jsonl` が表にする。
