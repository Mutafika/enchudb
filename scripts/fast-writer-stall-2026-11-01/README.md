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

## 続き: 記録への写しを WAL の mmap から直接 (#683 の 2 つ目の PR)

上の直しの後も consumer は写しに 4 秒のうち約 3.5 秒を使っていた。 写しの途中の物 (record ごとの `Record` と署名の範囲の
Vec、 `WireRecord`、 entry の bytes の Vec) をやめて、 WAL の mmap を借りた `RecordRef` から記録の置き場所へ直接
`WireRecord` の bytes を書く。 符号化・印 (wal_id)・crc は置く thread で 1 回にまとめ、 4096 件以上の回は rayon で並べる。
形式 (oplog v4・changefeed.seg・wire) は変えない。

途中で `sample` を取ると (1 回目の版、 consumer の起きている間の約 1,080 sample)、 WAL を読む段の半分以上が
`scan_for_change_log` の 「書き出した所で切る」 `filter().collect()` (record 1 件 約 140 B を全部動かし直す、 180 sample) と、
group (1〜2 件) ごとに閉じた record を別の Vec から移す memmove (41 sample) だった。 record は位置の順なので先頭から
`truncate` し、 scan は 1 本の Vec に積んで最後に閉じていない末尾を切り離す。 entry の長さは 1 回だけ計算して使い回す。

### 段ごとの時刻 (`probe_direct.patch` = この branch に計りを当てたもの (`ENCHU_T683=<file>`、 読み方は上と同じ `busy.py`)、 記録あり、 4 seed ずつ交互、 負荷 7.0〜7.9。 master は WAL の fsync が 1 回 2 秒かかった seed 4 を除く 3 seed)

| 段 (1 件あたり ns) | master | この branch |
|---|---|---|
| WAL を読む | 77.8〜81.2 | 18.1〜18.4 |
| 印と符号化 | 38.4〜38.7 | (記録に置く段の中) |
| 記録に置く (計) | 99.5〜100.4 | 93.9〜96.2 |
| うち crc / 写し (master は別々、 この branch は符号化と合わせて並べた 1 回) | 20.5 + 19.4〜20.0 | 31.4〜33.6 |
| うち書き出し (msync) | 36.1〜37.2 | 35.9〜37.0 |
| 写し 1 回の計 (`pump_tick`) | 225.1〜229.8 | 115.9〜118.3 |

書き出しの 36 ns は 1 件 208 B を disk に書く分で、 形式を変えない限り減らない。 残りの大きいものは置き場所の計算と古い
entry を捨てる所 (約 26 ns、 捨てる entry の header を読む) と WAL の fsync (約 23 ns)。

### 計りなしの A/B (`run_ab_direct.sh`、 master 3a6a888 と この branch、 seed ごとに順を入れ替え、 全力 4 秒)

`results_direct_ab.jsonl` (`round` 1 = 1 回目の版 = WAL を読む段の 2 つ (truncate / 1 本の Vec) と長さの使い回しを入れる前、 2 = この branch の最後の版)。 `sum_direct.py results_direct_ab.jsonl` が下の数を出す。

| 回 | 形 | seed | 負荷 | 書けた数の比の中央値 | 最大 (ms) master → この branch | 10 ms 超えの計 |
|---|---|---|---|---|---|---|
| 1 | log | 6 | 5.3〜6.6 | 1.038 | 3.0〜4.1 → 2.9〜58.6 | 0 → 125 ms (2 seed) |
| 1 | none | 6 | 5.2〜6.7 | 1.019 | 3.0〜37.7 → 2.9〜7.2 | 55 → 0 ms |
| 2 | log | 8 | 4.8〜7.2 | 1.058 | 3.0〜5.8 → 2.9〜3.1 | 0 → 0 |
| 2 | none | 8 | 4.8〜7.3 | 1.017 | 2.9〜51.0 → 2.8〜3.9 | 50 → 0 ms |

- 回 1 の この branch の 2 つの止まり (25.6 / 58.6 ms、 空け作り 0 回) は、 WAL を読む段を直す前の版。 記録の無い形 (none) の
  master にも同じ大きさの止まりが出ている (回 1 の 37.7 ms、 回 2 の 51.0 ms)。 最後の版は計りなし 8 + 計りあり 14 run (`results_direct_probe.jsonl`、
  `round` 2、 負荷 5.5〜9.2) で 10 ms を越えた止まりが 0 回。 計りありの master の 1 run (seed 4) では WAL の fsync が 1 回 2 秒かかり、
  書き手が 1.9 秒止まった (disk の揺れ)
- この日は 「速い状態」 (記録なしで 4 秒 1,900 万件) まで負荷が下がらず、 master (#686 の後) でも記録ありの止まりは再現しな
  かった。 書き手が速い状態の記録ありは 1 秒 350 万件 (1 件 286 ns) 積むので、 consumer が写しに使う時間の割合は 1 件 226 ns の
  master で約 79%、 117 ns のこの branch で約 41% の見積もり (WAL の fsync などは除く)
