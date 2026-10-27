# 球 (値の履歴) の試作の測り — 2026-10-26

branch `proto/ball-history` の `crates/enchudb-engine/src/ball.rs` の書き込みの値段と、時点を指した読みの速さ。

- `ballb.rs`: 測りの本体 (`w` = 書き手の段、 `r` = 読みの段)。 crate は `Cargo.toml.base` (master cb827e4) / `Cargo.toml.proto`
  (この branch、 feature `ball`) で別々に組む。 `lclk.patch` は時刻だけを書き手ごとの数え口にした測り専用の版 (時刻の順が壊れる、
  commit しない)
- `run_ballb.sh` / `analyze_ballb.py`: 書き手の段 (96 万行、 cycle 3、 書き手 1 / 4 本 × seed 41〜46)。 基準は script の頭に回す前に書いた
- `run_cols.sh`: 球にする列を分けた版 (a・b だけ / w だけ、 seed 41〜43)
- 結果: `results_ballb_v1.jsonl` (直す前: 面の場所を 1 つの数え口で取る、 u32 の値は全部配列) / `results_ballb_v2.jsonl` (直した後) /
  `results_ballb_cols.jsonl` / `results_ballb_r.jsonl` (読み、 seed 41〜43)
- 3 回目 (履歴をデッキのカードのスピン = 向きの時刻に残す): `results_ballb_v3.jsonl` / `results_ballb_cols_v3.jsonl` /
  `results_ballb_r_v3.jsonl`。 `ballb.rs` は 3 回目の形 (宣言を索引を組む前に、 読みの段は円柱を先に組まない、 時刻を書けなかった
  カードの数を出す)
- 4 回目 (詰め直しで外れたカードを履歴の段へ移す): `results_ballb_v4.jsonl` / `results_ballb_cols_v4.jsonl` / `results_ballb_r_v4.jsonl`
  (`ballb.rs` は詰め直しの回数も出す)。 段の道の変異 7 本は `ball_mut_past.json` (engine の `spun_bucket` 単体の試験で全部落ちる)
- 5 回目 (名簿は master のまま、 履歴は球の面とデッキの差分の帳面): `results_ballb_v5.jsonl` / `results_ballb_cols_v5.jsonl` /
  `results_ballb_r_v5.jsonl`。 どれも 4 回目の binary (`v4_on` / `v4ab` / `v4w` / `"v":"v4"`) と交互に回した。 変異は `ball_mut_v5.json`
  (17 本、 全部どちらかで落ちる。 時刻を帳面の鍵の外で取る変異は回の途中の時刻の突き合わせで 20/20)。 `ballb.rs` は 5 回目の形
- 6 回目 (帳面を名簿の鍵の中で書く、 時点 t を bit の表で戻す、 大きい値は列 1 本に糸): `results_ballb_v6.jsonl` /
  `results_ballb_cols_v6.jsonl` / `results_ballb_r_v6.jsonl`。 5 回目の binary (`v5_on` / `v5ab` / `v5w` / `run_r.sh` の `v5`) と
  交互に回した。 読みの段は `run_r.sh` (基準は script の頭)。 変異は `ball_mut_v6.json` (33 本、 全部どちらかで落ちる)、 並行の
  変異の検出率は `rate6.py`。 `*_v6_inlock.jsonl` は大きい値の帳面を write_lock の中で書いていた最初の版 (書き手 4 本で値の種類が
  多い列の書き換えが +947 ns、 場所を鍵の前に取る形に直した)。 `results_ballb_r_v6.jsonl` は重なり落としを外した後の版
- 7 回目 (帳面を時刻 / eid / 入った bit の列 3 本に分ける + 時点 t の bit の表を枝なしで当てて読む): `results_ballb_v7.jsonl` /
  `results_ballb_cols_v7.jsonl` / `results_ballb_r_v7.jsonl` (読みは 6 回目 / `v7a` = 細くしただけ / 枝なしの 3 つを交互)。
  `rbprof.rs` は bit の表の戻しを段ごとに測った単独の program (ballb r と同じ形のデータ、 値 100 個を回す / 1 個だけ、 引数 `all` / `hot`):
  時間の大半が枝の読み違いだと分かった測り。 変異は `ball_mut_v7.json` (47 本、 ball.rs の 37 本は単体で全部落ちる)。 `ballb.rs` に
  `BALL_ROUNDS` (履歴の長さ、 既定 10) と 1 回目の時点の読み (`pull_at_t1`) を足した
- 8 回目 (時刻ごとのデッキの控え): `results_ballb_r_v8.jsonl` (履歴 10 回) / `results_ballb_r50_v8.jsonl` (`BALL_ROUNDS=50`) /
  `results_ballb_cols_v8.jsonl` / `results_ballb_v8.jsonl`。 どれも 7 回目の binary (`v7b` / `v7bab` / `v7bw` / `v7b_on`) と交互。
  読みの要約は `sum_r.py <file> v7b,cur`、 書き込みは `sum_w.py <cols> <3 列> v7b`。 変異は `ball_mut_v8.json` (67 本、 ball.rs の
  55 本は単体、 engine の 12 本は schema の試験で全部落ちる)
- 9 回目 (球の時刻を WAL の HLC に): `results_ballb_v9.jsonl` (WAL なし) / `results_ballb_wal_v9.jsonl` (`BALL_WAL=1`)。 どれも
  8 回目を同じ bench で組み直した `v8w_on` と交互。 `*_v9a.jsonl` は HLC を thread-local で渡していた最初の版 (宣言の無い列の書き直しが
  +2 ns 重く線を越えた)。 base (cb827e4) は scratchpad に worktree で作り直して組んだ。 変異は `ball_mut_v9.json` (84 本、 全部落ちる)、
  並行の変異の検出率は `rate9.py` (`rate9.log` = 直した後、 `rate9a.log` = 最初の版)
- 10 回目 (球の面に skew binary の飛び糸、 時点 t の値を O(log n) 歩): `results_ballb_h_v10.jsonl` (熱い行、 `ballb h` = `MODE=h run_r.sh`、
  1000 行 × 書き換え 2000 回) / `results_ballb_r50_v10.jsonl` (`BALL_ROUNDS=50`) / `results_ballb_v10.jsonl` (書き込み、 base / `v9h_on` / `v10_on`)。
  比べる 9 回目は `h` の段を足した同じ bench で組み直した `v9h` (削除の HLC を直した後の db53420)。 要約は `sum_r.py <file> v9h,v10`
  (段の名前は file から取る)。 変異は `ball_mut_v10.json` (93 本、 ball.rs の 75 本は単体、 他の 18 本は schema の試験で全部落ちる。
  幅の計算の 2 本は並行の試験では書き手の panic で Barrier が止まり時間切れ、 飛び糸の試験だけなら 2〜3 秒で落ちる)。
  宣言の無い列の書き込みは `results_ballb_off_v10.jsonl` (`run_ballb.sh` の `v9h_off` / `v10_off` = BALL=0、 base と交互)。
  `miri_dir_probe.rs` は Miri (Tree Borrows) で OnceLock 65536 個の並びを `<[_]>::get` / 添字 / `len` / raw の `len` で引く値段
  (0.33 秒 / 4〜20 ms / 0.1 秒 / 0.1 ms)、 `miri_dir_asm.rs` は直した `Chunked::get` と前の形の release の asm を比べる
  (どちらも依存なしの crate の `src/lib.rs`)
- `ball_mut.json`: 変異 24 本 (4 回目で段の道の 7 本を足し、 意味の無くなった 「球の列も詰め直す」 を外した)。 その前は 18 本 (3 回目でカードの時刻の道の 7 本を足した。 全部どちらかで落ちる)。 前は 11 本 (`scripts/mutate.py`、 engine の `ball::` と schema の `ball_history` の 2 通りで回して全部どちらかで落ちる)

負荷は全部の回で 8 未満 (6.8〜8.0)、 空き 109 GiB。
