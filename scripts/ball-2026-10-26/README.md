# 球 (値の履歴) の試作の測り — 2026-10-26

branch `proto/ball-history` の `crates/enchudb-engine/src/ball.rs` の書き込みの値段と、時点を指した読みの速さ。

- `ballb.rs`: 測りの本体 (`w` = 書き手の段、 `r` = 読みの段)。 crate は `Cargo.toml.base` (master cb827e4) / `Cargo.toml.proto`
  (この branch、 feature `ball`) で別々に組む。 `lclk.patch` は時刻だけを書き手ごとの数え口にした測り専用の版 (時刻の順が壊れる、
  commit しない)
- `run_ballb.sh` / `analyze_ballb.py`: 書き手の段 (96 万行、 cycle 3、 書き手 1 / 4 本 × seed 41〜46)。 基準は script の頭に回す前に書いた
- `run_cols.sh`: 球にする列を分けた版 (a・b だけ / w だけ、 seed 41〜43)
- 結果: `results_ballb_v1.jsonl` (直す前: 面の場所を 1 つの数え口で取る、 u32 の値は全部配列) / `results_ballb_v2.jsonl` (直した後) /
  `results_ballb_cols.jsonl` / `results_ballb_r.jsonl` (読み、 seed 41〜43)
- `ball_mut.json`: 変異 11 本 (`scripts/mutate.py`、 engine の `ball::` と schema の `ball_history` の 2 通りで回して全部どちらかで落ちる)

負荷は全部の回で 8 未満 (6.8〜8.0)、 空き 109 GiB。
