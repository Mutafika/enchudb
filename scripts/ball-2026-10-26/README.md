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
  交互に回した。 読みの段は `run_r.sh` (基準は script の頭)。 変異は `ball_mut_v6.json`
- `ball_mut.json`: 変異 24 本 (4 回目で段の道の 7 本を足し、 意味の無くなった 「球の列も詰め直す」 を外した)。 その前は 18 本 (3 回目でカードの時刻の道の 7 本を足した。 全部どちらかで落ちる)。 前は 11 本 (`scripts/mutate.py`、 engine の `ball::` と schema の `ball_history` の 2 通りで回して全部どちらかで落ちる)

負荷は全部の回で 8 未満 (6.8〜8.0)、 空き 109 GiB。
