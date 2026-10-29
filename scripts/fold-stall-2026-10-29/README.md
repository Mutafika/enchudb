# #662 の測り: WAL の区画を畳む時に書き手が止まるか (2026-10-29)

書き手 1 本が upsert を回し続け、 1 件ごとの `commit()` の時間を取る (`main.rs`)。 表 t (id, a) 10 万行、 WAL 64 MiB、
changefeed の記録 64 MiB。

- 形 (1 つ目の引数): `none` (changefeed なし) / `log` (`enable_change_log`) / `loglisten` (記録 + 何もしない listener) /
  `listen` (何もしない listener だけ)
- 環境変数: `FOLD_DIR` (DB を置く所、 必須)、 `FOLD_RATE` (件/秒、 0 = 全力)、 `FOLD_BACKLOG_MIB` (listener の溜めの上限)、
  `FOLD_WAL_MIB` / `FOLD_LOG_MIB` / `FOLD_ROWS`
- 出力: 1 行 1 JSON。 `max_ms` (最大)、 `slow` / `slow_ms` (10 ms を越えた回と合計)、 `room_folds` (空け作りで畳んだ回)、
  `switches` (区画を移った回)、 `detached` (外された listener)

組み方: `Cargo.toml.foldstall` を別の directory に `Cargo.toml` として写し、 `path` を測る版の checkout に向ける (版ごとに
別の directory で組む。 同じ directory で path だけ変えると組み直されないことがある)。 `run_ab.sh` が 2 つの版を交互に回し、
`sum_ab.py results_ab.jsonl` が表にする。

`results_ab.jsonl`: 0.37.0 (base) と #662 の直し (fix)、 Apple M4 Max、 macOS 26.0.1、 負荷 6〜22 (別の仕事が走っていた)。
