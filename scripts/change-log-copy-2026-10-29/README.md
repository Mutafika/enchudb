# #667 の測り: changefeed の記録のある DB で、 全力の書き手が WAL の区画を埋めるたびに止まるか (2026-10-29)

測りの program は `scripts/fold-stall-2026-10-29/` の `foldstall` (書き手 1 本が upsert を回し続け、 1 件ごとの `commit()` の時間を取る)。
組み方もそこの README の通り (版ごとに別の directory で組む)。 script の中の path は測った時の scratchpad の場所なので、 回す時は書き換える。

- `run_steps.sh` / `results_steps.jsonl`: 0.37.1 (`base`) と直しの段を seed ごとに順を回して (記録 64 MiB、 WAL 64 / 256 MiB、 全力、 4 秒 × 3 seed)
  - `A` = 記録が古い entry を捨てる時に中身の crc を取らない
  - `B` = A + 新しい entry の crc を大きな回は rayon で並べて取る
  - `DmC` = B + 書き手が区画を移ったら consumer を起こして待たずに書き出す
  - `D` = DmC + 記録がある時は tick ごとに書き出した所まで写す (この PR)
- `run_modes.sh` / `results_modes.jsonl`: 0.37.1 と `D` を、 形 4 つ (changefeed なし / 記録 / 記録 + 何もしない listener / listener だけ) × 速さ 2 つ
  (全力 / 50 万件/秒) で seed ごとに交互に (WAL 64 MiB、 #664 の表と同じ形)
- `run_rate_p99.sh` / `results_rate_p99.jsonl`: 50 万件/秒・記録ありの p99 を 0.37.1 / B / DmC / D で (4 seed、 順を回して)
- `mutations.json` / `mutate_tests.sh` / `mutate.log`: 変異 (`scripts/mutate.py`)

段ごとの時間 (写しの読み / 符号化 / 記録に足す、 足すの中の計画 / 書く / 書き出し) は、 engine に一時の eprintln を入れて取った (commit していない)。
