# #268: sync の bridge の soak (2026-11-03)

#268 (実機で bridge が 41 時間 / 71 時間 0 を返し続け、 配布が黙って止まった) を、 今の版で長く回して確かめた記録。
報告元のアプリの環境は手元に無いので、 その形をまねて時間を縮めた soak を作った。

program は `crates/enchudb-sync/examples/bridge_soak.rs` (CI の `cargo test -p enchudb-sync` が組むので、 API が変わっても気づく)。

## 何を回すか

- peer A: 書き手 1 本。 2 万 file の表 (fid / hash / size = Number、 path = Tag) を塊で書き換え、 塊の間で休む (時々 0.5〜3 秒)。
  既定で署名する。 WAL は既定 1 MiB (区画 512 KiB) にして、 畳む回数を実機より何桁も増やす
- A の publish thread: 50 ms ごとに `transfer_oplog_to_sync_ops` + `publish_since(ZERO)`。 engine の consumer も bridge するので 2 本が競る
- peer B: 100 ms ごとに pull。 歴史を切られたら bootstrap
- 見張り: commit が進むのに A の `current_sync_lsn` が 5 秒動かない = 止まり。 `wal_dropped_records` の増え (dropped) も 1 行 1 JSON で残す
- 終わり: 書き手を止めて A を書き出し、 B が全 file の最後の hash を持つまで最大 60 秒待って突き合わせる
- exit 1 = 止まり・落ち・B の取りこぼし・A の読み違い・B が歴史を切られた回、 のどれかが 1 つでもある

```sh
SOAK_SECS=1800 SOAK_SEED=11 cargo run --release -p enchudb-sync --example bridge_soak > s11.log 2> s11.err
SOAK_SECS=1800 SOAK_SEED=13 SOAK_WAL_KIB=256 cargo run --release -p enchudb-sync --example bridge_soak > s13.log 2> s13.err
SOAK_SECS=1800 SOAK_SEED=14 SOAK_RATE=100000 cargo run --release -p enchudb-sync --example bridge_soak > s14.log 2> s14.err
```

## 結果 (`results_30min.jsonl`)

master 0202812 (0.37.3 の後、 WAL は 2 区画の v4)、 Apple M4 Max、 4 本を同時に 30 分。 A は署名あり。

| seed | WAL | 書く速さの上限 | 書いた数 | 周 | 区画の移り | 止まり | 落ち | B の取りこぼし | truncation |
|---|---|---|---|---|---|---|---|---|---|
| 11 | 1 MiB | 2 万/秒 | 5,109,034 | 5,543 | 1,946 | 0 | 0 | 0 | 0 |
| 12 | 1 MiB | 2 万/秒 | 5,181,292 | 5,595 | 1,987 | 0 | 0 | 0 | 0 |
| 13 | 256 KiB | 2 万/秒 | 5,027,180 | 13,915 | 9,915 | 0 | 0 | 0 | 0 |
| 14 | 1 MiB | 10 万/秒 | 5,061,456 | 5,543 | 1,895 | 0 | 0 | 0 | 0 |

`wal_commit_failures` も全部 0、 最後の scan はどれも `reached-head`。 書き手を止めてから B が揃うまで 3.9〜4.6 秒。

標準エラー (`results_30min_stderr.txt`) には 4 本とも #268 の警告が 1 行ずつ出た。 本物の止まりではない: head と cursor の差は
120 B (= 閉じる時の Commit 1 件、 header 112 B + 周の印 8 B) で、 書かない peer B を閉じる時の Commit に、 30 分前からの暇な時間を
「止まっていた時間」 として鳴っていた。 これが #694 で、 #695 で直した。 同じ soak 280 秒は直す前 1 行、 直した後 0 行。

## この soak が何を捕まえるか (`mutations.json`、 `results_mut.txt`)

```sh
python3 scripts/mutate.py scripts/bridge-soak-2026-11-03/mutations.json -- scripts/bridge-soak-2026-11-03/run_mut.sh
```

`run_mut.sh` は変異の入った tree で組み直して 60 秒回す。 終わった後の target は変異入りの build のままなので、 組み直してから使うこと。

- **M1: bridge の cursor を 1 回だけ record の境目でない所 (+8 byte) にずらす** (#268 の 3 回目の症状)。 回ごとに現れ方が違った
  - 1 回目: bridge が止まって WAL が数秒で埋まり、 5,749 件落ちる → floor → B が truncation 1 回 → bootstrap 1 回で揃う。 止まりは 5 秒に
    届かない。 この時の soak は落ちと truncation を数えていなかったので生き残った。 それで両方を失敗に数えるようにした
  - 2 回目 (数えるようにした後): 落ちは 0、 60 秒待っても B に 92 file が届かず killed (終わりの突き合わせで落ちた)
  - 3 回目 (この directory の `run_mut.sh`、 master ef3ba91): 落ちは 0、 B に 186 file が届かず killed
- **M2: 今の区画を書き終えた所 (`written`) でなく場所を取った所 (`head`) まで読む** (#268 の 3 回目の直し、 #332 を外す)。 soak は 3 回とも
  通った (最後の 1 回は A が署名する形)。 この原因は `crates/enchudb-oplog/tests/issue268_stale_record.rs` が落とす
  (bridge と同じ形の scan を毎周畳みながら回す試験)。 soak は症状の側 (止まる・落ちる・届かない) を、 単体の試験は原因の側を見る

## 分からないこと

実機の 41 時間 / 71 時間の環境 (報告元のアプリ、 その版) はそのまま作れていない。 実機でまた出たら、 警告の行と
`bridge_last_scan_stop()` / `bridge_pending_records()` / `bridge_last_from()` / `bridge_last_committed_end()` /
`wal_commit_failures()` / `wal_dropped_records()` を見れば、 どの分岐かが決まる。
