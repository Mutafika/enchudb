# v29 PLAN — 保守強化 / 完全な耐久性

## 目的

v28 の残債を埋めて、SaaS primary として本当に使える状態にする。opyula で既に v27(= v28 のサブセット)動いてる前提で、裏で enchudb を育てる。

## 前提

- opyula は既に v27 運用中(path 依存なので path push で v28 が自動で乗る)
- 外に出していないので破壊的変更 OK
- 対象: v28 で `#[ignore]` にした 3 件 + ハードニング一般

## タスク一覧

### 1. File size 検証(0.5 日) — 即効、SIGBUS 解消
open() で file.metadata().len() を layout.total_size と比較。不一致なら明示エラー。
v28 の truncate_db_to_half_fails_to_open が PASS するようになる。

### 2. Page checksum(1-2 日) — 本命
region 単位 CRC。別ファイル `*.db.crc` に保存。

```
[magic "ECRC"][version u32][file_size u64][n u32]
[region_id u16 | crc u32] × n
```

対象 region: 各 himo column(used portion)、vocab、himoreg、content。
- 書込時は CRC 無関係(現状維持、速度劣化ゼロ)
- flush() 時に CRC 再計算 + .crc ファイル更新
- open() 時に全 region CRC 検証 → 不一致で error(region ID 付き)
- .crc ファイル欠損は許容(後方互換)

v29_gap_body_bit_flip_should_be_detected が PASS する。

### 3. fsync barrier(0.5-1 日) — HW 順序保証
macOS: F_FULLFSYNC を使って WAL → body の順序を OS/HW 越しに強制。
Linux: sync_file_range + fdatasync。

`Durability::Strict` モード追加(既存 Async/Sync の上位)。

v29_gap_out_of_order_sync の設計検討完了。

### 4. ストレステスト増強(1 日)
- 1 千万件書き込み + kill + recover
- 24 時間ランダムファズ(slow test、`#[ignore]` で run 時だけ)
- 10 並列 writer + 周期 kill

### 5. メモリ監視 API(0.5 日)
```rust
struct Stats {
    wal_head: u64,
    wal_checkpoint: u64,
    wal_lag_bytes: u64,
    pair_table_mem: usize,
    consumer_queue_len: usize,
    durable_lsn: u64,
}
pub fn stats(&self) -> Stats
```

### 6. ドキュメント(0.5 日)
- CLAUDE.md v28/v29 API 反映
- README に WAL/リカバリ使い方
- CHANGELOG

## 完了基準

- `#[ignore]` だった 3 件が全て通る
- 既存 96 lib + 8 WAL + 10 destruction = 114 テスト全緑
- stress test で 1M 回 kill-recover サイクル完走
- opyula(path 依存で自動取り込み)で退行なし

## 順序

1 → 2 → 3 → 4 → 5 → 6。1 と 5 は軽いのでスキマ時間に。
