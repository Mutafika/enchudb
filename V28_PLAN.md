# v28 PLAN — WAL + Crash Consistency

## 目的

SaaS primary として使える耐久性を獲得する。ただし **v27 の読み書き速度を絶対に落とさない**。

## 大原則

1. **読みは WAL に一切触らない**（pull_raw 10ns, query 70ns を維持）
2. **書きは WAL append を memcpy 1 回で済ます**（tie_async ~1μs を維持）
3. **fsync は hot path から外す**（非同期 + 設定可能）

naive WAL は commit 毎に fsync → 1〜10ms/commit → v27 の 1000 倍遅くなる。これを絶対に回避する。

## アーキテクチャ

```
[writer]                        [consumer スレッド]           [fsync タイマー]
  │                                   │                          │
  ├─ WAL.append(op)  ◄─ memcpy 100ns  │                          │
  ├─ WriteQueue.push ─────────────────►│                          │
  └─ return                            ├─ pop → mmap 適用         │
                                       │                          │
                                       │ ◄──── 100ms or 64KB ──── │
                                       │                          │
                                       ├─ WAL fsync               │
                                       ├─ body msync              │
                                       └─ checkpoint 前進         │
```

## Durability モード

| モード | write レイテンシ | crash 時損失 | 用途 |
|---|---|---|---|
| `Async`（default） | 100ns〜1μs | 最大 100ms 分 | 開発、Oboro 用途 |
| `GroupCommit` | 100μs〜1ms | 最大 10ms 分 | SaaS 標準 |
| `Sync` | 1〜10ms | 0 | 金銭・重要データ |

## 実装フェーズ

### Phase 1: WAL レコード形式（0.5 日）

固定長レコード:
```rust
#[repr(C)]
struct WalRecord {
    magic: [u8; 2],      // [W, L]
    op_type: u8,         // 0=Tie, 1=Untie, 2=Delete, 3=Content, 4=Commit
    _pad: u8,
    len: u32,            // payload bytes
    lsn: u64,            // Log Sequence Number（単調増加）
    crc32: u32,          // payload の CRC
    payload: [u8; N],    // op 固有
}
```

### Phase 2: WAL リングバッファ（1 日）

- 別ファイル `*.db.wal`、sparse mmap（初期 256MB）
- head pointer（atomic、writer が前進）
- checkpoint pointer（consumer が前進）
- head と checkpoint の距離が閾値超えたら back pressure

### Phase 3: writer パス統合（0.5 日）

```rust
pub fn tie_async(&self, eid: u32, himo: &str, value: u32) {
    let himo_id = self.resolve_himo(himo);
    let lsn = self.wal.append(WalOp::Tie { eid, himo_id, value });
    self.write_queue.push(Op::Tie { eid, himo_id, value, lsn });
}
```

### Phase 4: consumer + 背景 fsync（1 日）

```rust
loop {
    if let Some(op) = queue.pop() {
        apply_to_mmap(op);
        last_applied_lsn = op.lsn;
    }
    if time_since_last_fsync > 100ms || bytes_since_last_fsync > 64KB {
        wal.fsync();
        body.msync();
        checkpoint.advance(last_applied_lsn);
    }
    if mode == Sync && queue.empty() { signal waiters; }
}
```

### Phase 5: 起動時リカバリ（1 日）

1. checkpoint LSN 読む
2. WAL を checkpoint+1 から head まで走査
3. CRC 検証、破損したらそこまで打ち切り
4. Commit で挟まれたグループのみ適用
5. 新規 head に移行

v10 の replay_wal 流用。

### Phase 6: チェックポイント（0.5 日）

- Phase 4 の背景 fsync に統合
- 手動 API: `db.checkpoint()`
- WAL サイズ閾値超えで強制

### Phase 7: ベンチ + 耐障害テスト（2 日）

**性能ターゲット**:
- Async: 書き込み 2μs 以下（現 ~1μs から 2 倍以内）
- GroupCommit: 100μs/op 以下（10k ops/sec）
- Sync: 2ms/op 以下
- 読み取り: 変化なし（pull_raw 10ns, query 70ns）

**耐障害テスト**:
- SIGKILL → 最後の Commit まで復元
- WAL 末尾破壊 → CRC で検出、そこまで復元
- fsync 直前で殺す（OS クラッシュ模擬）

## 実日数

| Phase | 日数 |
|---|---|
| 1 | 0.5 |
| 2 | 1 |
| 3 | 0.5 |
| 4 | 1 |
| 5 | 1 |
| 6 | 0.5 |
| 7 | 2 |
| **合計** | **6.5 日** |

## リスク

1. **WAL fsync と body msync の順序厳守** — WAL が先、body が後。順序逆転でリカバリ崩壊
2. **CAS 競合** — WAL head の atomic 前進で複数 writer のスループット確保
3. **リングバッファ一杯** — checkpoint 遅延時の back pressure、ブロック or panic の選択

## 既存資産

- `enchu/src/v10/wal.rs` — WAL 実装 350 行、CRC + truncate + multi-commit テスト済み
- `enchu/src/v11/replication.rs` — WAL ship ベースのレプリ（v30 で復活）
- `tests/v9_wal_recovery.rs` — クラッシュ復旧テスト
