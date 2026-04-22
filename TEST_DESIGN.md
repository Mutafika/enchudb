# EnchuDB テスト設計

## 目的

DB として成立するための **網羅的テスト体系**を設計する。既存テストは feature / phase 別に散っており、「何がカバーされて何が抜けているか」が見えない。カテゴリ軸で整理する。

## テストカテゴリ（DB としての最低線）

### A. 正確性（Correctness）
- CRUD 基本: entity 作成・tie / untie / get / pull_raw / query
- 永続化: flush → open でデータが残る
- 境界値: 0 entity、u32::MAX - 1、空文字列、巨大 content
- スキーマ: define_himo の等値 / 超過 / 削除不可
- トランザクション: commit / rollback の巻き戻し正確性

### B. 永続性・クラッシュ整合性（Durability / Crash Consistency）
- flush 後の再起動で状態復元
- WAL 経由書き込みの crash 時再現
- SIGKILL / プロセス強制終了後の recovery
- 部分書き込み（torn write）の検出と切り捨て
- WAL リングバッファの繰り返し使用
- ファイル truncate 時のエラー検出

### C. 並行性（Concurrency）
- 複数 reader 並行（get / pull_raw / query）
- reader と writer の並行（ダブルバッファ Cylinder）
- 複数 writer 並行（lock-free 書き込み、write queue）
- rebuild 中の reader 非停止
- atomic swap の正しさ

### D. 整合性検証（Integrity）
- ヘッダ CRC の改竄検出
- region CRC sidecar の検証（seal_integrity）
- 1 ビット反転 tampering の検出
- ファイルサイズ不一致 → open エラー
- mmap 可能な最小構成の境界

### E. 分散（Distribution、v32）
- 2-peer pull sync の E2E
- N-peer 収束（chaos_sim 経由）
- LWW 衝突解決
- HLC 単調性・tiebreaker
- ed25519 署名検証
- TOFU pubkey 登録 / rotation rejection
- ACL 書き込み許可
- WAL 経由 bootstrap（sparse file）
- Replay attack / future HLC / mixed batch / stuck peer / keypair rotation

### F. 性能（Performance / Bench）
- insert throughput（1 thread / multi-thread）
- pull_raw latency（O(1) 確認）
- query latency（多条件 AND、pair table 効果）
- 集計（sum / min / max / avg / count）速度
- vs SQLite / DuckDB ベンチ
- 大量データ（100万〜1億 entity）スケール
- BlobStore put / get throughput
- WAL append latency（100ns 目標）

### G. API 網羅（API Coverage）
- 公開 API の全 method に対する最低 1 件のテスト
- エラーパス（Err を返す条件）のテスト
- ドキュメント例（lib.rs / CLAUDE.md）がビルド通る

### H. セキュリティ（Security、v32）
- 署名なし op の拒否
- 署名改竄（1bit 反転）の拒否
- 未登録 pubkey の拒否
- ACL 外 peer の拒否
- replay idempotency
- future HLC squat 対策（既知の制約を文書化）

### I. ランダム・プロパティ（Property-based / Fuzz）
- ランダム操作列 vs shadow model（HashMap/HashSet）
- proptest で invariant 検証
- 差分シミュレーション（sum / min / max など集計の一致）

### J. 破壊・回復（Destructive / Restore）
- `.db` file の一部 zero 化 → open 時検出
- `.wal` file 破損 → recover で切り捨て
- `.crc` sidecar 改竄 → open 時検出
- snapshot_export → 別プロセスで open して復元

---

## 現状カバレッジマトリックス（2026-04-22 P0/P1/P2 完了後）

| カテゴリ | カバー度 | 該当テストファイル |
|---|---|---|
| A. 正確性 | 高 | scenarios.rs、edge_cases.rs、lib 内 unit tests、api_coverage.rs |
| B. 永続性 | 高 | wal_integration.rs、v29_destruction.rs、v29_stress.rs、v32_crash_recovery.rs |
| C. 並行性 | 中 | concurrent_stress.rs、blob_store_extended.rs（並行 r/w）、lib 内 `apply_pair_delta` 等 |
| D. 整合性 | 高 | v29_destruction.rs（CRC / truncate）、blob_store_extended.rs（truncate → HashMismatch） |
| E. 分散 | 高 | v32_two_peer_sync.rs、v32_http_transport.rs、v32_replica_mode.rs、v32_byzantine.rs（flaky 修正済）、v32_snapshot_restore_sync.rs、crdt_and_chaos.rs |
| F. 性能 | 中 | benches/core.rs（criterion、baseline 可）、large_scale.rs（#[ignore]、大規模 soak） |
| G. API 網羅 | 高 | api_coverage.rs（pub fn 95 件 audit 済、漏れ 6 件に最小 test） |
| H. セキュリティ | 高 | v32_byzantine.rs、v32_crash_recovery.rs（signed WAL 復旧） |
| I. プロパティ | 高 | property_based.rs、fuzz_like_parsing.rs（WireRecord / WAL の random bytes） |
| J. 破壊・回復 | 高 | v29_destruction.rs、v32_crash_recovery.rs、v32_snapshot_restore_sync.rs |
| K. doc | 高 | cargo test --doc で 4 件 pass（lib / acl / blob_store / sync） |

---

## 完了した追加テスト（2026-04-22）

| 項目 | Commit | 成果物 |
|---|---|---|
| P0-1 flaky 修正 | 3ba12a2 | src/wal.rs auto_reset flag、Syncer で off、30/30 pass |
| P0-2 API 網羅 audit | a5f7231 | tests/api_coverage.rs(6 tests)、pub fn 95 件 audit |
| P0-3 crash/recovery E2E | a38f798 | tests/v32_crash_recovery.rs(2 tests)、crash_writer v32_signed_loop |
| P1-4 性能 regression | e5e4e53 | benches/core.rs(6 bench)、criterion、baseline 化 |
| P1-5 BlobStore 拡張 | 35f61ab | tests/blob_store_extended.rs(4 tests)、並行 r/w、readonly、truncate |
| P1-6 snapshot→restore→sync | 7c44fd5 | tests/v32_snapshot_restore_sync.rs(2 tests)、incremental sync |
| P2-7 doctest 実行可能化 | ae3a6b1 | 4 doctest 全て runnable、--doc で pass |
| P2-8 fuzz ライク | 3c5a889 | tests/fuzz_like_parsing.rs(5 tests)、proptest で decode 系 robustness |
| P2-9 大量データ | 860bc0c | tests/large_scale.rs(3 tests #[ignore])、10M/1M/100k |

---

## 既知の問題

### ~~1. v32_byzantine.rs 並行 flaky~~ → **解決(3ba12a2)**
根本原因は consumer の `try_reset` が `wal_sync` 直後(head==checkpoint)で
発火し、`publish_since` 前に WAL を reset していた race。`Wal::auto_reset`
フラグを default off にし、`Syncer::new` で attach 先を明示的に off に。
keypair_rotation 単独 30/30、cargo test 全体 10/10 緑。

### 2. ファイル命名が feature / phase 軸
- カテゴリ軸でない → 「この種類のテスト全部」を見つけづらい
- 再配置は巨大、現実的には README / 本 TEST_DESIGN.md で index する

---

## 残タスク（P3）

優先度低。実案件で必要性が出てから着手。

1. **CI gating**: `.github/workflows/test.yml` で criterion の ±10%、
   `--test --release`、`--doc` を pipeline 化
2. **真 fuzz**: cargo-fuzz (libFuzzer、nightly 必須) で coverage-guided
   — 現状は proptest の random bytes で代替
3. **S3BlobStore**: サーバー配置時に必要。今は Local で十分
4. **細粒度 ACL**: entity 単位 / himo 単位。Phase D 以降
5. **多次元距離クエリ**: 範囲 / nearest neighbor、Ravn 側で検討

---

## CI の推奨設定（P3 で整備）

```yaml
# .github/workflows/test.yml (例)
- name: default build tests
  run: cargo test --lib

- name: v32 tests (並行 OK、flaky 修正済)
  run: cargo test --features v32

- name: doctests
  run: cargo test --features v32 --doc

- name: integration tests
  run: cargo test --features v32 --test '*'

- name: bench regression
  run: cargo bench --features v32 --bench core -- --baseline main
```

`--test-threads=1` は不要(flaky 修正済)。

---

## 次セッション以降の方針

本体機能を作り込むフェーズへ。テスト土台は十分整った。
P0/P1/P2 で加えた harness を壊さずに機能追加していく。
