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

## 現状カバレッジマトリックス（2026-04-22 時点）

| カテゴリ | カバー度 | 該当テストファイル |
|---|---|---|
| A. 正確性 | 高 | scenarios.rs、edge_cases.rs、lib 内 unit tests |
| B. 永続性 | 中 | wal_integration.rs、v29_destruction.rs、v29_stress.rs |
| C. 並行性 | 中 | concurrent_stress.rs、lib 内 `apply_pair_delta` 等 |
| D. 整合性 | 中 | v29_destruction.rs（CRC / truncate） |
| E. 分散 | 高（flaky） | v32_two_peer_sync.rs、v32_http_transport.rs、v32_replica_mode.rs、v32_byzantine.rs、crdt_and_chaos.rs |
| F. 性能 | 低 | examples/ にあるが tests ではない |
| G. API 網羅 | 中 | ad-hoc、網羅チェック無し |
| H. セキュリティ | 中 | v32_byzantine.rs |
| I. プロパティ | 中 | property_based.rs |
| J. 破壊・回復 | 低 | v29_destruction.rs（破壊のみ、restore 経路弱い）、phase_e_f.rs（snapshot roundtrip） |

---

## 既知の問題

### 1. v32_byzantine.rs 並行 flaky（2026-04-22 記録）
- `cargo test` デフォルト並行で不定失敗
- `--test-threads=1` / 単独実行で通る
- 原因未特定。HLC wall-clock 衝突 or InMemoryTransport の状態干渉を疑う
- 修正は本テスト整備フェーズで対処

### 2. ファイル命名が feature / phase 軸
- カテゴリ軸でない → 「この種類のテスト全部」を見つけづらい
- 再配置は巨大、現実的には README / 本 TEST_DESIGN.md で index する

---

## 追加すべきテスト（優先度順）

### P0（必須、次セッション）
1. **flaky 修正**: v32_byzantine.rs
   - 失敗レース原因の特定（HLC / tmp / transport state のどれか）
   - 各テストで真に独立な state を確保
   - 並行実行で 100 回連続 pass を確認

2. **crash / recovery E2E**: 実プロセス kill 系を強化
   - SIGKILL 中の tie_async 連続、再起動後 checkpoint 超えを読む
   - 現状 v29_destruction.rs にあるが v27 限定、v32（WAL + 署名）下での再現が不足

3. **API 網羅 audit**: 公開 API を列挙してテスト有無チェック
   - `grep -n "pub fn" src/engine.rs | wc -l` と `grep "fn test_" tests/**/*.rs` をクロス照合
   - 漏れのある API に最小テスト追加

### P1（高、今後 2〜3 セッション）
4. **性能 regression 検出**
   - `benches/` を `criterion` で回す（今は examples/）
   - 主要 op（tie、pull_raw、query、snapshot、audit）の baseline を記録
   - CI で ±10% 以上の劣化を検出

5. **BlobStore 拡張テスト**
   - 並行 reader + writer
   - ディスクフル時の error propagation
   - 破損検知（HashMismatch）が意図通り

6. **snapshot → restore → sync の全経路**
   - snapshot_export（origin）→ 別プロセスで open → peer として sync 続行
   - WAL 位置の整合

### P2（中、余裕あれば）
7. **ドキュメント例ビルド確認**
   - `cargo test --doc` を CI で回す
   - `lib.rs` / `README.md` のコードブロックが全て compile / run 可

8. **fuzz テスト追加**
   - cargo-fuzz で WAL record parsing を fuzz
   - content blob の edge size

9. **大量データ tests**
   - `#[ignore]` 付きで 1 千万 entity のインサート / 検索
   - 手動実行で回帰検知

---

## CI の推奨設定（未整備、P0 と並行着手可能）

```yaml
# .github/workflows/test.yml (例)
- name: default build tests
  run: cargo test --lib

- name: v32 tests (single thread)
  run: cargo test --features v32 -- --test-threads=1

- name: doctests
  run: cargo test --doc

- name: integration tests
  run: cargo test --features v32 --test '*' -- --test-threads=1
```

`--test-threads=1` は v32_byzantine flaky の暫定対応。flaky 修正後は 4〜8 に戻す。

---

## 実行計画

**次セッション着手（この TEST_DESIGN.md を参照しながら）:**

1. v32_byzantine.rs flaky 修正（最優先、CI 信頼性の土台）
2. API 網羅 audit → 漏れに最小テスト追加
3. crash / recovery E2E を v32（WAL + 署名付き）で 1〜2 本

P1 以降はそれらが片付いてから。

**想定工数:**
- P0 1 + 2 + 3: 3〜5 時間
- P1 全体: 10〜15 時間
- P2: 余裕時
