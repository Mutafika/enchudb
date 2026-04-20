# v32 PLAN — 分散 EnchuDB（GitHub 級スケールへの基盤）

## 目的

sinfo を「GitHub 級」まで育てられる基盤を作る。v32 で GitHub の全機能を作るわけではなく、**今後 3〜5 年で GitHub 級に到達できる設計**を確立する。

## スケール目標（長期）

| 軸 | 短期（v32 完成時） | 中期（v33-34） | 長期（GitHub 級） |
|---|---|---|---|
| 同時 peer | 数十 | 数千 | 数十万 |
| project 数 | 100 | 10 万 | 10 億 |
| 総 entity | 10 億 | 1 兆 | 1 兆+ |
| リージョン | 1 | 3-5 | 10+ |
| 認証 | 共有シークレット | 公開鍵 + 基本 ACL | OAuth + RBAC + audit |
| blob 格納 | 内蔵 content store | S3 互換抽象 | 多層 CDN + cold storage |

## 破壊変更（今すぐ決断する）

**この 3 つは今 v32 で確定する。後から拡張不可能なため**：

### 1. Entity ID: u32 → u64

- layout: `[peer_id: 32bit][local_id: 32bit]`
- peer 43 億、entity 43 億/peer、合計 1.8 × 10^19
- **既存 u32 API は全面廃止**（tie/get/query すべて u64 eid に変更）
- ファイルフォーマット 完全非互換

### 2. WAL フォーマット v2

- レコードに `(lamport: u64, peer_id: u32, signature: [u8; 64])` 追加
- ヘッダ 20B → 約 128B に拡張
- 既存 WAL は読めない（migration tool 別途）

### 3. Project = 1 DB の原則

- **1 DB ファイル = 1 project**（sinfo の各 project、GitHub の各 repo）
- project 間のクエリは不可能、検索インフラ（v36+）が担当
- global metadata（users, orgs, project list）は別の専用 DB

## アーキテクチャ概観

```
┌────────────── Client (laptop / CI / edge) ────────────────┐
│  app → enchudb (1 project) → transport → region relay    │
│                             ↓                              │
│                         local WAL                          │
└────────────────────────────────────────────────────────────┘
                             ↓ WAL ship (signed)
┌────────────── Region Relay (DC) ──────────────────────────┐
│  recv WAL → verify sig → apply local → fanout to peers   │
│  blob を S3 へ offload                                     │
└────────────────────────────────────────────────────────────┘
                             ↓ cross-region
┌────────────── Central Metadata ───────────────────────────┐
│  user accounts, org, billing, project registry           │
│  ここだけ強整合（Raft 的）                                  │
└────────────────────────────────────────────────────────────┘
```

## 設計要素

### 1. Peer ID と階層

```rust
#[repr(C)]
pub struct PeerId {
    kind: PeerKind,   // 0=edge, 1=region_relay, 2=central
    region: u8,        // 0..=255
    id: u16,           // peer unique within region
    _pad: u8,
}
// 合計 u32（32bit）
```

sinfo edge peer の例: `kind=0, region=1, id=5` → 0x00010005

### 2. Entity ID 空間

```
u64 eid = [peer_id: u32][local_id: u32]
例: peer 0x00010005 で local 42 → 0x0001000500000042
```

- peer がクラッシュ復旧してもそのまま local_id 継続
- peer が永久退役したらその peer_id の eid は tombstone（未達 entity）

### 3. 論理時刻: HLC（Hybrid Logical Clock）

```rust
pub struct Hlc {
    wall: u64,    // physical time (ms since epoch)
    logical: u32, // lamport-like counter when wall ties
    peer: u32,    // tie-breaker
}
```

- 物理時刻を含むので人間が見て意味が分かる
- 時計ずれに耐性（logical part で順序維持）
- 全順序: `(wall, logical, peer)` 辞書順

### 4. WAL レコード v2

```text
[magic "WL" 2B]
[version u8]       = 2
[op_type u8]       0=Tie 1=Untie 2=Delete 3=Content 4=Commit 5=Schema
[len u32]          payload size
[hlc: 20B]         wall(8) + logical(4) + peer(4) + pad(4)
[author_peer u32]  WAL 書いた peer(hlc.peer とは別: 中継 peer が書くこともある)
[crc32 u32]        payload CRC
[signature 64B]    ed25519 over (hlc ‖ op_type ‖ payload)
[pubkey_fp 8B]     pubkey の先頭 8 バイト（検証に使う）
[payload len B]
```

レコードヘッダ 116B + payload。署名検証は opt-in（初期 phase では no-op pubkey で）。

### 5. 署名と検証（ed25519）

- 各 peer は起動時に ed25519 鍵ペアを生成 or 読み込み
- 書き込みごとに HLC + op + payload に署名
- 他 peer から受信時に pubkey で検証
- pubkey の分散配布は project metadata で（初期は trust-on-first-use）

### 6. ACL / 書き込み権限

紐で表現可能：

```rust
// project に対して誰が書けるか
tie_ref(project_eid, "acl_writer", alice_pubkey_fp_entity);
tie_ref(project_eid, "acl_writer", bob_pubkey_fp_entity);
// WAL 適用時: author の pubkey が acl_writer に入ってるか確認
```

ACL ルール自体が project 内の紐データなので、ACL 変更も WAL に乗り分散される。

### 7. Pluggable Blob Store

```rust
#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn put(&self, hash: &[u8; 32], data: &[u8]) -> io::Result<()>;
    async fn get(&self, hash: &[u8; 32]) -> io::Result<Vec<u8>>;
    async fn exists(&self, hash: &[u8; 32]) -> io::Result<bool>;
}
```

- 紐の値は **blob のハッシュ (32B)**、実体は BlobStore に
- 実装: `LocalBlobStore`, `S3BlobStore`, `IpfsBlobStore`, ...
- EnchuDB 本体は blob を持たない（content store は小さいデータ専用に残す）

### 8. Pluggable Transport

```rust
#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, to: &PeerId, msg: SyncMsg) -> io::Result<()>;
    async fn recv(&self) -> io::Result<(PeerId, SyncMsg)>;
    async fn broadcast(&self, msg: SyncMsg) -> io::Result<()>;
}
```

- `InMemoryTransport` — テスト用
- `WebSocketTransport` — sinfo（既存コード流用）
- `QuicTransport` — 将来の高性能用
- `HttpRelayTransport` — ファイアウォール越え

### 9. 同期プロトコル

pull-based、階層対応：

```
Edge peer (Alice)
  ↓ (1) PullRequest { since: {peer_B: hlc_A_knows, ...} }
Region Relay (Tokyo DC)
  ↓ (2) PullResponse { ops with signatures }
Edge peer (Alice)
  ↓ (3) verify signatures, apply to local WAL
  ↓ (4) Ack
Region Relay
  ↓ (5) broadcast to other edges in region
```

Cross-region：region relay 同士が gossip、hot project は全 region に、cold は on-demand。

### 10. Bootstrap（新 peer 参加）

```
新 peer → Central Metadata: 「このアカウントが参加するプロジェクト一覧は？」
        → project metadata: 「alice/myrepo の初期状態 snapshot URL は？」
        → S3 / CDN: snapshot .db をダウンロード（大量）
        → region relay: snapshot LSN 以降の delta pull
        → 以降 live sync
```

## 実装フェーズ

各 phase の完了時点で sinfo が動くようにする（continuous integration）。

### Phase A: 破壊変更の確定（2 週間）

- u32 eid → u64 eid（全 API 書き換え）
- PeerId 構造体、eid = [peer|local] 合成
- WAL v2 フォーマット（HLC、署名フィールドは allocate のみ、検証まだ）
- Project = 1 DB の原則確認（既存そう）
- 既存 140+ テスト全部 u64 に移行、全緑

sinfo: u64 eid に追従する改修のみ（全 tie_to/get 等の型変更）

### Phase B: 基本分散（3 週間）

- HLC 実装
- InMemoryTransport、2-peer 同期の統合テスト
- LWW 衝突解決
- pull-based sync（flat、階層なし）
- sinfo-sync を enchudb-sync に移行（500 行 → 0 行）

sinfo: 2 peer で動く、sinfo-sync 廃止完了

### Phase C: 署名と初歩 ACL（2 週間）

- ed25519 鍵ペア、WAL 署名
- 受信時検証、不正署名は reject
- trust-on-first-use の pubkey 管理
- ACL 紐（acl_writer）ベースの書き込み制御
- WebSocketTransport 実装（sinfo-sync の既存コード流用）

sinfo: TLS/auth 付きで動く、sinfo-sync の auth 部分を流用

### Phase D: 階層 sync + blob store（3 週間）

- Region relay の位置づけ
- BlobStore trait、LocalBlobStore + S3BlobStore 実装
- content store を blob store にリダイレクト可能に
- hot/cold project の on-demand sync

sinfo: 大きな blob（モジュール tar.gz）を S3 に逃がして DB を軽量化

### Phase E: Central Metadata + Bootstrap（3 週間）

- user/org/project registry 用の専用 EnchuDB
- Raft 的な強整合は後回し、当面は「中央 1 台 + replica」で
- 新 peer の bootstrap フロー
- snapshot export/import（project 単位）

sinfo: 完全な分散運用、新規 peer が zero から参加可能

### Phase F: 運用（2 週間）

- Prometheus メトリクス
- 監査ログ（誰がいつ何を書いたか）
- backup / PITR（WAL 保存 + snapshot）
- スロークエリログ

sinfo: 本番運用に必要な可視性確保

---

**Phase A-F 合計: 15 週間（約 3.5 ヶ月）**

ここまでで GitHub の 1〜2% 規模まで。以降：

### Phase G+（v33〜）

- v33: コード検索（全文索引）、外部 Elasticsearch / Tantivy 連携
- v34: Actions 相当（webhook + runner）、event stream
- v35: OAuth、RBAC、audit 本格化
- v36: 高可用（Raft on central metadata）、DR
- v37: 多言語 SDK、GraphQL API、Webhook
- v38+: 独自 Git 互換層（リポジトリの「ファイル」抽象、既存 Git 資産取り込み）

## v32 の非目標

- **GitHub の全機能を作る** — 5 年以上かかる、v32 では基盤のみ
- **Raft / Paxos** — central metadata のみ将来必要、v32 はシンプル LWW
- **独自検索エンジン** — Tantivy/Elasticsearch 外部依存
- **PR / Issue UI** — アプリ層、sinfo-gui で実装
- **モバイル app** — 将来

## リスクと対策

| リスク | 対策 |
|---|---|
| u64 eid 移行で既存コード全破壊 | Phase A を 1 commit で完了、テスト全緑確認後に merge |
| HLC の wall 時計ずれ | NTP 前提 + logical part で緩和 |
| 署名検証の overhead | batch 検証、hot path はキャッシュ |
| S3 依存で組み込み環境壊れる | BlobStore は optional、LocalBlobStore がデフォルト |
| 階層 topology 複雑化 | まず flat、階層は Phase D で opt-in |

## sinfo との継続的統合

各 Phase 終了時に sinfo が動くことを確認：

| Phase 終了 | sinfo の状態 |
|---|---|
| A | u64 eid で動く（sinfo-sync はまだ使う） |
| B | 2 peer で enchudb-sync 経由、sinfo-sync 廃止 |
| C | TLS/auth 付き、本番っぽい運用可能 |
| D | 大 blob が S3 に、DB 軽量 |
| E | 新規 peer の zero bootstrap 成功 |
| F | メトリクス + 監査ログ、SLI/SLO 設定可能 |

**sinfo = v32 の continuous integration 環境**。各 phase で壊れたら即修正。

## GitHub 級への長期ロードマップ

```
2026 Q2: v32 Phase A-B  (sinfo 2-peer 分散運用)
2026 Q3: v32 Phase C-D  (署名 + blob store)
2026 Q4: v32 Phase E-F  (bootstrap + 運用ツール)
2027 Q1-Q2: v33 コード検索、Actions
2027 Q3-Q4: v34 RBAC、マルチリージョン
2028: v35-v36 高可用、PR/Issue 本格化
2029-2030: v37+ エコシステム（API、SDK、Webhook）
2031+: "GitHub 級" の 10〜30% スケールに到達
```

**GitHub 級は 5-6 年プロジェクト**。v32 はその最初の 3.5 ヶ月、**基盤の骨格**を固める。

## 即時アクション

1. この計画で合意取れたら V32_PLAN.md 確定
2. Phase A 着手: u32 → u64 eid 移行
3. sinfo 側も並行で u64 対応準備（型変更のみ、ロジックそのまま）

## 未確定の設計議論点

1. **u64 eid だが Ref 型の互換**: tie_ref(eid, himo, target_eid) — target も u64、WAL サイズ増える
2. **HLC の時計ソース**: OS 時計 vs NTP 強制 vs chrony 必須
3. **ed25519 か ed448 か X25519**: ed25519 が性能・成熟度で優位
4. **central metadata の位置**: sinfo 内部 project として扱うか、別アプリか
5. **sinfo-sync の auth コード流用範囲**: JWT か ed25519 か
