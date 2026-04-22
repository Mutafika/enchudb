# EnchuDB — 紐ベース円柱エンジン

## これは何？
組み込みDBエンジン。SQLite/DuckDBの代替。テーブルではなく「紐（himo）」でデータを管理する。
単一ファイル。全mmap。ロックフリー並行read。

## 依存
```toml
[dependencies]
enchudb = { path = "../enchudb" }
```

## 基本API

```rust
use enchudb::{Engine, HimoType};

// 作成 or オープン（単一ファイル）
let mut db = Engine::create("/path/to/db.db").unwrap();
// let db = Engine::open("/path/to/db.db").unwrap(); // 既存DBを開く（auto rebuild）

// 紐を定義
//   max_values は「索引カーディナリティのヒント」。値の上限ではない。
//   v24/v26: prefix sum O(1) + bitmap AND が有効になる目安。
//   v27: BucketCylinder の初期サイズヒント。超過値は動的拡張(silent clamp なし)。
//   0 を渡すと「ヒントなし、必要時に拡張」。
db.define_himo("age", HimoType::Value, 100);    // 整数、ヒント 0..=100
db.define_himo("dept", HimoType::Value, 20);     // 整数、ヒント 0..=20
// 100 や 20 を超える値も tie 可能(v27 は動的拡張、v24/v26 は max_values で clamp される注意点あり)。
// define_himo しなくても tie 時に自動作成される。

// 紐ごとの実カーディナリティ(v27 のみ、O(1))
db.himo_cardinality("age");  // → Some(現在の unique 値数)

// entity作成 + 紐を張る
let e = db.entity();
db.tie(e, "age", 30);                    // u32値（< u32::MAX）
db.tie_text(e, "city", "東京");           // 文字列（Vocabulary経由）
db.tie(e, "company", other_entity_id);   // entity参照もu32値として格納

// 値を読む（Column直読み、rebuild不要）
db.get(e, "age");                        // → Some(30)
db.get_text(e, "city");                  // → Some(b"東京")

// 紐を引く（検索）— rebuild() 後に使う
db.rebuild();                            // Column → Cylinder キャッシュ構築
let entities = db.pull_raw("age", 30);   // age=30 の全entity。O(1) or O(log n)

// 複数条件AND（query内部でrebuildも呼ばれる）
let result = db.query(&[("age", 30), ("dept", 5)]);  // age=30 AND dept=5

// 紐を外す / entity削除
db.untie(e, "age");
db.delete(e);

// トランザクション
db.commit();     // 変更確定（undo クリア）
db.rollback();   // 直前の commit まで巻き戻し

// 永続化
db.flush().unwrap();  // mmap を sync + メタデータ書き出し

// 非索引コンテンツ（検索対象外のblob、上限512MB）
db.content(e, "memo", b"hello");
db.get_content(e, "memo");  // → Some(b"hello")
```

## 大量データ（1億entity超）
```rust
let mut db = Engine::create_with_capacity("/path/to/db.db", 100_000_000).unwrap();
```

## 文字列検索
```rust
// tie_text で張った値は Vocabulary でIDに変換済み
// 検索時は vocab_id で先にIDを引く
let tokyo_id = db.vocab_id("東京").unwrap();
let result = db.pull_raw("city", tokyo_id);
```

## HimoType
- `HimoType::Value` — 整数値（age, dept, price など）
- `HimoType::Symbol` — 文字列（city, name など、Vocabulary経由）
- `HimoType::Ref` — entity参照

## マルチスレッド
```rust
use std::sync::Arc;

let mut db = Engine::create("db.db").unwrap();
// スキーマ定義（&mut self、起動時に1回）
db.define_himo("age", HimoType::Value, 100);
db.define_himo("city", HimoType::Symbol, 0);

// 初期データ投入（&mut self）
db.tie(e, "age", 30);
db.rebuild();

// Arc共有（以降 &self のみ）
let db = Arc::new(db);

// 読み書き並行（全部 &self）
db.entity();                          // entity作成
db.tie_to(e, "age", 30);             // 定義済み紐への書き込み
db.tie_text_to(e, "city", "東京");    // 定義済み紐へのテキスト書き込み
db.query(&[("age", 30)]);            // 検索（内部でrebuild）
db.pull_raw("age", 30);              // Cylinder直引き
db.get(e, "age");                    // Column直読み
db.rebuild();                        // ロックフリー（compare_exchange排他）
```

### &mut self vs &self
| メソッド | self | 用途 |
|----------|------|------|
| `define_himo` | `&mut` | スキーマ定義（起動時） |
| `tie` / `tie_text` / `tie_ref` | `&mut` | 書き込み（紐の自動作成あり） |
| `tie_to` / `tie_text_to` / `tie_ref_to` | `&self` | 書き込み（定義済み紐のみ） |
| `entity` / `delete` / `untie` | `&self` | entity操作 |
| `query` / `pull_raw` / `get` | `&self` | 読み取り |
| `rebuild` | `&self` | キャッシュ構築（ロックフリー排他） |
| `commit` / `rollback` | `&self` | トランザクション |
| `flush` | `&mut` | 永続化 |

## クエリ戦略（自動選択）

### v24（デフォルト）
1. **全条件bitmap有** → bitmap AND（非選択的クエリ ~5μs）
2. **それ以外** → 最小Cylinderスライスpull + Column直読みフィルタ（選択的 ~2-12μs）
3. **1条件** → Cylinder slice_one 直返し（10ns）

### v26（`--features v26`）
ペアテーブル + デルタシンクによる高速クエリ。v24 の上に積む。

```toml
enchudb = { path = "../enchudb", features = ["v26"] }
```

```rust
db.rebuild();
db.rebuild_pairs();  // ペアテーブル構築（rebuild 後に呼ぶ）

// クエリは同じ API。内部でペアテーブルを自動選択
let result = db.query(&[("tenant", 3), ("dept", 2), ("status", 1)]);

// 差分更新（tie のたびにペアテーブルを即時更新）
let old_val = db.get(eid, "dept").unwrap();
db.tie(eid, "dept", new_val);
db.update_pair_tie(eid, db.himo_id("dept").unwrap(), old_val, new_val);

// compact（デルタをベースに統合）
db.compact_pairs();
```

#### 戦略
1. **2条件以上** → 全紐ペアテーブルから最小セルを O(1) 選択 → 残りは Column 直読みフィルタ
2. **ペアテーブルより Cylinder スライスが小さい場合** → v24 パスに fallthrough
3. **1条件** → Cylinder slice_one 直返し（v24 と同じ）

#### 速度（100万 entity, 7紐）
| クエリ | v24 | v26 | 倍率 |
|---|---|---|---|
| 自社+部署+ステータス | 19.8μs | 69ns | 287x |
| 5条件全部 | 37.2μs | 79ns | 471x |
| 自社+給与帯 | 1.8μs | 72ns | 25x |
| 7条件全部 | 1.8μs | 99ns | 18x |
| ミス | 1.8μs | 50ns | 36x |

#### ペアテーブルの仕組み
- rebuild_pairs で全紐ペアの二次元テーブルを構築
- セル = (紐A の値, 紐B の値) → ソート済み entity リスト (Vec<u32>)
- define_himo で max_values 指定した紐のみ対象
- セル数 100万超のペアはスキップ
- デルタシンク: adds/removes リストで差分管理、compact でベースに統合
- 差分更新: 164ns/件

#### メモリ
- 100万 entity: 約 60MB（ペアテーブル全体）
- entity 数とペア数に比例
- mmap ではなくヒープ上（揮発、open 時に rebuild_pairs で再構築）

## クエリ言語（REPL用）
```rust
use enchudb::query_lang::{execute, QueryResult};
let result = execute(&mut db, "age:30 city:\"東京\" | count");
```

## ファイル構造（単一ファイル）
```
[Header 4KB]     レイアウト + himoメタデータ
[EntitySet]      生存ビットマップ + free stack
[UndoLog]        トランザクション用
[Vocabulary]     文字列辞書（data + offsets + index）
[HimoRegistry]   紐名辞書
[ContentStore]   非索引blob（index + data）
[Himo 0..N]      各紐: Column + Cylinder×2（ダブルバッファ）
```

## 紐のメンタルモデル
- **ぶら下げる** = 猫と"足"を引っ張って、交差したところに値（4）をぶら下げる
- **引く** = "足"を値4で引く → 猫,犬。その交差点にぶら下がってるもの全部出てくる
- **円柱** = ある entity からぶら下がってる紐を全部束ねたもの。猫の円柱 = [足|4], [読み|ネコ], [分類|哺乳類]...
- **暗記カードの束** = 円柱の別表現。1枚のカード = 1本の紐（表=紐名、裏=値）。束が円柱

概念を説明する時はAPIメソッド名を使わない。「ぶら下げる」「引く」で統一。

## 設計原則
- **紐が本質、円柱はキャッシュ。** Column（紐）がソースオブトゥルース。Cylinderはrebuildで構築されるキャッシュ。ペアテーブルもキャッシュ。
- **伝播はEnchuの仕事ではない。** JOIN相当の伝播はRavn（クエリ言語）が担当。
- **単一ファイル、全mmap。** 1つのスパースファイルに全領域配置。仮想サイズ大、実ディスク使用量は書いたページ分だけ。
- **ロックフリー並行。** ダブルバッファCylinder + AtomicBool swap。rebuild中もreaderは止まらない。
- **HashMap 不使用。** 紐名の解決は線形探索（himo_id）。紐数は高々数百なので十分速い。

## アーキテクチャ（v31）
```
tie_async(entity, himo, value)
  ↓
WAL append (memcpy、100ns)            → *.db.wal 永続化
  ↓
WriteQueue push (SegQueue、lock-free)
  ↓
consumer スレッドが pop
  ↓
BucketCylinder (v27、O(1) insert/remove)  → *.db mmap 永続化
  ↓ (並行 & 非同期)
PairTable (2次元キャッシュ、多条件 AND 70ns) → ヒープ
  ↓ 100ms 毎に consumer が
WAL fsync → body msync → checkpoint 前進 → WAL try_reset

[読み側]
pull_raw / query / get / Ravn.follow / reverse_follow / bfs
  → WAL に一切触れない、hot path は v27 と同速
```

**耐久性レイヤ**:
- WAL(線形 mmap、CRC32 per record。ring buffer reset は v32 で `auto_reset` flag が default off、`Wal::set_auto_reset(true)` で opt-in)
- header CRC(open 時自動検証)
- region CRC(`seal_integrity()` で opt-in、コールド検証)
- file size 検証(truncate → SIGBUS 防止)
- undo log(WAL commit で自動 clear、plain Engine 用)

### v26 で試して却下したもの
- CAS（コンテンツアドレッサブル）→ to_vec + hash 再計算で 46μs/件。デルタシンクの 164ns に負け
- 多次元ビットマップ Cylinder → 24GB メモリ爆発
- リンクドリスト（v20 Reverse）→ remove O(k) で遅い
- Z-order 曲線（v6）→ exact match にはペアテーブルが速い
- ペアセル同士の sorted intersect → Column 直読みの 8 倍遅い
- フーリエ変換 → 交差（積）を速くする道具ではない

## v28: WAL + crash consistency

```rust
// WAL 付きで作成 / open(concurrent writer 込み)
let db = Engine::create_concurrent_with_wal("/path/to/db", 256 * 1024 * 1024)?;
// 既存 DB の open は open_concurrent_with_wal(path, wal_capacity)
// 既存 WAL があれば commit 済みレコードを自動復旧

// 書き込み(既存 tie_async などそのまま使える、WAL に先に append される)
let e = db.entity();
db.tie_async(e, "age", 30);
db.content_async(e, "memo", b"hello");

// トランザクション境界を明示
db.wal_commit();       // Commit marker、非同期(consumer 側で fsync)
db.wal_sync()?;        // 強制 fsync + body msync + checkpoint 前進(Sync 相当)

// 観測
let s = db.stats();    // EngineStats: wal_head/checkpoint/durable_lsn など
```

**耐久性モード**(起動時に auto、手動切替は wal_sync で Sync 化):
- **Async** (default) — 100ms 周期で背景 fsync。最大 100ms 分失う可能性
- **Sync** — `wal_sync()` を毎回呼ぶと ~1-10ms/commit、損失ゼロ

## v29: 整合性 + 破損検知

```rust
// ヘッダ CRC(必ず ON、自動)
// → open 時に [0..64) の FNV-1a で検証。改竄で open エラー。

// region CRC(opt-in、コールドバックアップ封緘用)
db.seal_integrity()?;  // flush + *.crc sidecar 生成(全 region CRC)
// 次回 open で *.crc があれば自動検証、不一致でエラー

// file size 検証(自動)
// → open 時に file サイズ < Layout.total_size なら truncated エラー
```

**注意**: `.crc` は `open_concurrent_with_wal` で自動削除される(WAL 活動後 stale になるため)。WAL モードで使っている間は `seal_integrity` は無意味。**コールド状態の封緘**にのみ使う。

## v32: 分散 (HLC + 署名 + LWW)

```rust
// 1) peer ID + 鍵ペア
eng.set_peer_id(1);
let kp = std::sync::Arc::new(enchudb::keys::Keypair::generate());
eng.set_keypair(Some(kp.clone()));   // WAL append 時に ed25519 署名

// 2) 他 peer の pubkey を TOFU 登録
eng.pubkeys().force_register(2, &peer2_pub_bytes);

// 3) ACL(オプション、未定義なら全員通す)
eng.acl().add_writer(1);
eng.acl().add_writer(2);

// 4) Syncer + Transport で 2 peer 間 sync
use std::sync::Arc;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
let syncer = Syncer::new(eng_a.clone(), transport.clone());
syncer.set_require_signature(true);  // 未署名 / verify 失敗 op を reject

// publish: 自 peer の commit 済み op を transport に流す
syncer.publish_since(enchudb::Hlc::ZERO);

// pull: 他 peer の op を取得 + LWW で apply
let out = syncer.pull_once(2);
// out.{received, applied, skipped, rejected_signature, rejected_acl}
```

**EntityId の構造**: `u64 = [peer_id: 32bit][local_id: 32bit]`。`make_eid(peer, local)` / `eid_peer(eid)` / `eid_local(eid)`。単独 peer 運用は `peer_id = 0`。

**HLC**: `(wall: u64, logical: u32, peer: u32)` 辞書順全順序。`HlcStore` が `(eid, himo_id) -> Hlc` を保持して LWW 判定。

**LWW 規則**: 受信 op の HLC が `HlcStore` 既存より厳密に大きい → apply。等しい/小さい → skip。Delete は entity 全 himo に波及(sentinel `himo_id = u16::MAX`)。

**Transport は別 crate**: HTTP/WS 実装は `enchu-transport` に分離。`Transport` trait と wire format(`encode_batch` / `decode_batch`)は `enchudb::transport` に残る。

```toml
[dev-dependencies]
enchu-transport = { path = "../enchu-transport" }
```

```rust
use enchu_transport::http::{HttpRelay, HttpTransport};
let relay = HttpRelay::start("127.0.0.1:0").unwrap();
let client = HttpTransport::new(format!("http://{}", relay.addr()));
// client は Transport を実装、Syncer に渡せる
```

## v32: BlobStore (大 blob 外出し)

`content()` は単一 DB ファイル内に bytes を埋めるので 512MB/blob 上限。
画像/動画/モデル等は `BlobStore` に逃がして、紐の値 = `BlobId`(sha-256、32B) で参照する。

```rust
use enchudb::blob_store::{BlobStore, LocalBlobStore};
let store = std::sync::Arc::new(LocalBlobStore::new("/var/enchu/blobs")?);
eng.set_blob_store(Some(store.clone()));

// 直接使う
let id = store.put(image_bytes)?;        // sha-256 で content-addressed
let bytes = store.get(&id)?;             // Some(Vec<u8>) or None
assert!(store.exists(&id));
store.delete(&id)?;

// Engine 経由
let id = eng.put_blob(image_bytes)?;
let bytes = eng.get_blob(&id)?;

// id を紐の値として保存(BlobId.to_hex() を Vocabulary に乗せる等、運用次第)
```

**特性**:
- content-addressed: 同 hash = 同ファイル、自動 dedup
- atomic write: tmp + rename
- 読み取り時に sha-256 再計算で破損検知(`HashMismatch`)
- レイアウト: `<root>/ab/cd/ef0123...<60 hex>`
- 並行 r/w 安全(Arc<dyn BlobStore>)

## v32: changefeed (リアルタイム push sync)

WAL に commit が durable 化したタイミングで listener に WireRecord を渡す。
pull polling 抜きで「engine 触るだけで全 subscriber に流れる」構成が組める。

```rust
use std::sync::Arc;
use enchudb::changefeed::ChangeListener;
use enchudb::transport::WireRecord;

struct MyListener;
impl ChangeListener for MyListener {
    fn on_changes(&self, records: &[WireRecord]) {
        // commit 1 batch 分。HLC 昇順。consumer / wal_sync 呼出スレッドから。
        // 重い処理はここでブロックせず別スレッドへ forward すること。
    }
}

eng.add_change_listener(Arc::new(MyListener));
// 以降 wal_sync() / consumer の 100ms tick 後に on_changes が呼ばれる
```

**発火タイミング**:
- caller の `wal_sync()` 完了時(即時、caller スレッドから)
- consumer の背景 fsync 完了時(100ms 周期、consumer スレッドから)
- shutdown(Drop)時の最終 sync

**semantics**:
- 初回 `add_change_listener` 時に cursor を `wal.head()` に揃える → 過去 commit は流れない
- HLC 昇順保証
- at-least-once(crash → restart 跨ぎは `audit(AuditFilter { from_hlc, .. })` で resume)

**典型用途**:
- WS push hub: `enchu_transport::ws::WsPushHubAdapter` が listener を実装、broadcast に流す
- metrics: commit 数 / size を集計
- external replication: 別ストア (PostgreSQL / S3) に shadow

```rust
// WS push hub に流す例
use enchu_transport::ws::{WsPushHub, WsPushHubAdapter};
let hub = Arc::new(WsPushHub::start("0.0.0.0:8080")?);
eng.add_change_listener(Arc::new(WsPushHubAdapter::new(hub.clone(), 1)));
// あとは tie_async + wal_commit + wal_sync で全 subscriber に届く
```

## v32 Phase E/F: snapshot + 監査

```rust
// snapshot: main DB + .wal + .crc を別パスにコピー(全部 atomic に取れる)
let files = eng.snapshot_export("/backup/snap.db")?;
// files.{main, wal: Option<String>, crc: Option<String>}

// snapshot 取得前に必ず flush() or wal_sync() で durable 化すること。

// 監査: WAL の commit 済みレコードを filter
use enchudb::AuditFilter;
let recs = eng.audit(&AuditFilter {
    from_hlc: Some(some_hlc),
    author_peer: Some(2),
    pubkey_fp: None,
    ..Default::default()
});
// 各 RecoveredRecord は (lsn, hlc, author_peer, op, signature, pubkey_fp, signed_bytes)

// stats に HLC 情報も載る
let s = eng.stats();
// s.{peer_id, hlc_entries, max_hlc, ... 既存 wal_head/checkpoint/durable_lsn など}
```

## ファイル構成

```
{path}         — メイン DB(mmap)
{path}.wal     — WAL(v28 有効時のみ、sparse)
{path}.crc     — region CRC sidecar(seal_integrity 時のみ生成)
<blob_root>/   — BlobStore(別ディレクトリ、content-addressed)
```

## 制約
- `tie()` の value は `< u32::MAX`（u32::MAX は sentinel 予約）
- content data 上限 512MB(超えるなら BlobStore へ)
- max_himos デフォルト 256
- max_entities デフォルト 16M（create_with_capacity で変更可）
- WAL は線形成長(default `auto_reset = false`)。長期運用で容量食いつぶす前に `Wal::set_auto_reset(true)` で ring buffer reset を opt-in する。Sync 系で record を消失させないための既定値変更(v32)。
- v32 sync は WAL に署名済み record を残す前提。`auto_reset = true` で sync 中の peer がいると未配送 record が消える可能性あり、現状は watermark 未実装
