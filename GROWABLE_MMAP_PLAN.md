# Growable mmap Plan

組み込み DB として致命的な「空 DB が 88 GB sparse」問題の architectural fix。
仮想アドレスを最初に予約 (PROT_NONE) して、 ファイル本体は書き込みに応じて伸ばす。

## ゴール

- 空 DB が **数 KB** から始まる (SQLite と同等のサイズ感)
- 書き込みに応じて **amortized O(log N) syscall** でファイル拡張
- ベースアドレス不変なので **既存の Region 直 deref パターンを維持**
- macOS / Linux 両対応

## 非ゴール (今回スコープ外)

- 縮小 (`shrink_to_fit` 相当) — 別 PR
- 既存 DB の auto-migrate — `Engine::migrate_to_growable(path)` の opt-in CLI で対応予定、 today はデフォルト据え置き

## 設計

### 仮想予約 + コミット

```
起動時:
  base = mmap(NULL, layout.total_size, PROT_NONE, MAP_ANON|MAP_PRIVATE, -1, 0)
    ↑ 仮想空間だけ。 物理メモリも file も 0 byte 消費

  file.set_len(initial)   // 64 KB 等
  mmap(base, initial, PROT_RW, MAP_FIXED|MAP_SHARED, fd, 0)
    ↑ 先頭だけ実マップ

書き込みで committed を超えそう:
  ftruncate(fd, new_size)
  mmap(base + cur, delta, PROT_RW, MAP_FIXED|MAP_SHARED, fd, cur)
    ↑ 同じ仮想アドレスに重ね張り、 既存ポインタ無効化されない
```

`MAP_FIXED` は既存マップを silent override するので、 必ず自分の reservation 内
アドレスにのみ使う (アドレス算術ミス禁止)。 Linux では `MAP_FIXED_NOREPLACE`
(4.17+) が安全だが macOS 非対応なので careful arithmetic で統一。

### Growth 戦略

byte 単位 grow は syscall コストで NG。 amortized doubling:

```
64 KB → 128 KB → 256 KB → ... → 64 MB
それ以降は 16 MB chunk linear
```

10 万行 INSERT で grow 14 回程度。

region 別の方針:

| region | 性質 | 戦略 |
|---|---|---|
| header | 固定 | 初期 fully commit |
| entity index | 上限 max_entities × 8 byte | 初期 fully commit (小さい) |
| himo registry | 上限固定 | 初期 fully commit |
| vocab data | append-only 成長 | doubling |
| content data | append-only 成長 | doubling |
| cylinder | per-entity 紐配列 | 必要時 doubling |

「上限が小さく決まってる」物は最初から確保、 「成長するもの」だけ doubling。

## 実装手順

### Step 1: `GrowableMap` 新設 (`crates/enchudb-engine/src/growable_map.rs`)

- `new(file, reserve, initial) -> io::Result<Self>` — 仮想予約 + 初期 commit
- `grow_to(new_size) -> io::Result<()>` — page-aligned で伸ばす、 idempotent
- `base() -> *mut u8` / `committed() -> usize`
- `Drop` で `munmap(base, reserve) + close(fd)`
- libc 直叩き (memmap2 は MAP_FIXED 出さない)
- unit test: reserve 100 MB / initial 4 KB / grow 4K→8K→1MB→100MB の各段階で書き込みできること

### Step 2: `Engine::create_growable` 追加

- `engine.rs:939` の `create_full_with_cyl` を分岐: 旧 `set_len + MmapMut` パスと、 新 `GrowableMap` パス
- 新パスは `GrowableMap::new(file, layout.total_size, header_size + 4 KB)`
- ヘッダに `H_GROWABLE: u8` フラグ書く (open 時に旧/新を判別)
- `Engine` 構造体に `mmap_handle: GrowableMmapHandle` (`MmapMut` または `Arc<GrowableMap>`) を持つ

### Step 3: `Region` を offset ベースに

- 現状 `Region { ptr: *mut u8, len }` → 新 `Region { map: Arc<GrowableMap>, offset, len }`
- `slice() / slice_mut()` は `map.base().add(offset)` で同じパターン
- `ensure_committed(end)` を追加 — store 側がここを呼ぶ

旧 `Region` 直接生成箇所は engine の init 部分のみ、 全部新 ctor に置換。

### Step 4: 各 store の書き込み境界に `grow_to` 挿入

書き込みする store (要 grep audit):
- `entity_set.rs::add_entity` (上限固定なので grow 不要、 確認)
- `vocabulary.rs` の append 系
- `content_store.rs::set` (data_end fetch_add の手前)
- `cylinder.rs` の push / extend
- `himo_store.rs` (固定上限、 確認)

各書き込みで `region.ensure_committed(new_end)` 呼ぶ。 doubling は `ensure_committed`
内で計算、 caller は要求サイズだけ渡す。

### Step 5: ベンチ + 閾値調整

`crates/enchudb-engine/benches/growable_throughput.rs` 新設:
- N 行 INSERT (N = 1K, 10K, 100K, 1M) の wall time
- ファイルサイズ推移 (apparent / actual)
- syscall 回数 (`dtruss` または `perf trace` で外部計測)

比較対象:
1. 旧 `create_standalone` (現状)
2. 新 `create_growable` (各種閾値)
3. SQLite (rusqlite bundled)

調整ポイント:
- 初期 commit サイズ (4 KB / 64 KB / 256 KB)
- doubling 上限 (16 MB / 64 MB / 256 MB)
- linear chunk サイズ (4 MB / 16 MB / 64 MB)

ベンチで決まった閾値を const に固定。

### Step 6: 互換 + ロールアウト

- `H_GROWABLE` ヘッダ 1 byte で旧/新分岐
- 旧 DB は `MmapMut::map_mut` の旧パス維持 (read/write 両方)
- 新 DB はデフォルトで growable (`create_standalone` / `create_compact` も内部で growable に切替)
- `migrate_to_growable(path)` を別 PR で

### Step 7: 落とし穴 / テスト

- マルチプロセス: 別プロセスが先に grow した場合、 自プロセスは古い committed のまま。 書き込み時に `grow_to(end)` を idempotent に呼べば自プロセスにも MAP_FIXED が反映される
- fsync ordering: `ftruncate` → write → fsync
- Drop 順: `Engine` が `Arc<GrowableMap>` を最後に手放すよう設計
- `MAP_FIXED` の destructive 性 — reservation 範囲外に絶対呼ばない、 `debug_assert!` で boundary check

## 工数

- Step 1: 半日
- Step 2-3: 1 日
- Step 4: 1 日
- Step 5 (bench): 1 日
- Step 6-7: 1 日

合計 4-5 日。 breaking change なし (旧 DB 読める)。

## 進め方

ベンチ駆動で閾値決める。 まず動くもの → 計測 → 閾値調整 → 再計測のループ。
"理論最適" を最初に決め込まない、 dogfooding で見えた数値で詰める。

## 関連

- `issue.md` の `[open] FFI / C ABI`、 `[open] state-log tiny preset` と紐づく
- これが解決すると tiny preset の必要性が低下 (空 DB が KB から始まるから)

---

## 実装ステータス (2026-05-08)

### Phase A: プラミング完了 ✅

実施済み:
- `crates/enchudb-engine/src/growable_map.rs` 新設 — 仮想予約 + コミット primitive。 6/6 unit test green。 macOS 仕様で whole-file remap パターン採用 (隣接 slice の MAP_FIXED は EINVAL になる)。
- `Engine::Backing::Growable(Arc<GrowableMap>)` バリアント追加 — `as_mut_ptr / as_slice_mut / flush_to_disk / ensure_committed` を実装、 既存 `Mmap` パスと並列に動く。
- `Engine::create_growable` / `create_growable_full` 新規 — `create_full_with_cyl` と同じヘッダ書き込み + region init を growable backing で実行。
- `Region::with_grower` 新規 — region がファイル内 offset と grower への back-reference を持つ ctor。 既存 `Region::new` (静的 backing 用) は据え置き。
- `Region::ensure_committed(end_in_region)` — store 側が write 境界で呼ぶ。 grower 無しの場合 no-op。
- `Vocabulary::insert` (vocabulary.rs) — `data_end.fetch_add` 直後に `data.ensure_committed` + `offsets.ensure_committed` を挿入。
- `ContentStore::set` (content_store.rs) — 同様に append 境界で grow plumbing。
- `enchudb-sql::Database::create_growable` 公開。
- 既存 114 unit test 全 pass + growable 専用 test 2 件追加 (roundtrip + reopen-via-standalone)。

**現状の制約**: `create_growable` は init 時に layout 全体を pre-commit するため、 ファイル apparent size は `create_standalone` と同じ (88 GB sparse)。 「空 DB が KB スケール」の見た目はまだ実現していない。

### Phase B-tiny: state-log preset (実施済み 2026-05-09) ✅

**Phase B の本命 (lazy init refactor)** は 5-7 日仕事の大手術なので、 その手前で
matcha レベルの実用ニーズに応える **`create_growable_tiny` プリセット** を投入。

#### 中身
- `Engine::create_growable_tiny` — 1024 entities / 16 himos / 64 KB per data region
- `UndoLog::region_size_with(max_entries)` 追加 — undo は 16M × 10 B = 160 MB が
  layout を支配してたので、 4 K に縮めるオプションを開放
- `H_UNDO_MAX_ENTRIES` をヘッダ追加 — open 時に layout を再現可能に
- `Layout::compute_with_undo` / `from_params_with_undo` — undo size を渡せる
- `enchudb-sql::Database::create_growable_tiny` ラッパ公開

#### matcha 実測 (2026-05-09)
| 経路 | apparent | actual disk |
|---|---|---|
| 旧 `create_standalone` | 88 GB | 6 MB |
| `create_compact` | 305 MB | 46 MB |
| **`create_growable_tiny`** | **5.0 MB** | **5.0 MB** |

apparent **61× 縮小**。 backup ツール (Time Machine / rsync / Backblaze) が
まともに扱える。 lazy init 入ればさらに ~ KB スケールまで行ける。

#### 制約
- 1024 行を超えそうな state には不向き (`create_growable` (DEFAULT_MAX_ENTITIES) /
  `create_growable_with_capacity` を使う)
- まだ全 region が pre-commit されてる (sparse じゃなく fully written)
- 「真の lazy init」(空 DB ~ KB) は依然として Phase B の本命タスク

### Phase B (本命): 初期 commit 縮小 + Lazy init (未実施)

**当初想定より大きいスコープ** であることを実装中に発見:

#### 当初プランの問題

「audit して store の write 境界に grow を仕込めば commit を縮められる」と
読んでたが、 **各 store の `init` が region offset 0 に magic を eager に書く**
パターンがあるため、 layout 中盤の vocab/content init が先頭 commit を
即超えて SIGBUS する。 1 MB 初期 commit テストで実証済み:

```
Engine::create_growable() with initial_commit = 1 MB
  → Vocabulary::init writes magic at vocab_data_off ≈ 16.5 MB → SIGBUS
```

つまり「fresh DB の apparent size を layout total 以下に縮める」には、
**各 region の init を初書き込み時に遅延させる lazy-init refactor** が必要。

#### 必要な変更

1. **Lazy region init** — 各 store (Vocabulary / ContentStore / EntitySet / HimoStore /
   Cylinder) が `init()` で magic を書く設計をやめ、 「region は uninit で構築、
   最初の append/set 時に magic check + 必要なら write」に変更。
2. **Magic check の冪等性** — uninit 状態を区別するためのフラグまたは "first write
   detector"。 現状は init で magic を書き、 open で check するが、 lazy 化すると
   "今の region は init 済みか?" の判定が必要。
3. **Layout reorg** — variable セクションをファイル末尾に揃える。 lazy init で
   個々の magic write は遅延されるが、 grow 時に「必要な page だけ commit」する
   には全て monotonic-末尾 grow にできる layout の方が圧倒的に簡単。
4. **HimoStore + Cylinder の write 境界 audit** (これは元々想定通り)。
5. **Initial commit 計算式** — `header_size + 4 KB` 程度に絞れる。

#### 工数見直し

当初 3-4 日 → **5-7 日**。 各 store の init 設計に手を入れるのと、 全テスト
suite を新方式で通すのが主。

#### 暫定状態

`Engine::create_growable` は Phase A プラミング完備 + `initial_commit =
layout.total_size` (= 旧 create_full_with_cyl と同じファイルサイズ) で
保持。 Phase B 進めるまで、 matcha は `create_compact` 据え置きが正解。

### Phase C: 既存 DB の migrate (将来)

- `Engine::migrate_to_growable(path)` CLI — 旧 fixed-size DB を読み、 growable で書き直して atomic rename。
- 任意実行、 default は据え置き。

### matcha への適用

`matcha-shell::notif_state` は当面 `Database::create_compact` 据え置き。 Phase B 完了で `create_growable` に切り替えると `~64 KB` から開始する DB が手に入る。

---
