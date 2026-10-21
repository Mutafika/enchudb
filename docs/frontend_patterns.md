# EnchuDB アプリのフロント構成パターン — in-process と thin client

対象読者: EnchuDB にデータを持つアプリの「store から画面までの届け方」を決める人。
実証は naruhodo（法令リーダー・EnchuDB×sabitori の実験台・全 9,532 法令 / store 計 ~5.7GB）。
本文はアプリに依存しない一般則で、naruhodo 固有の実測は「実証」枠に隔離してある。

## 結論から

1. **EnchuDB でフロントを作るなら sabitori を使う。** EnchuDB の速さは
   「データ構造を変換しない」（himo 直読み・mmap・materialize しない）ことから
   来ている。フロントだけ JS/JSON 系にすると、**最後の 1 ホップで全部捨てる**。
   両端 Rust なら store → 画面まで変換ゼロの経路が物理的に引ける — これが
   スタックとして揃える価値そのもの。
2. 境界の渡し方は 2 型しかない:
   - **型 A: in-process** — 端末に store を積み、アプリが直接 mmap で読む。
   - **型 B: thin client** — server が store を読み、組んだビューを配る。
3. 型 B は放っておくと「render-ready DTO を毎リクエスト JSON で焼く」形になり、
   そこが**パイプライン全体で唯一の遅い場所**になる（実測: 応答の P50 1ms に
   対し P99 4.2 秒 — 遅さの全量が境界 1 ホップ由来）。処方は
   **不変鍵つき rkyv artifact**（§型 B）。

## フロント選定は選択問題ではない（sabitori 一択）

EnchuDB を採った時点でフロントの自由度は実質消えている。
web スタック（JS/React 系）にすると、境界で必ずこうなる:

```
himo (zero-copy) → Rust struct → JSON text → JS parse → GC オブジェクト → DOM
```

store 側でどれだけ規律を守っても、後半 4 段が毎回・全データに対して走る。
sabitori なら:

```
himo (zero-copy) → Rust struct ──(同一プロセスならそのまま / 境界越えは rkyv)──→ wgpu
```

- **DTO crate を両端で共有**できる（server / native / iOS / wasm が同じ struct を
  git tag pin で参照）。schema のズレはコンパイルエラーになり、runtime に漏れない。
- **rkyv が選択肢に入る**（後述）。JS フロントでは原理的に選べない。
- 1 つの UI コードが native / iOS / wasm に出る = 型 A と型 B を**同じ画面コードで**
  併用できる（フロント書き換えなしに配信形態を選び直せる）。

他の Rust UI（egui / iced 等）は transport 側の条件（rkyv・DTO 共有）だけなら
満たせるが、native / iOS / wasm 単一コードベースと tag pin 運用の統合を
再発明することになる。このスタックの中では選択肢は 1 つ。

## 型 A: in-process（端末に store を積む）

```
app プロセス ── mmap ──> store ファイル
     └── sabitori/wgpu で直接描画（serialize/parse ゼロ）
```

**選ぶ条件**: store が端末の容量・配布経路に載る ∧（オフライン要件 or
レイテンシが製品価値）。desktop ツール・端末内データのビューワが典型。

**規律**:
- **lean load**: 起動時に materialize しない。骨格は himo から復元し、本文級の
  大きい値は表示時に遅延読み。常駐 = 触ったページだけ（mmap の意味論を殺さない）。
- **配布前に seal する**: dirty store の readonly open は vocab 索引を heap 再構築
  する（cap 比例・実データ量と無関係）。writer open→Drop で clean flag を立ててから
  配る。
- **更新 = データ配布問題**として最初から設計する。store の差分配布・再取得の
  経路が無いなら、更新頻度の高いデータは型 B に置くべき。

**プラットフォーム別**:
- **desktop**: 最有力。制約ほぼ無し。
- **iOS ネイティブ**: 技術的には成立する（Rust+Metal+CJK 実証済み）。決め手は
  **store サイズ vs アプリ配布の予算**。GB 級データなら型 B へ。数十〜数百 MB で
  オフラインが価値なら型 A が正当。
- **wasm**: **選ぶな**。ブラウザには OS の lazy page-in が無く、
  「見かけのファイルサイズ = 確保メモリ」になる（実証済みの敗因）。
  ブラウザは常に型 B。

## 型 B: thin client（server 経由）

```
server ── mmap ──> store
   │ assemble（render-ready DTO を組む）
   │ rkyv archive（版ごとに一度だけ焼く）
   ▼
派生キャッシュ ──> HTTP（不変 URL）──> client（sabitori: 受信バッファをそのまま読む）
```

**選ぶ条件**: データが端末予算を超える / 中央で更新される / web に出す /
クライアントが多数。

### 既定形の罠

型 B を素朴に作ると「リクエストごとに DTO を組んで serde_json で焼く」になる。
storage は細粒度・lazy・zero-copy なのに、transport がエンティティ丸ごと・
text・毎回 — **粒度と形式の非対称**が境界に残り、大エンティティの直列化 CPU が
レイテンシの裾を作る。メモリでも帯域でもなく、ここが詰まる。

### 処方: 不変鍵つき rkyv artifact

これは「**mmap の意味論をネットワークへ延長する**」パターン:

| ローカル (EnchuDB) | ネットワーク (artifact 配信) |
|---|---|
| mmap ページ | 固定チャンク |
| ページの物理位置 | 不変 URL |
| OS ページキャッシュ | ブラウザ / CDN / nginx キャッシュ |
| 触ったページだけ page-in | 見える範囲の chunk だけ fetch |
| inode 参照 | resolver（論理 id → 現行版）|

構成要素:

1. **DTO は shared crate + serde と rkyv の両 derive**。rkyv が配信の本線、
   serde はダンプ窓とローカル永続（設定ファイル・localStorage 等）用で
   配信には使わない。offset 幅 size_32 を両端で固定
   （x86_64 / wasm32 / ARM で表現一致・全部 little-endian）。受け側は
   AlignedVec に受ける。encode/decode は **DTO crate が公開する唯一のペア**
   （blanket な `Wire` trait）を必ず通す — 各端が rkyv を個別に触ると
   版・feature・整列処理のズレの余地が生まれる。
2. **鍵 = (logic_ver, schema_ver, content_rev)** の 3 成分。全部必須:
   - `content_rev`: データ自体の版。EnchuDB アプリは append-only / 版付きに
     寄るので自然に存在する（無いなら作る — この鍵が立たないデータに
     このパターンは適用できない）。
   - `schema_ver`: DTO の形。serde では無痛な additive 変更も archive では
     レイアウトが変わる。
   - `logic_ver`: **assemble する側の版。忘れると事故る** — serve 時
     ロジックの修正は content_rev も schema も動かさないので、これ無しでは
     修正前の artifact を配り続ける。
   - 実装形: logic_ver + schema_ver は **build.rs の内容ハッシュ**（serve ソース
     + DTO ソース + Cargo.lock）1 個に潰すのが安全 — 「版番号の bump 忘れ」
     という人的操作自体が存在しなくなる。無変更 rebuild では変わらない =
     キャッシュ温存。
3. **lazy に一度だけ焼く**。初回リクエストで assemble → rkyv archive →
   派生キャッシュ dir に temp+rename。同一鍵の並行 miss は single-flight。
   **store には入れない** — 第二の master を作ると本体とズレうる
   （staleness の前例あり）。派生 dir は `rm -rf` 常に安全・store から再生可能。
4. **配信 = バイト列を送るだけ**。hit 時は assemble も serialize も走らない。
   `Cache-Control: immutable` で手前（nginx / CDN / ブラウザ）が勝手に持つ。
   bake 時に **gzip sidecar を併産**し Accept-Encoding で選ぶ（毎回圧縮しない・
   受けない相手には raw = 純粋な opt-in）。定型句だらけのドメインテキストは
   1/7〜1/8 に縮む。
5. **クライアントは段階的に**: まず bytes → rkyv deserialize → 既存 DTO
   （UI 無傷・text parse 消滅）。zero-copy render（&Archived 直読み）は
   UI 側の改修が山なので、実測で deserialize がまだ痛い場合のみ。
6. **大エンティティはチャンク**: chunk 0 = メタ + 内部索引（意味単位 → chunk
   割当表）、以降は意味境界（条・章・レコード群）で切った ~256KB。
   **byte Range は使わない** — 206 部分レスポンスはブラウザ / CDN のキャッシュに
   乗らない。固定チャンク URL（常に 200 + immutable）に切る。可変なのは
   「論理 id → 現行 content_rev」を返す小さな resolver だけ。
7. **配信は全 endpoint rkyv で統一する**。小レスポンス（検索結果・一覧・
   カウント級）は artifact キャッシュ不要 — 組み立てが安いのでその場 archive で
   良い。それでも format は揃える: 例外を作ると client に decode 経路が 2 本
   （= serde_json 依存）残り続ける。統一すれば fetch/decode は generic helper
   1 本になり、wasm binary からも serde_json が消える。

### クエリ型 endpoint: QueryCache

検索のような「入力の型が開いていて、答えが store にのみ依存する」endpoint は
artifact（不変 id 鍵）でなく **クエリ→レスポンス bytes の汎用キャッシュ**:
鍵 = logic_ver + **依存 store の (名前, サイズ, mtime) 指紋** + クエリ。
データ入替が server 再起動を伴う運用（mmap は動作中に新ファイルを見ない）なら
指紋は起動時計算で正しい。日付依存の endpoint は呼び側が鍵に日付を含める。

## 選び方まとめ

| 状況 | 型 |
|---|---|
| desktop ツール・データが端末に載る | A |
| オフラインが製品価値・データ小〜中 | A（iOS 含む）|
| データ GB 級 / 中央更新 / 多クライアント | B |
| ブラウザ | 常に B |
| 併用（中核データは端末・裾は fetch） | A+B（同じ DTO・同じ UI コードで両立可）|

## 実証: naruhodo（2026-07）

- 型 B・全 3 シェル（desktop / iOS / wasm）が同一 DTO を HTTP 取得する thin client。
  store 9,532 法令 / 本文 store 計 ~5.7GB — 端末配布は不成立で型 B が確定した例。
- lean load により server 常駐 315MB（materialize 型の 1.5GB から削減）。
- 負荷実測（100 並列・一様ランダム全法令）: 素朴 JSON 形は 167 req/s・
  **P50 1ms / P99 4.2s**（遅さの全量が「丸ごと assemble + serde_json」の境界
  1 ホップ・最大 19MB/レスポンス）。メモリ・store 読みは無罪。
- artifact 化後: **34,000+ req/s（×204）・P99 9.1ms（×460）・
  100 並列の処理メモリ +178MB→+56MB**。gzip sidecar で
  **民法 1.78MB→245KB / 租特法 19.5MB→2.4MB**。
- クエリ型（判例全文検索 = 1 クエリ 50〜680ms・頻出語ほど重い）は QueryCache で
  **674ms→0.5ms**。
- 配信の全 14 endpoint を rkyv 統一済み（wasm シェルから serde_json 依存が消滅）。
  検証は「wire decode ≡ JSON ダンプ窓」の全数 parity（9,532 法令 mismatch 0）。
- 型 A 側の実証: iOS ネイティブ spike（Rust+Metal+CJK 成立）、wasm 直読みの
  敗退（見かけサイズ = メモリ）、dirty store shadow 索引（seal で 6.9GB→603MB）。
- 適用の詳細（段階 Z1〜Z4・鍵設計・テスト常設化）= naruhodo repo の
  `docs/lawpage_artifact_plan.md`。

## ダンプは配信ではない

JSON がこのスタックに現れていい場所は**点検の覗き窓だけ**: 検証スクリプト・
curl・目視デバッグのための `?fmt=json` ダンプ。DTO に serde derive が併存する
限りコストは数行で、これは配信アーキテクチャの構成要素ではない — 設計文書で
論じる対象ですらない。「ダンプが JSON である」ことを配信経路に JSON を残す
理由にしないこと（軸が違う）。

外部公開 API（他言語クライアント向け）を持つなら、それはフロント配信とは
別プロダクトの設計であり、本文書のスコープ外。
