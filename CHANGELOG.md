# Changelog

EnchuDB の主要 release ごとの変更を時系列で記録。 0.x 段階につき **semver 厳密
ではない**が、 patch (z) は非 breaking、 minor (y) は API/format 変更を含む方針。

## 0.24.0 — 2026-08-28

**`.etxt` の segment merge (#188) と、 relay topology に残っていた scalar 前提 3 箇所の
除去 (#226 / #227 / #228)。** 前者は索引の作り直しを「全再索引」から「segment を足して
時々統合」に変え、 **build のピークをコーパス量から独立させる**。 後者は 0.23.1 が
relay の受け取り側 (cursor / ack / floor) を author 別に直した続きで、 **配布側・ack 側・
push 側**を揃える。

**on-disk format は不変** (`.etxt` / engine の v9 とも、 migration 不要)。 公開 API は
追加のみだが、 `enchudb-ngram` の再エクスポートと `Transport` trait の新メソッド
(いずれも既存コードは無改修) で公開面が広がるため minor bump。

### Added — `.etxt` の segment merge (#188 / PR #194)

`storage::merge_files(inputs, out) -> MergeStats` と、 `NgramIndex::merge_files` /
`TextSearch::merge_files` の委譲。 Gram Index (key 昇順) / Doc Index (eid 昇順) /
posting run (compact 済み = eid 昇順) が全部整列済みなので、 素直な k-way merge で書ける。

- 一度に触るのは **1 gram ぶんの posting run** だけ。 メモリに残すのは Gram Index 相当
  (distinct gram × 16B) と Doc Index 相当 (doc × 14B) で、 **本文量に比例しない**
- **後の input が勝つ** (LSM の上書き)。 上書きされた doc の**旧 posting も落とす**ので、
  統合結果は 「新本文で最初から索引した場合」 と一致する。 これが無いと畳んだ後も旧本文の
  語で候補に出る
- 上書きが 1 件も無い場合 (segment が doc を分割している通常ケース) は所有権判定を丸ごと
  飛ばす — hot path に binary search を持ち込まないため
- `n` 不一致 / 原文保持・postings-only の混在は明示エラー (flag を勝手に継承しない)
- **segment を作る専用 API は無い**。 `index()` → `save()` で焼いた普通の `.etxt` が
  そのまま segment になる

実測 (naruhodo の法令索引・494,133 doc / 1,007,416,823 B の `.etxt`):

| | ピークメモリ |
|---|---|
| 一気に索引 (従来) | 索引投入 +1.8GB / `save()` +0.9GB = **~2.7GB** |
| 25,000 doc × 20 segment に刻んで焼く | **146MB** |
| 20 segment を統合 | **27MB** |

統合結果は一気に索引したものと **byte 単位で同一** (1,007,416,823 B / sha256 `ebb172cd…`)。
`grams=227,268 / postings=69,238,492 / docs=494,133 / text=442,875,511B`。

統合の所要時間は **1.2〜5.8 秒** (page cache 次第。 segment が温かい直後が 1.2s、
cold read で 5.8s)。 ピークメモリは 2 回とも 27〜28MB で安定。 従来側の時間は
`save()` フェーズ単体しか測れておらず索引投入ぶんを含まないため、 比較対象として
載せない。

**公開 API**: `enchudb_ngram::{MappedIndex, MergeStats}` を再エクスポート
(`storage` モジュール自体は `pub(crate)` のまま — `save` / `write_to` 系の生の入口は
`NgramIndex` の wrapper と二重に露出させない)。

### 呼び出し側にとって何が変わるか

索引の作り直しが segment 単位になるので、 build ピークがコーパス量から独立する。
naruhodo は日次差分のために **本体 `.etxt` + `delta.etxt` の 2 層引き** (delta 在籍 doc は
本体ヒットを tombstone 抑制) を既に実装していて、 これは segment 数 2 に固定した segment
検索そのもの。 delta を本体へ畳む作業がこの merge に置き換わる。

### relay topology の補完 — replica 配布 / transitive watermark / push cursor (#226 / #227 / #228)

0.23.1 が relay の **受け取り側** (cursor / ack / floor) を author 別に直したのに続いて、
こちらは relay topology で**まだ scalar のままだった 3 箇所**を塞ぐ。 根は 0.23.1 と同じ
一つで、 **複数 author が merge された stream に scalar な値を当てている**こと。

| 実地で起きうる壊れ方 | 直したもの |
|---|---|
| relay 経由でしか author に届かない follower が `history_truncated` から**回復できない** (relay の state batch が常に空、 `bootstrap_pull` が None) | replica を state 配布元にする (#226) |
| relay が「配った」だけの履歴を author が「消化された」と信じて reclaim し、 relay の恒久消失で下流が**永久欠落** | ack を下流の消化で丸める (#227) |
| relay に張った WS subscriber が、 **中継された古い HLC の record を永久に落とす** (#216 の push 版) | push の subscriber filter を author 別に (#228) |

#### #226 — relay/replica が author の live state を配れるように

`Engine::state_records()` は live cell の走査で translated local を全 skip していた。
relay は author の行を**まさに translated local として**保持しているので、 replica の
state batch は**常に空**だった (実測: note 3 件を relay 済みの peer で
`state_records()` = 0 records、 `translated_locals_of(author)` = 3)。 加えて
`serve_state` は self_peer の 1 key でしか provider を登録せず、 `fetch_state(author)` が
relay に当たらなかった。 結果 #140 の bootstrap は **author 直結でしか効かなかった**。

- `Engine::state_records_for(author)` — replica 版の state 合成。 **原 eid** /
  **原 author** / **原 HLC** に戻す (remote apply は `set_cell(.., hlc)` 経由で author の
  HLC を版数に書いているので、 relayed cell の版数 = 原 HLC)。 `as_of` は emit した
  record の max HLC — 自分の clock で author の HLC 空間を進めると、 author 別 cursor
  (#216) が author の後続 record を飛び越す
- **Tag は author の vid 空間に戻す** (`peer_vocab_map` の逆引き)。 relay の local vid を
  `(author, vid)` として配ると、 author 直 pull で来る同じ key の別テキストと衝突して
  vocab 写像が壊れる (#209 と同種)。 逆引きできない vid は配らない
- `Transport::register_state_provider_for(author, by, provider)` (**default 実装つき**、
  既存 transport は無改修)。 `InMemoryTransport` は **author 本人発を優先**、 次に
  replica 発 — 本人発だけが `complete: true` (= ghost sweep を許せる)。 本人の engine が
  drop 済みなら自動で replica にフェイルオーバする
- `Syncer::serve_state()` は opt-in を記憶し、 pull で新しい author を取り込むたびに
  provider を自動追加する (app 側に儀式を増やさない)
- `Syncer::bootstrap_pull_via(link, author)` — **cursor は link の下に住む**。 link と
  author を同一視したままだと、 誰も pull しない link の下に cursor を書いてしまい
  前進が効かない。 `bootstrap_pull(p)` = `bootstrap_pull_via(p, p)` で後方互換
- `SyncOutcome::truncated_authors` — relay link には複数 author の stream が乗るので、
  bool だけでは bootstrap 対象が決まらない。 空 = author を特定できない広域 truncation

**v1 の限界**: replica 発の batch は `complete: false`。 relay が author の live state を
全部持っている保証が無い (途中から relay を始めた場合) ので、 受信側の ghost sweep は
走らせない。 **亡霊掃除は author 直 bootstrap の特権**として残る。 HTTP/WS transport は
state provider を実装していないので、 #140 と同じく `fetch_state` = None のまま。

#### #227 — relay の ack を下流の消化で丸める (transitive watermark)

pull-as-ack (#149) は「自分が apply した位置」を author に返す。 1-hop ならそれが消化
証明そのものだが、 **relay では「配った」でしかない**。 reclaim の安全条件は
「全 follower が **apply し切った**」なので、 relay が配った直後に恒久消失すると author は
履歴を捨て、 下流は永久欠落する (#191 の裏返しで 1 段深い)。 実測: 5 note (= 10 row) を
author し relay だけが pull した時点で、 下流が 1 件も消化していないのに author の
watermark = 10。

- `Engine::sync_delivered_cursors()` — 「下流全員が消化し切った位置」を author 別 HLC で
  返す。 **新しい永続 state は増やさない**: `lsn <= sync_watermark()` の `_sync_ops` row の
  author 別 max HLC + `sync_reclaimed_floors()` (#216) の author 別 entry から導出する。
  無帰属 baseline (`u32::MAX`) は author に帰属させられないので使わない (保守側)。
  `None` = 下流ゼロ
- ack 送出直前に `ack[a] = min(自分の cursor[a], delivered[a])` へ丸める。 tree のどの
  深さでも規則は同じ (「直接の下流の min」) なので、 hop を跨いで transitive に成立する
- 丸めるのは **relay (gossip) の時だけ** — 非 relay は他 author の row を転送しないし、
  下流ゼロの葉ノードで丸めると author の reclaim を永久に止める

**既知の縮退** (どれも「欠落」ではなく「reclaim 遅延」側): SubscriptionFilter で一部
author しか見ない下流がいると prefix walk が止まる (根は #219) / 下流の恒久消失で ack が
固定される (author 側の dead follower と同じ性質) / **「pull はしたが 1 件も消化して
いない下流」は `_sync_peers` に行を作らない**ので下流ゼロと区別できず丸めが効かない
(塞ぐには pull で行を materialize する必要があるが、 それは relay に限らず全 author の
reclaim 挙動を変える = 一度 pull して消えた peer が watermark を 0 に固定する #149 の
失敗形。 受け皿は #140 / #226 の bootstrap)。

#### #228 — WS push の subscriber filter を author 別 cursor に

`WsPushHub` の broadcast filter が `r.hlc > sub.since` の scalar 比較だった。 pull path は
#216 で author 別に直したが、 **push path は scalar のままだった**ので、 relay に張った
subscriber は中継された古い HLC の record を永久に落とす。

- `Subscriber` が author 別 cursor を持つ。 **entry の無い author は `Hlc::ZERO` 起点** —
  「既知 author の max」への短絡は同じ穴を一段下で再現する
- subscribe query に `since_by=<author>:<wall>.<logical>.<peer>[,...]`。 **1 entry でも
  壊れていたら丸ごと捨てる** (半端に読むと「消化済み」を偽る floor になる。 空なら全部
  ZERO 起点で送り直されるだけ)
- legacy の scalar `since` は **無帰属 baseline** として全 author に効かせる
  (「単一 author の stream に張っている」という宣言なので契約を変えない)
- `WsPushClient::connect_and_run_multi(url, from, &[(author, hlc)], cb)` を追加

doc で 2 点を明示: **push は pull を置き換えない** (delivery ≠ apply — WS の送信完了は
相手が apply したことを何も保証しない。 cursor 前進と pull-as-ack は `pull_once` の
仕事で、 push は pull の頻度を下げるもの) / relay が使う時の `peer_id` は **link 名**
(changefeed には原 author の record が流れるが、 subscriber は pull と同じく link に張る)。

### Known limitation — この release で増えたテストの多くは CI で走らない (#98)

CI の `test` job が回すのは core 5 crate (`oplog` / `engine` / `schema` / `sql` / `sync`)
だけで、 **`ngram` / `textsearch` / `transport` / `ffi` / `cli` / `rag` / root は全部対象外**。
`clippy` job も `continue-on-error: true` で non-blocking。 この release で増えたテストのうち

- `enchudb-ngram` 9 本 (#194)
- `enchudb-textsearch` 9 本 (#194)
- `enchudb-transport` 2 本 (#228)

は **CI では 1 本も実行されない**。 PR の緑チェックはこれらの crate について何も保証しない
(緑なのは「無関係な 5 crate が緑」という意味)。 拡張は #98。

実際にこの穴を踏んでいる: PR #194 の初版は `merge_files` を `save_postings_only` の
doc comment と `#[cfg(not(target_arch = "wasm32"))]` の**間**に挿入していて、 cfg gate が
新関数側に移り **`enchudb-ngram` の wasm32 build が壊れていた** (`E0425` / `E0433`)。
native では 66 pass で素通りするので、 native だけ回している限り永久に見えない。
**`cargo check --target wasm32-unknown-unknown` を 1 回走らせるだけで捕まる**種類の欠陥
だったが、 CI に wasm job が無いためどこにも信号が出なかった (merge 前に修正済み。
並べ替えのみ・意味の変更なし、 `wasm32-unknown-unknown` / `wasm32-wasip1` ともに
green を確認)。 wasm の対象範囲を定義して CI で守る件は #230。

## 0.23.1 — 2026-08-28

**relay 経路の correctness patch。** 0.23.0 が relay の**配布**を byte 単位の素通しに直した
(#209) のに対し、 こちらはその帰結を**受け取る側**が引き継げていなかった 3 箇所を塞ぐ。
症状はどれも「黙って消える」で、 relay を使っていれば定常運転で踏む。 非 breaking。

根っこは 1 つ: **relay の stream は複数 author の merge で HLC が単調でない** (自分の row は
自 clock、 中継 row は原 author の HLC 素通し)。 なのに cursor / ack / floor が scalar HLC の
まま = 「単調な単一 stream」 を前提にしていた。

| 実地で起きうる壊れ方 | 直したもの |
|---|---|
| 下流の cursor が relay 自身の新しい row で進んだ後、 **中継された古い HLC の record が永久に届かない** (`received: 0`、 truncation 通知も無し) | pull cursor を link × author の vector に (#216) |
| relay 混在 ring で ack が**未消化 record を watermark の下に巻き込み**、 reclaim が消す | ack walk を longest-consumed-prefix + per-author 述語に (#217) |
| relay の定常運転で `min(cursor) < floor` が恒常成立し、 **既追従の follower が bootstrap ループに入る** | history floor も author 別に (#216) |
| 並行 reclaim が free list に slot を二重 push し、 **同じ eid が二度払い出されて bridge row が上書き消失** | purge の delete + push を同一 lock 区間に (#221) |

### #216 — cursor / ack / floor を author 別に

author ごとの substream は relay を何 hop 挟んでも HLC 単調 (relay は pull 順 = author の
HLC 順に append する)。 この不変式が唯一の健全な粒度を決めるので、 3 つとも author 別にした。

- `Transport::pull_as_multi` / `record_pull_ack_multi` / `take_pull_acks_multi` /
  `set_history_floor_multi` / `history_floor_multi` を追加 (**すべて default 実装つき**、
  既存 transport は無改修で動く)
- **未知 author は `Hlc::ZERO` 起点** — 「既知 author の min」への短絡は同じ穴を一段下で
  再現する (新しく relay され始めた author の古い record が落ちる)
- cursor sidecar は v2 (`link author wall logical peer` の 5 field)。 **legacy 4 field 行は
  author=link として読む**ので、 他 author は ZERO 起点で再配送される = **旧実装が silent
  drop した record を upgrade が自己修復する**
- floor は author 別 max を記録する v2 encoding。 legacy の scalar floor は
  **無帰属 baseline (`u32::MAX` entry)** として温存し、 受信側が `max(entry[a], baseline)` で
  全 author に畳み込む。 baseline を越えた author から順に per-author 精度が戻るので、
  legacy floor を持つ既存 DB でも patch が効く (sentinel は自然 retire する)
- **`u32::MAX` は peer id として予約**になった (baseline sentinel と衝突するため)

### #217 — ack は「消化済みの最長 prefix」

旧実装は生存 row を lsn **降順**に走査して最初の `hlc <= cursor` で ack していた。 relay 混在
ring は lsn 順で HLC 非単調なので、 高 lsn に乗った古い HLC の中継 row に即 match し、
**その下の未消化 row を watermark の下に巻き込む**。 doc の「降順打ち切りは安全側に落ちる」
という記述は逆だった。

- lsn **昇順**に走り、 消化の証明が続く間だけ前進、 証明の無い row で停止する
- `Engine::ack_sync_up_to_cursors(peer, &[(author, hlc)])` を追加 (完全形)。 既存の
  `ack_sync_up_to_hlc` は `[(self, cursor)]` の退化形として同じ walk を通る — scalar は
  author を判別できないので **self-author 行の証明としてのみ**解釈する (`author == 0` =
  peer identity 設定前の local 著作も self 扱い。 除外すると単独運用 → sync 参加の path で
  永久 blocker になる)
- **dead row (payload 欠落 / decode 不能) は削除して越える** (`sync_dead_rows_purged()` で
  観測)。 配送不能な row を prefix blocker にすると全 peer の ack がそこで止まり ring が
  満杯になる (#149 で潰した backpressure の復活)
- 走査は前回 walk の到達点から再開する。 ただし**再開点は in-memory** — 永続の
  `consumed_lsn` には旧実装が over-ack した値が残りうるので、 session 最初の walk は
  それを信用せず全 ring を検証し、 小さければ**下方修正する** (移行 heal)

### #221 — purge の delete と free list push を atomic に

`Engine::delete` は冪等で「実際に消したか」を返さないため、 並行 reclaim
(`absorb_pull_acks` は複数 peer からの並行 pull で並行実行される) が同じ slot を free list に
二重 push していた。 実測で **138 write に対し 4 row の silent 消失**、 purge 数の二重計上も
確認。

- purge 専用 lock の下で **`expected_lsn` 一致**を再検証してから消す。 生存判定だけでは
  ABA を踏む (T1 が purge → slot が bridge に再利用されて同一 eid に新 row → T2 が stale
  snapshot で「生存」と判定して**その新 row を消す**)。 `lsn` は単調増加なので row の
  同一性判別子として機能する
- **`free_locals` は `delete` の前に取り push まで保持**する。 free list の producer は purge
  だけではなく、 枯渇 slow path の `rebuild_free_locals` が非 live local を穴として push する
  ため、 「delete 済み・push 前」の中間状態を観測されると両者から独立に入る

### observability

- `Syncer::truncated_pulls()` — `history_truncated` を返した累計。 単調増加しているのに
  bootstrap 成功が無いなら回復経路が塞がっている
- `bootstrap_pull` が state provider 不在で失敗したときの once-warn。 その構成では
  truncation が**回復経路の無い行き止まり**になるため。 **relay / reclaim が回る topology では
  `serve_state` は実質必須**
- `Engine::sync_dead_rows_purged()` — #217 の dead row purge 累計 (平常時 0)

### 既知の残り

- **#218** — decode 不能 row を purge しても history floor が上がらない。 この 1 種類だけは
  floor に現れないので、 over-ack が起きても truncation 通知に乗らず silent partial になる
- **#219** — publisher 側 `SubscriptionFilter` は pull cursor を scope 依存にする。 filter で
  落とした record は publisher の reclaim 後 bootstrap でしか戻らない (契約の明文化が必要)
- #221 の窓を**決定論的に踏む test は作れなかった** (発火に「free list が空 + 枯渇 +
  delete と push の間」の同時成立が要る)。 test 2 本は回帰検知として置いてあるが、
  正しさの根拠は lock 順序であってその緑ではない、と doc に明記した

検証: workspace 1058 passed / 0 failed (root crate 込み)、 参照 app (sunsu2) 21/21。

## 0.23.0 — 2026-08-28

**「黙って消えない」 を 3 層で塞いだ release。** 0.21.0 / 0.22.0 が sync の壊れ方を潰したのに
対し、 こちらは **engine が host に対して起こす事故** (panic での即死、 mmap 越しの SIGBUS)
と、 **`Ok(())` を返しながら op を捨てる 2 つの経路** を止めている。 relay/replica の
「中継 ≠ 作者交代」 も実装で固定した。

| 実地で起きうる壊れ方 | 直したもの |
|---|---|
| `oplog_sync()` が `Ok(())` を返したのに op が peer に永久に届かない | bridge cursor の lost update (#196) |
| entity 枠 / vocab / content が満杯になった瞬間、 **組み込み先の process ごと死ぬ** | capacity 到達を 「拒否 + 計数 + warn」 に (#59) |
| vocab index が満杯になると 100% CPU で無限ループ | 線形 probe の bounded 化 (#59 の副産物) |
| ディスクが満杯になると **`SIGBUS` で即死** (errno を返す syscall が無い) | `grow` 前の空き確認 → `ErrorKind::StorageFull` (#167) |
| **ディスク満杯で受け取れなかった record が二度と再配送されない** | 容量拒否を `SkippedOlder` に潰さない (#210) |
| relay 経由の配布が翻訳後の宛名を author 名義で撒き、 direct 経路と混ざると行が重複する | relay を byte 単位で素通しに (#209) |

### ⚠️ breaking: `Engine::entity()` が `Result` を返す

```rust
// 0.22.0 まで
let e = eng.entity();
// 0.23.0 から
let e = eng.entity()?;          // or .unwrap()
```

table 版の `Engine::entity_in()` は元から `Result` だった。 同じ 「entity を作る」 操作が
**片方は `Err`、 片方は process 即死** という非対称を消すための変更 (#59 の締め)。
`Err` になる条件は 2 つだけで、 どちらも 0.22.0 までは panic だった:

- **entity 枠が満杯** — 「DB が一杯」 は実行時の状態であって使い方の誤りではない。
  空き枠は `remaining_eid_space()` で事前に見られる
- **anonymous table が closed** (= `define_table` 済み) — `entity_in("<table>")` を使うこと

移行は呼び出し側に `?` か `.unwrap()` を足すだけで、 意味は変わらない。

### ⚠️ breaking: `remote_*_apply` の `relayed` 引数を撤去

`remote_tieleaf_apply` / `remote_tie_apply` / `remote_untie_apply` /
`remote_delete_apply` / `remote_content_apply` / `remote_vocab_apply` の末尾引数
(`Option<RelayedHeader>`) を削除した。

#209 で relay の実行が Syncer 経由の `Engine::relay_record` (原 `WireRecord` を持つ場所) に移った時点で
engine 側では **使われなくなっていた**引数。 breaking を 1 つの release にまとめるため
ここで撤去する。 移行は呼び出し側の末尾 `None` / `Some(relayed_header(rec))` を消すだけ。

### 検証状況 (先に読むこと)

- **#196 の fix は決定論的 regression test 2 本で固定**してある (fix 無しで必ず落ちる)。
  一方 **300 回の stress A/B は 「flake が消えた」 証明にはなっていない** — baseline 側が
  再現しない round があった (0/8)。 残りの切り分けは **#208** で追う
- **#167 / #210 は ディスクを埋めずに決定論的に再現**した (`set_space_margin` で必要空き量を
  水増しする knob)。 #167 は guard を外すと SIGBUS が戻ることも、 #210 は mapping を旧挙動に
  戻すと `skipped: 1 / min_rejected_hlc: None` になることも確認済み
- **#196 は 0.22.0 で踏みやすくなっていた**: #149 pull-as-ack が reclaim で ring を回す
  ようになったぶん `head == checkpoint` が成立しやすく、 fold の発火頻度が上がっていた
- **workspace 全体 (root crate 込み)**: release 対象の tree (`c473895`) で
  **1042 passed / 0 failed**。 2 台のセッションが独立に実測しており、 うち片方は
  CI 対象外の root crate (263 passed) と clippy パリティも併せて確認している
- **実 consumer での確認**: sunsu2 が **20/20 green** (relay fanout の収束 + relay 死亡 →
  bootstrap 復旧を含む)。 `entity()` の breaking は schema 層を経由しているため無風だった
- **CI の範囲に穴があった** (#213): `test` job は core 5 crate しか回さず、 root crate は
  重量 dev-dep のため除外されている (#98)。 #59 で panic を撤去したのに
  `#[should_panic]` の test が取り残されていたのを、 root crate 全体を回して発見・修正した
  (影響はその 1 件のみ)。 以降は手元 gate に root crate を含めている

### #196 — `Ok(())` を返しながら op を捨てる経路

`_sync_ops` は oplog の commit 済み record を peer 配布用に写す bridge で、
`sync_ops_offset` がその cursor。 ring を畳む (`try_reset`) ときは cursor も
`HEADER_SIZE` に巻き戻す必要がある。 ここに lost update があった:

1. consumer が fold して cursor を `HEADER_SIZE` に巻き戻す
2. **その直後**、 走っていた transfer が完了して **古い cursor 値を `store` で上書き** する
3. cursor が **未 bridge 領域を飛び越えた** 状態になり、 その区間の op は
   `_sync_ops` に永久に現れない = peer に届かない。 `oplog_sync()` は `Ok(())` を返す

3 層で塞いだ:

- cursor 前進を **CAS 化** — 期待値と違えば stale store として弾き、
  `sync_ops_cursor_repairs` に計上
- fold を **`transfer_lock` 保持下で再評価** — `try_reset_if(|| wal_fold_safe_locked())` に
  述語として渡し、 lock 外の判定と fold の間に transfer が挟まる窓を閉じる
- **tripwire**: `offset > head` (= cursor が head を追い越した) を検知したら cursor を
  巻き戻して計数 + warn。 既に壊れている DB も self-heal する

### #59 — capacity 到達で host を殺さない

embedded DB は他人の process に埋め込まれる。 「枠が満杯」 は想定内の実行時状態であって
使い方の誤りではないので、 panic で host を殺してはいけない。

- `FaultKind` (`EntitySpace` / `ContentSpace` / `VocabSpace` / `ValueOutOfRange` /
  `DiskSpace`) 単位で計数 + 1 秒に 1 回の rate-limited warn。 `fault_count(kind)` /
  `fault_total()` で観測できる
- `EntitySet::allocate` が `Option`、 `Vocabulary::insert` が sentinel、
  `ContentStore::set` が `bool` を返すようになった (旧: `assert!` / `panic!`)
- `value >= u32::MAX` の 7 箇所の `assert!` を 「拒否 + 計数」 に。 `u32::MAX` は sentinel 予約
- `Syncer::try_new` を追加 (`new` は従来どおり panic)
- FFI の cell accessor が row/col の範囲前提を外した

**副産物の hang 修正**: vocab index が 100% 埋まった状態で `lookup` / `index_insert` の
線形 probe が終了条件を持たず、 **10 分間 100% CPU で回り続ける** のを実測した。
両方 `index_cap` で bounded 化。

### #167 — ディスク満杯を SIGBUS ではなく Result にする

全 mmap なので書き込みは write syscall を通らない。 **errno を返す経路が無いため、
ディスクが満杯だと SIGBUS になる**。 `ftruncate` も ENOSPC を報告しない (sparse なので
予約しない)。

`grow_to` の前に `fstatvfs` で空きを見て、 `必要 delta + margin` に足りなければ
`io::ErrorKind::StorageFull` を返すようにした。 margin 既定 32 MB、
`set_space_margin()` で調整、 `space_denials()` / `disk_free_bytes()` で観測。

併せて、 **捨てられていた `ensure_committed` の error を 12 箇所 threading** した
(`LeafStore::insert` / `Vocabulary::insert` が sentinel、 `HimoStore::set` が `bool` 化、
cell version / tombstone の read は `Hlc::ZERO`)。 ここを捨てていると、
空き確認を通した後で伸ばせなかった場合に同じ SIGBUS に戻る。

### #210 — 「今は置けない」 を 「再配送不要」 に潰さない

#167 の容量拒否は engine 側の戻り値が `bool` だったため、 sync 受信側が
`ApplyResult::SkippedOlder` (doc に 「再配送は不要」) として計上していた。 `SkippedOlder` は
`min_rejected_hlc` を立てないので **pull cursor がその record を越え、 空きが出ても二度と
再配送されない**。 #167 は破損を防いだ代わりに、 それを静かな喪失に置き換えていた。

`remote_*_apply` の戻り値を **`RemoteApply::{Applied, Stale, RejectedCapacity}`** の 3 状態に
した (容量拒否の経路を持つ `remote_tieleaf_apply` / `remote_content_apply` の 2 関数のみ)。
sync 側は `RejectedCapacity` で `note_reject` を呼び、 cursor を止めて再配送させる
(`SyncOutcome::rejected_capacity` で観測)。

Content 経路は `store.try_set` が engine 呼び出しの **前** にあり、 1 byte も書けなかった
record の HLC だけが残って再配送も弾かれる状態だったので、 **apply 成功後に記録** する形に
変えた。

### #209 — relay/replica の正しさ: 中継 ≠ 作者交代

gossip relay が **翻訳後の eid / value** (relay-local slot / vid / translated ref) を
author 名義で再配布していた。 relay 経由のみなら 「一貫して間違った namespace」 で辻褄が
合うが、 direct 経路と混在 (bootstrap 復旧 / relay 死亡 fallback) すると **行が重複し、
vocab 写像が汚染される**。 署名も eid / value を書き換えた時点で不一致になるため、
`require_signature` 環境では relay がそもそも成立しない。

- engine 側 6 箇所の gossip 分岐を撤去し、 relay の判断を `Syncer` に移した
  (**`ApplyResult::Applied` の枝限定** + Commit / 自 author 除外)。 これは
  「LWW gate だけが cyclic topology の echo を止めている」 ため — 無条件に relay すると
  閉路 1 本で無限反響する
- **署名素通しは byte 単位でしか成立しない**: 署名は LSN 込みの固定 header に掛かるので
  op を再 encode すると必ず壊れる → `OpLog::append_relayed_verbatim` が `signed_bytes` を
  そのまま格納し、 LSN も author のまま置く (`next_lsn` を消費しない)
- verbatim 化で **原 eid が WAL に載る** ため、 WAL recovery に翻訳経路
  (`replay_relayed_op`) を追加

`remote_*_apply` の `_relayed` 引数は互換のため残置してあり、 `entity()` の breaking と
同じ release で撤去する。

storage format 変更なし (v9 のまま)。

## 0.22.0 — 2026-08-27

**author の生涯 op 数上限 (≒ ring 容量) を撤廃した release。** peer SNS 試験機
(sunsu2) を 24-peer Zipf chaos / celebrity fanout / ring 溢れで回して踏んだ 7 件を、
1 つずつ根まで追って潰している。全て sync 経路の正しさ・容量の話で、 **storage
format 変更なし** (file format v9 のまま、 旧 DB はそのまま開ける)。

| 実地で見えた症状 | 直したもの |
|---|---|
| relay/pull-only 構成で author が数千 op 書くと、 以後の変更が一切配布されない | #149 pull-as-ack (PR #202) |
| 一部 follower だけ永久に 0 配布になる (eager reclaim の床上げ) | reclaim の圧力 50% gate (PR #202) |
| reclaim 1 回で消化済み follower まで全員 bootstrap 送り | #191 floor = reclaim 済み最大 HLC (PR #193) |
| 大 burst (数万 tie) で書き込みが永久停止する livelock | #195 consumer の自 queue push 撤廃 (PR #198) |
| `.tables` sidecar が並行 persist で消える / 壊れる (ENOENT / torn install) | #190 persist の per-engine 直列化 (PR #192) |
| peer 1 個開くだけで write queue が 1M slot を eager 確保 | #116 scaled default + capacity knob (PR #189) |
| foreign entity への Ref が sync で壊れる / read で peer prefix が落ちる | #183 TieRef wire (PR #187) + #184/#185 Ref read・facade (PR #186) |

### #149 pull-as-ack — 生涯 op 数上限の撤廃 (PR #202)

relay/gateway (pull-only) 経路には明示 ack が無く、 誰も `ack_sync` を呼ばない →
`sync_watermark()` = 0 固定 → `_sync_ops` ring が reclaim されず満杯 → bridge が
backpressure で stall。 **author の生涯 op 数 ≒ ring 容量** (65k-entity 構成で
実測 971 post で stall) だった。 CHANGELOG 0.18.2 の self-ack 回避策は現行では
二重に死んでいる (v9 で `stats().max_hlc` = None / `sync_watermark` の self 除外)
ので、 利用側での回避も不可能だった。

fix: pull cursor は durable barrier 通過後にしか前進しない = そのまま消化の
到達証明なので、 これを transport 経由で author に還流する。

- `Transport` trait に `record_pull_ack` / `take_pull_acks` を追加 (**default
  no-op で後方互換**。 運べない transport は従来挙動)。 `InMemoryTransport` 対応済み
- `Syncer::pull_once` が確定 cursor を自動記録、 `Syncer::publish_since` 冒頭の
  `absorb_pull_acks` が `ack_sync_up_to_hlc` で consumed_lsn に写す。
  **app は publish/pull を回すだけ、 新 API 呼び出し不要**
- E2E: ring 850 row に対し 6000 op を publish/pull loop で全配布
  (旧挙動は **846/6000 で silent stall**)

**reclaim は ring 使用率 50% 超の時だけ** (ここが要)。 「ack が来たら即 reclaim」
は 24-peer chaos が即死パターンを検出した: floor が即上昇し、 その author を
**まだ一度も pull していない follower** (round 1 個ズレの参加 / 一時 offline) が
cursor < floor で永久 truncation になる。 履歴は容量が許す限り保持して差分
追いつきを最大化し、 reclaim は容量管理に徹する。 semantics の明示:

- watermark は「一度でも pull して ack が届いた peer」の min。 pull したことの
  ない peer は待たない (open topology では存在を知りようがない)
- 一度 pull して消えた laggard は watermark を pin する (追い出し policy は未実装)
- ack は「実在確認済み生存 row の lsn」までしか進まない = 未 pull record の
  過剰 reclaim なし

### その他の fix

- **#191 (PR #193)**: history floor が「生存 record の最小 HLC (空なら Hlc::MAX)」
  だったため、 reclaim 1 回で**全履歴消化済みの follower まで** truncation 判定に
  なっていた。 floor = 「reclaim で消えた record の最大 HLC」 に変更し、
  `_sync_peers` の sentinel row に永続 (reopen 後も正しく広告)。
- **#195 (PR #198)**: consumer thread の bridge が `entity_in("_sync_ops")` 経由で
  **自分しか drain しない write queue に blocking push** し、 満杯時に livelock。
  drain handler が no-op の `EntityCreated` を counter 対称 bump に置換。
  #116 の小 queue default で顕在化していた。
- **#190 (PR #192)**: `.tables` sidecar の atomic write が同一 tmp 名を共有し、
  consumer の定期 persist × pull 側 persist の並行で ENOENT / torn install /
  順序逆転。 per-engine lock で serialize → write → fsync → rename を直列化。
- **#116 (PR #189)**: write queue が容量 1M slot を eager 確保していた。 default を
  max_entities 連動に scale + `with_queue_capacity` knob。 queue 4096 でも
  write/read 速度は不変 (drain 分布重複を実測)。
- **#183/#184/#185 (PR #186/#187)**: foreign entity への `tie_ref` が wire で
  local 部しか運ばず壊れていたのを世界番号 (peer prefix 込み eid) 同乗で発送
  (TieRef op)。 `Value::Ref` read の peer prefix 落ちと facade の eid ヘルパー
  re-export も修正。

### 既知の制限 (= 0.23 の scope)

- **HTTP transport (enchu-transport) は ack / floor をまだ運ばない** — trait
  default により旧挙動のまま動く (壊れないが ring 回転の恩恵なし)。 endpoint
  追加は enchu-transport 側の follow-up
- truncation された遅参 peer の復旧経路 (#140 bootstrap-first flow) は未実装 —
  圧力 gate で発生自体は稀だが、 起きたら手動 bootstrap
- laggard (一度 pull して消えた peer) の追い出し policy なし — watermark が
  pin されたままだと ring は再び埋まりうる (その場合も #152 backpressure で
  欠番は出ない)

### 検証状況

- workspace 1045 tests green (新規 guard: #149 ×4 / #190 / #191 ×2 / #195)
- 実 consumer gate: sunsu2 全 18 tests green — 24-peer Zipf chaos (churn /
  offline 窓 / restart ×2) 収束 + truncation ゼロ、 ring 容量超 1500 post flood
  全配布、 cold backlog 1200 post の ring 回転 drain
- fanout 実測 (M4 Max): follower catch-up 26k posts/s/core、 25 並列 62.6k
  deliveries/s/host

## 0.21.0 — 2026-08-26

**sync 経路で 「消えた / 復活した / 二重になった」 が起きる道を塞いだ release。** 2 台の
daemon に SIGKILL を混ぜた soak と実機で出ていた壊れ方を、 1 つずつ根まで追って潰している。

| 実地で見えた症状 | 直したもの |
|---|---|
| 相手が削除した行がこちらで生き残る / text cell に無関係な文字列が入る | pull cursor の順序違反 + `.vocabmap` (写像の永続先) |
| WAL に届いた write が body 未適用のまま checkpoint に埋められて消える | `recover_with_tail` |
| 翻訳できない remote vocab id を cell に書く | `dropped_vocab` として落とす |
| 消したはずのファイルが復活する | delete の冪等化 + open の sweep。 **移行してきた行も** |
| 観測記録だけ消えて、 その path の削除が永久に見送られる | local-only table (request19) |
| eid 枠が詰まって回復不能になりうる | 残量 API + 「満杯でも delete は通る」 の固定 |

### 検証状況 (先に読むこと)

**この rev そのものでは chaos soak を回していない。** 直前の rev (`7977053` = pull cursor /
`recover_with_tail` / delete の冪等化 / local-only table まで) では **8 seed × 30 分 ×
SIGKILL を 3 回連続で全 green**、 実機でも 218,971 records を捌いて dropped 0。

その green な rev からの差分は 2 つだけで、 どちらも独立に確認してある:

- **`bind_over_local_writes` counter** — 数えるだけで挙動を変えない (#178 の検知)
- **sweep の版数不明 cell の扱い** — 実機 store の複製で before / after を実測
  (8,490 行 → 0、 枠の使用率 97% → 46%)

---

**pull cursor が、 それが消費した state より先に durable になる順序違反を直した。** 併せて
sync の写像 (`(author_peer, remote_vid) → local_vid`) に永続先を作り、 `SyncOutcome` の
counter を「正常系の LWW skip」と「二度と来ない形で捨てた分」に分けた。

### 何が壊れていたか

受信 op を適用すると、 cell (mmap) のほかに 3 つの派生 state が動く:

| state | 置き場 | 消えたら |
|---|---|---|
| `next_local` (翻訳先 slot の払い出し位置) | `.tables` | #117 が live bitmap から自己修復 |
| `(author_peer, foreign_local) → local` の entity 写像 | `.eidmap` | **復元手段が無い** |
| `(author_peer, remote_vid) → local_vid` の text 写像 | **どこにも無かった** | **復元手段が無い** |

「復元手段が無い」のは、 **受信 op が自分の WAL に残らない**から (gossip_remote_apply が
off なら `append_relayed` も走らない)。 一方 `Syncer` の pull cursor は disk に永続する。

`pull_once` は `apply_records` → `save_cursors()` の順で、 写像の永続は caller 任せだった。
つまり **cursor だけが先に durable になる**。 ここで落ちると 「cursor は消費済みと言うが
写像は無い」 が確定し、 差分 pull では**二度と埋まらない** (cursor が越えているので当該
record は再配送されない)。

**caller 側では直せない** — `pull_once` が return した時点で cursor は既に落ちているため。

実地の発現 (syncretic、 SIGKILL を混ぜた 2 台の soak、 6 万操作):

- **相手が削除した行がこちらで生き残る** (194 件)。 写像を失うと後続の `Delete` が
  `resolve_remote_eid_existing` で外れ、 `skipped` に紛れて cursor が越える
- text cell に**受信側の無関係な文字列**が入る。 `Vocab` を消費した後に写像を失うと、
  後続 `Tie` の生 vid が別の文字列を指す

### 変更

- **`.vocabmap` sidecar を追加**。 `.eidmap` と同格の永続先。 magic `EVCM` / v1、
  atomic write + fsync、 不在なら空で続行 (additive、 後方互換)。 dirty のときだけ書く。
  1 entry 12 byte、 `(peer, remote_vid)` ごとに 1 つ (= memory 上の写像と同じ増え方。
  永続化で増加特性は変わらない)。 `snapshot_export` と HTTP bootstrap
  (`GET /bootstrap/vocabmap`) も運ぶ — ここが抜けると restore 後に同じ穴が再発する
- **`Engine::persist_sync_state()`**: sync 由来 state の durability barrier。
  `body_msync()` (cell 本体) + `persist_tables()` (`.tables` / `.eidmap` / `.vocabmap`)。
  local write 経路が既に守っていた順序 (`oplog_sync`: WAL fsync → body msync →
  checkpoint 前進) の**受信側 counterpart**で、 pull cursor が受信側の checkpoint に当たる。
  `persist_tables()` 自体の意味は変えていない (sidecar のみ、 `.vocabmap` が 1 つ増えただけ)
- **`Syncer::pull_once` が barrier を内側で守る**。 適用があったら永続してから cursor を
  進め、 **永続に失敗したら cursor を進めない** (次の pull で同じ record を再適用 —
  apply は冪等)
- **`SyncOutcome::dropped_unresolved`** を追加。 `skipped` は 「LWW で古いと判定した」
  だけを数える。 合算されていると、 無視して良い LWW noise に本物の欠落が紛れる。
  内訳は entity 写像が引けない / himo を定義できない / ref の target を解決できない。
  一度も sync していない entity 宛の `Delete` のように**正常系でも 0 にならない**が、
  予期しない増加は配送欠落の兆候
- `apply_one` の戻りを `bool` から型 (`ApplyResult`) に変更 (internal)

### 残っている穴 (この変更では塞いでいない)

`dropped_unresolved` に数えられる中で、 **「今は無理だが後なら適用できる」**ケースは
cursor を止めていない (= 従来どおり捨てて前進する):

- **table 容量の枯渇** — `resolve_remote_eid` の slot 払い出しが失敗すると、 受信行が
  黙って落ちる。 本来は backpressure として cursor を止めるべきだが、 `resolve_remote_eid`
  は `Option` で理由を返さないため区別できない
- **himo 予算の枯渇 (#118)** — こちらは枠が解放されないので、 止めると永久に進まない。
  捨てて数えるのが正しい

前者は理由を返す形に変えれば止められる。 「止めると永久に進まない」 ものと混ざっているので、
一律に止めるのは危険。

### 移行

`.vocabmap` は無ければ空で始まり、 次に `Vocab` を受信した時点から積み上がる。 既存 DB に
手当ては不要。 **ただし 「すでに消えている写像」 は戻らない** — 過去に取りこぼした cell は
再 author / bootstrap で埋め直すこと。

自前で cursor を持つ caller (WS push で `apply_records` を直接叩く等) は、 cursor を永続
する前に `Engine::persist_sync_state()` を呼ぶこと。

---

**recovery が、 body に適用していない record を checkpoint に埋めて恒久消失させていた
のを直した。**

concurrent write path は queue を 2 本持つ。 producer は op を `write_queue` へ、 record を
`oplog_record_queue` へ push し、 consumer thread が 1 tick の中で WAL append → body 適用 →
fsync/msync/checkpoint 前進 の順に流す。 つまり **WAL は body より先に書かれる**。

その間で殺されると 「WAL には在るが body には無い record」 が末尾に残る。 これ自体は crash
として正常で、 次の open の recovery が replay して埋めるのが筋。 ところが旧実装は
`recover()` が捨てた未 commit tail まで `advance_checkpoint(head)` で越えていた。 越えられた
record は以後どの scan からも見えず、 **body に反映されないまま恒久的に失われる**。

`advance_checkpoint` を committed_end に留めるだけでは直らない。 走行中の engine は誰もその
record を body に適用しないので、 次の周期 fsync が Commit を打って checkpoint を再び越える。
しかも Commit が付いた時点で `_sync_ops` へ bridge されるので、 **body に無いものを相手に
配る**状態が確定する。

実地 (SIGKILL 混じりの 2 台 soak): 9 cell を 1 行として書く insert が 「著者側の body には
2 cell、 相手には 3 cell」 で固まり、 以後の scan でも埋まらない行として残った。 PK cell が
欠けた行は PK 引きに掛からないので、 次の scan が同じ行をもう一度 insert し、 同一 PK の
entity が 2 つになる。

### 変更

- `OpLog::recover_with_tail()` を追加。 commit 済み group に加えて末尾の未 commit batch も
  返す。 `recover()` は既存の意味のまま (呼び出し元が他に 7 箇所あるため)
- `scan_from_offset` が未 commit batch を戻り値に含める (今までは捨てていた)
- `Engine` の recovery 2 経路を `recover_with_tail()` に

再適用は冪等 — cell 版数 (v9) / LWW が同じ HLC を弾く。 CRC 破損 record は
`scan_from_offset` が元から打ち切るので、 書きかけの tail は混ざらない。

### 移行

手当て不要。 **ただし既に失われた cell は戻らない** — checkpoint が越えた record は物理的に
残っていても scan 対象外なので、 再 author で埋め直すこと。

### 移行してきた行の delete が open の sweep で埋まらない (#176 の続き)

**#177 の sweep が、 実運用で最も修復が要る母集団だけ素通りしていた。**

実地 (syncretic の実機 store、 作り直す前のコピー / live 15,953 / 枠 16,384) を
**#177 入りのバイナリで複数回 writer open した上で** 8,490 行が残っていた。 内訳:

| | |
|---|---|
| tombstone 付きで生きている行 | 8,490 |
| うち 「削除より後に書かれた cell」 を含む (= 作り直し。 直す対象ではない) | **0** |
| うち live cell が全部 **版数不明 (ZERO)** | **8,490** (100%) |
| live cell の内訳 | zero 67,919 / stale 0 / newer 0 |

= **全行が全列そろって生きたまま、 削除が 1 cell も進んでいない**。 sweep が走った上で
残っているので、 版数不明 cell を素通りしていたことの裏付けになる。

修正版 (df90377) を同じ store に当てた結果:

|  | 修復前 | 修復後 |
|---|---|---|
| `interrupted_delete_count()` | 8,490 | **0** |
| `files` の live | 15,953 | 7,463 |
| `files` の free | 431 | 8,921 |
| 枠の使用率 | 97% | 46% |

open 時に `warning: finished 8490 interrupted delete(s) at open` が 1 行。 **枠が 8,490 戻る**
ので、 この形の store は作り直さずに済む。

v8 以前から上げてきた DB は、 移行時に既存 cell の版数が **不明 (`ZERO`)** になる。 sweep は
版数不明の cell を 「削除との前後が判らない」 として保守的に残していたため、 そういう行は
tombstone が durable でも本体が残り続けた — 再配送が来ない (record が ring から落ちた後)
場合、 **二度と直らない**。

durable な tombstone は v9 領域が生えた後にしか書けない (pre-v9 の tombstone は揮発
`HlcStore`、 `.eidmap` から復元される foreign 分も復元先は移行後の tombstone column) ので、

> durable な tombstone が在る ⇒ その版数不明 cell は削除より前に書かれた

が言える。 本体除去 (`remove_entity_body`) は元からこの判断で版数不明 cell を消していたので、
**sweep だけが食い違っていた**形。

- sweep の判定を本体除去と同一にした (残すのは **tombstone より真に新しい cell** だけ)
- `Engine::interrupted_delete_count()` — 修復せずに数えるだけ (readonly 可)。
  アプリ側が 「tombstone が在る && 行が生きている」 で数えると **削除の後に作り直された行**
  まで拾い、 修復しても数字が減らない (実地の監査が 8,490 行を残骸と報告したのがこの形)。
  判定を sweep と共有することで 「直したのに直っていない」 を無くす
- `Engine::repair_interrupted_deletes()` — 再起動せずにその場で埋める入口
  (通常は writer open のたびに自動で走る)

### eid 枠の残量を問い合わせられるようにした

**枠は create 時に固定で、 満杯にすると回復不能になりうる。** `entity_in` が `Err` を返した
ところでアプリが掃引を止めると、 **削除も流れなくなる** — 削除は枠を空ける唯一の手段なので、
一度この形に入ると自力では戻れない。 手前で気付く手段が公式に無く、 既知の table 名の
range から手で引き算するしかなかった。

- `Engine::remaining_eid_capacity()` / `Database::remaining_eid_capacity()` —
  まだどの table にも割り当てていない eid 空間 (= これから `with_capacity` で切り出せる上限)
- `Engine::table_eid_usage(name)` / `Database::table_eid_usage(name)` →
  `TableEidUsage { capacity, allocated, live, free }`。 `free` は削除で戻る
  (`allocated` は払出の最大なので減らない)
- `define_table` の枠超過 error に残量を載せた。 **黙って縮めない** — 頼んだ枠と違う
  table ができる方が事故になる

満杯でも `delete` は通り、 枠は即座に戻る (test で固定)。 枠そのものを後から伸ばす件は
`notes/requests/request20.md`。
### 書き戻しの宛名が付け替わらなかった時に、 それを数える (#178 の検知)

**静かに壊れる経路を、 まず観測できるようにした。** 直しそのものは #178 で継続。

翻訳した行 (= 相手が author の entity) への書き戻しは、 bridge 時に
`eid_translator.reverse()` を引いて元 entity の世界番号へ付け替えてから発送する。
つまり **束ねられる前に書いた分は自分の eid のまま出て行く**。 受け側はそれを既存行に
結び付ける手段が無いので (`bind_by_primary_key` は PK Tie が同 batch に居る時だけ効く)、
**PK を持たない重複行**を払い出す。 その行は代表 column (= PK) を持たないため
`Table::all()` の母集団にも入らず、 **アプリの監査からも見えない**。

実地 (syncretic の chaos soak) では 8 seed 中 1 seed で両側に 1 件ずつ出た。

- `Engine::bind_over_local_writes()` / `EngineStats::bind_over_local_writes` を追加。
  **「自分が書いた行が、 後から foreign identity に束ねられた」 回数**を数える
  (= その行の write は既に自分の eid で出ており、 相手に重複行が生えている可能性がある)。
  判定材料は cell の版数 (`Hlc::peer`) だけで、 追加の state は持たない
- 一度だけ warning を出す。 `0` が常態

### local-only table — WAL の耐久性は使うが peer には配らない table (request19)

**「この端末で観測した事実」 を、 本体の行と同じ WAL / commit に載せられるようにした。**

アプリが 「この path を、 まさに disk と突き合わせた」 のような**端末ローカルな観測**を
持つとき、 それを別ファイル (JSON 等) に置くと **「本体の行は WAL 経由で復元されるのに、
観測記録だけ消える」** が起きる。 実地 (syncretic の chaos soak) では、 削除の証拠を失った
path の削除が永久に見送られ、 apply が書き戻して**削除したファイルが復活**した
(8 seed 中 4 seed)。

一方この記録は **peer に配ってはいけない** — 端末ごとに違う事実なので、 配ると相手の
判断を壊す。 つまり 「**WAL には載せたいが `_sync_ops` には流したくない**」 table が要る。
`_sync_ops` / `_sync_peers` が既にその性質を持っていたので、 一般化して外に出した。

#### 変更

- `TableBuilder::local_only()` (schema 層) — `_` 始まりの table を local-only として作る。
  engine 層では従来どおり `Engine::define_reserved_table()`
- bridge の除外判定を **`_sync_ops` / `_sync_peers` 決め打ちから 「reserved table (= `_`
  始まり) 全部」 へ**一般化。 判定は名前だけなので sidecar の format 変更は無く、
  reopen を跨いでそのまま効く
- **local-only table への write を WAL に載せるようにした**。 従来は reserved table への
  write を丸ごと WAL から外していたが、 それでは耐久性が本体の行と揃わない。 外すのは
  **engine 自身の内部 table (`_sync_ops` / `_sync_peers`) だけ** — あちらの行は WAL record
  から作られるので、 積むと WAL が自分自身を食う
- local-only table の `Leaf` 列も通常 table と同じ LeafStore 経路に載るようにした
  (内部 table の vocab 据え置きは `_sync_ops.payload` 等だけの都合)
- `Engine::clear_local_only_tables()` を追加。 **snapshot / bootstrap の受け側**で呼ぶ —
  body を丸ごと写す以上 snapshot には local-only の中身も乗るが、 受け取った側にとって
  それは 「自分が観測していない事実」 なので空にしてから使う。 engine 内部 table は対象外
  (未配送 backlog と peer watermark は引き継ぐのが正しい)

#### 使い方

```rust
db.table("_local_seen").local_only()
  .tag("path").tag("hash").number("size").number("mtime")
  .with_capacity(200_000)
  .build()?;
```

- **eid 空間を `with_capacity` 分予約する** (`max_entities` と同じ性質)。 溢れると
  `entity_in` が `Err("table '...' eid range exhausted")` を返す (黙って落ちはしない)
- 「本体の行と同じ commit」 の意味は **WAL の同一 commit group に入る**まで。 group 途中で
  crash した場合は 「途中まで適用された状態」 で復元される (`recover_with_tail`)

### crash が途中で切った delete を、 再配送と open の両方で埋める (#176)

**削除は 「tombstone は在るが行は生きている」 で固まってはいけない。**

delete の 3 経路 (`Engine::delete` / `remote_delete_apply` / WAL replay) はいずれも
**(1) tombstone 版数を書く → (2) 全 himo の cell を落とす → (3) live 登録を外す** の順で
流す。 SIGKILL が (1) と (2) の間、 あるいは (2) のループ途中で落ちると (1) だけが残る。

query 経路 (`entities_with_himo` / `Table::all()`) は tombstone を見ないので、 この行は
**アプリからは生きて見える** — 実地では 「消したファイルが復活する」 形になる。 しかも
3 経路とも 「本体除去は `set_tombstone_local` が `true` を返したときだけ」 と書かれていて、
LWW は同値 HLC を弾くので **同じ Delete が再配送 / replay されても本体除去に到達しない**。
判定と適用が bool 1 本に潰れていたのが根で、 **一度この形になると二度と直らなかった**。

実地 (syncretic の chaos soak / SIGKILL 混じり) では保全した peer store **3 本すべてに
1 件ずつ**在った。 生き残った cell が毎回 himo 宣言順ループの**接尾辞**になっており、
中断点がループ内であることが確認できている:

| 生き残った cell | 形 |
|---|---|
| 9 cell 全部 | PK も生きているのでアプリから見える行 = 亡霊ファイル |
| `size` / `mode` / `mtime` / `symlink_target` | 識別子が落ちた残骸。 `Table::all()` の母集団に入らないので監査からも見えず、 `entities.free` も走らないので slot を占有し続ける |

#### 変更

- `Engine::apply_delete_local()` を追加し、 delete 3 経路をここへ寄せた。 **判定 (LWW) と
  適用 (本体除去) を分ける** — 拒否するのは受信 HLC が既存 tombstone より**真に古い**ときだけで、
  それ以外 (新しい / 同値) は本体除去を必ず実行する = **冪等**。 再配送と WAL replay が
  修復経路になる
- `Engine::finish_interrupted_deletes()` を追加し、 **writer open のたびに** 「tombstone より
  古い cell が生きている entity」 を掃除する。 record が ring から落ちた後は再配送が来ないので、
  既に壊れた DB を直す道がこれしか無い。 直した件数は warning として stderr に出す
- 本体除去は **削除より真に新しい cell を残す** ようになった (削除後に作り直された行を、
  同じ Delete の再配送で巻き添えにしないため)。 版数不明 (`ZERO`) の cell は従来どおり消す —
  pre-v9 / oplog 無効の standalone write は版数を持たないので、 残すと delete が効かなくなる
- WAL replay の Delete 経路が `free_leaf_cell` を呼んでいなかった (Leaf payload の leak) のも
  同時に直った

順序 (tombstone 先行) は**変えていない**。 先に本体を消すと 「tombstone 無しで cell が半端」 な
窓ができ、 そこへ届いた古い tie が復活させうる。 crash 窓に残る形は冪等化と open sweep の
両方で埋まる。

#### 移行

手当て不要。 **既に壊れている DB は writer open で自動的に直る** (v9 の DB のみ — pre-v9 は
tombstone 版数が揮発なので対象外)。 実地で壊れていた store 3 本で、 open 時に 1 件ずつ
掃除されることを確認済み。

### 翻訳できない remote vocab id を cell に書かない

**翻訳できない remote の vocab id を cell に書かないようにした。** text (Tag/Leaf) の値は
`(author_peer, remote_vid) → local_vid` の写像を通してしか意味を持たないのに、 apply 経路
(`Syncer::apply_one` の `Tie` / `TieNamed`) は写像が無いとき **生値をそのまま書いていた**。

vid は author ローカルな番号なので、 生値は受信側の**無関係な文字列**を指す。 実地
(syncretic / mac ↔ Windows) では files table 15962 行のうち 12 行がこの経路で壊れた:

| 壊れた列 | 入っていた値 |
|---|---|
| `path` | 別の行の PK (`"{module_id} {path}"`) |
| `path` | mtime の 13 桁 epoch ms |
| `size` | mtime の 13 桁 epoch ms |
| `key` (PK) | blob の sha256 hex |
| `module_id` | 空文字 |

アプリ側はこれを信じて `outputs/win/ab7e38fa2b56d1f72ebb09f3623a91e7 myapp/kasane/…` という
化けた名前のディレクトリを disk に作っていた。

写像は受信済み `Vocab` op から組み立てる。 上の `.vocabmap` で再起動を跨いで残るようになったが、
`_sync_ops` ring の巻き込みで `Vocab` op 自体を取り逃した場合は依然として欠けうる。 `history_floor` を広告しない
transport では `history_truncated` も立たないので、 欠落は黙って通る。

#### 変更

- `Syncer::apply_one`: `Tie` / `TieNamed` の値翻訳を `try_translate_remote_vid` に変更。
  **未翻訳なら op を適用しない**。
- `SyncOutcome::dropped_vocab`: そうして捨てた op 数 (`ApplyResult::DroppedVocab`)。 `> 0` なら
  当該 cell は**古いまま** (壊れてはいない)。 caller は再 author / bootstrap で埋め直す。
  `dropped_unresolved` と**分けてある** — あちらは未知の foreign entity 宛 `Delete` などで
  正常系でも 0 にならない背景値を持つのに対し、 こちらは**定常 0 であるべき**値なので、
  合算すると警報の閾値が引けなくなる。

黙って壊すより、 書かずに数える方を選んだ。 PK bind 経路 (#141) は 0.17.0 で既に厳密版に
移していたので、 残っていた最後の生値 fallback がこれ。

### 0.19.0 / 0.20.0 の封印 (sync 用途)

**sync に参加する DB では 0.19.0 / 0.20.0 を使わないこと。** 0.21.0 (本 release) へ上げる。

理由は上の 2 つの欠陥そのものではなく、 **露出の仕方**にある。 上の 2 つは 0.19.0 より前から
在る (vocab 写像の永続先はどの版にも無く、 `advance_checkpoint(w.head())` は crate 分割期の
`538b943` / `3b3f38a` から在る)。 だが **v9 (per-cell version) を既定にした `b28446d` = 0.19.0
以降**、 crash 後の壊れ方が 「PK cell だけ落ちた行が残る」 形を取るようになり、 これは

- PK 引きに掛からない → アプリが同じ行をもう一度 insert → **同一 PK の entity が 2 つ**
- `Table::all()` は代表 column (= PK) を tie した entity しか列挙しないので、 **監査からも見えない**

という、 再 author では戻らない壊れ方になる。 実地の chaos soak (2 台 daemon / SIGKILL 混じり /
1800s / 4 seed) では v0.20.0 で PK 欠落行 3 件・PK 重複 2 seed、 本 release で 0 件・0 seed。

0.20.0 は加えて **v8 の既存 DB を writer open で自動的に v9 へ引き上げる** (`d8b287f`) ので、
対象が広い。 0.19.0 は新規作成分だけが v9 になる。

**0.18.3 以前が安全という意味ではない。** 同じ soak で 0.18.3 も 4 seed とも収束せず
(亡霊 57 / 内容不一致 24)、 PK 重複という次元で出なかっただけ。 sync を使うなら 0.21.0 へ。

tag は消していない (既存の pin / lock を壊さないため)。 0.19.0 / 0.20.0 の節にも同じ注意を
書いてある。


## 0.20.0 — 2026-08-21

> **⚠️ sync 用途では封印 (0.21.0 で修正)** — この版は v9 (per-cell version) が既定なので、
> crash 後に 「PK cell だけ落ちた行」 が残り、 同一 PK の entity が 2 つになる経路が露出する。
> 詳細と実測は 0.21.0 の 「0.19.0 / 0.20.0 の封印」 節。 sync に参加する DB は 0.21.0 へ。

**v8 以前の DB を writer open したときに、 自動で v9 領域を生やすようにした。** 手動作業は
不要。 0.19.0 で 「migration 不要」 としていた方針を **明示的に翻す**変更で、 既存 DB の
on-disk 状態が変わるため minor bump。

### なぜ方針を変えたか

0.19.0 の 「migration 不要」 は 「古い DB もそのまま動く」 という意味では正しかった。 だが版数
(= その cell がいつ書かれたか) を持たない DB は:

- LWW の判定材料が無いので **#154 / #160 の巻き戻りを抱えたまま**
- **anti-entropy (Phase 2) が効かない** — digest に載せる HLC が無い

つまり 「新機能の恩恵を受けられない DB が永久に残る」 構図だった。 DB としてデータを持って
いけないのは筋が悪いので、 自動移行に切り替えた。

### なぜ自動でよいか — 移行がほぼタダだから

request17 step 1 で **v9 領域を variable cluster の末尾に置き、 手前の region を 1 byte も
動かさない**設計にしてあった。 `cell_version` の真偽で変わるのは末尾に付く領域だけなので、
移行は:

1. ファイルを新しい `total_size` まで `ftruncate` で伸ばす
2. `H_CELL_VERSION = 1` を書いて header CRC を貼り直す

**データの移動が一切無い。** 100 GB の DB でもミリ秒。 version column の header 初期化すら
不要 (`ver_column_from_region` が lazy に `Column::init` する)。 #123 の vocab index migration
(`VIX2` 検出 → in-place、 「手動作業は不要」) と同じ方針。

### ⚠️ 移行しただけでは過去の巻き戻りは直らない

移行直後は **全 cell の版数が ZERO (= 版数不明)**。 A-1 の定義どおり 「不明 = 現状維持 =
何でも受け入れる」 なので、 各 cell が一度書かれて初めて版数が入り、 そこから守られる。

**移行は 「修正を有効化する」 ものであって 「過去に遡って直す」 ものではない。**
誤読されると危ないので test で明示的に固定してある
(`migration_alone_does_not_retroactively_protect_existing_cells`)。

**例外は削除の記録**: `.eidmap` sidecar が foreign entity の tombstone HLC を既に永続化して
おり、 その読み込みが `set_tombstone_local` を通るので、 生えたばかりの tombstone column に
自動で載る。 版数と違って移行前の情報が残っている唯一の軸なので、 ここだけは移行直後から
効く。

### ⚠️ apparent size が ~3.6 倍になる

開いた瞬間にファイルが伸びる (既定 capacity で 24 GB → 85 GB 相当)。 **物理消費は変わらない**
(sparse) が:

- `ls -l` の数字は跳ねる
- apparent で数えるバックアップツール (`--sparse` 無しの `rsync` / Time Machine) には効く
- **#167 (ディスク満杯 = SIGBUS) と組み合わさると危険**。 伸ばせなかった場合は warn を出して
  v8 のまま開くが、 **伸ばせた後に書き込みで埋まると SIGBUS**

0.19.0 の README に書いた 「空きは apparent size ぶんを見込むこと」 がそのまま効く。
DB を copy する必要がある場合は `enchudb_engine::copy_sparse` を使うこと。

### 実装ノート

- **mmap を張る前**に実行する (ファイルを伸ばすので、 先に map すると古いサイズで固定される)
- **先にファイルを伸ばしてから flag を立てる**。 逆順だと 「flag だけ立って領域が無い」 DB が
  crash で残り、 次の open が file 末尾の外を触る。 伸ばすだけなら中断しても 「末尾に穴が
  増えた v8 DB」 にしかならず無害
- **readonly open は移行しない** (共有 mmap を書かない契約)。 readonly consumer から見える
  DB は writer が一度開くまで v8 のまま
- header CRC / field sanity が通らない DB は移行せず素通しする (ここで新しい失敗モードを
  作らない)。 移行自体が失敗しても warn を出して v8 のまま開く

### Changed

- `pre_v9_db_opens_and_behaves_as_before` → `pre_v9_db_opens_and_migrates_without_touching_the_data`
  に書き直し。 A-1 は **cell の粒度では維持**されている (移行しても既存 cell の版数は ZERO の
  ままで古い record を弾かない)。 変わったのは 「領域を生やすかどうか」 だけ

### 検証

回帰テスト 7 本。 falsify 2 通り:

| 無効化したもの | 結果 |
|---|---|
| migration 呼び | 3 本 FAILED |
| ファイルを伸ばす処理 (= flag だけ立てる) | 4 本 FAILED — **open 自体が失敗** (= 順序の担保が効いている) |

5 crate 全 green (exit 0 / 85 binary)、 clippy 新規指摘なし、 Windows target の cfg エラーなし。

## 0.19.0 — 2026-08-17

> **⚠️ sync 用途では封印 (0.21.0 で修正)** — この版は v9 (per-cell version) が既定なので、
> crash 後に 「PK cell だけ落ちた行」 が残り、 同一 PK の entity が 2 つになる経路が露出する。
> 詳細と実測は 0.21.0 の 「0.19.0 / 0.20.0 の封印」 節。 sync に参加する DB は 0.21.0 へ。

**LWW の真実を、 配送バッファ依存の揮発 HashMap から storage へ移した (request17 Phase 1)。**
削除や上書きの版数が per-cell の column として本体に載るようになり、 reopen しても
配送履歴が reclaim されても失われない。 判定は `set_cell` 1 本に集約したので、 呼び忘れで
黙って壊れる構造も無くなった。 **on-disk format は v8 → v9**、 migration 不要だが
**version stamp は一方通行** (下記)。

### ⚠️ Migration — 先に読むこと

**v8 以前の DB を 0.19.0 の writer で open すると、 version stamp が 9 に上がり、
0.18.x 以前の binary では開けなくなる。** layout そのものは 1 byte も変わらない
(v9 領域は header flag で gate されている) ので migration 作業は不要だが、 **戻れない**。

- **consumer を全部 rebuild してから本番 DB に触ること** (opyula / oboro / sinfo /
  sinfohub / sunsu / bisquit)。 readonly consumer も含む
- 試すときは本番 DB を直接開かず、 `Engine::snapshot_export` か
  `enchudb_engine::copy_sparse` で隔離コピーを取ってから
- 既存の v8 DB は **v9 領域を持たない**ままなので、 per-cell 版数の恩恵は
  作り直すまで得られない (A-1: 「版数不明 = 現状維持」)。 壊れはしない

`.eidmap` sidecar も v2 → v3。 reader は v1 / v2 / v3 すべて読める。

### Added

- **per-cell version column + tombstone column (file format v9)** — 各 cell の HLC を
  生 16B で eid 空間の column に持つ。 variable cluster の末尾に置いたので v8 以前の
  領域は 1 byte も動かない
- `Engine::set_cell` / `clear_cell` / `set_tombstone` / `cell_hlc` / `tombstone_hlc` /
  `has_cell_version` — 値と版数を不可分に書く API
- `OpLog::append_with_hlc` / `mint_hlc` / `append_at_hlc` / `append_many_with_hlcs` /
  `observe_hlc` — HLC の採番責務を呼び出し側に開いた
- **`enchudb_engine::copy_sparse`** — 穴を維持したままファイルを copy する
  (`SEEK_DATA` / `SEEK_HOLE`)。 バックアップを自前で取る場合はこれを使う

### Fixed — ローカル write が LWW に参加しない (#154 / #160)

版数を記録するのが受信経路だけだったので、 ローカルで書いた値には版数が付かず、
古い remote record に負けて巻き戻ることがあった。 ローカル write (同期 / async /
WAL replay) も版数を書くようにし、 判定を engine の `set_cell` 内側 1 本に集約した。
再現テスト `local_write_lww_gap.rs` は `#[ignore]` を外して green。

**この修正は v9 を有効化する前から効く** — 版数の置き場を v9 column と pre-v9 の揮発
`HlcStore` に振り分ける形にしたので、 既存 DB でもローカル write の穴は閉じる。

### Fixed — tombstone が reopen + reclaim で消える (#140 の一部)

削除の版数が揮発 `HlcStore` にしか無く、 その再構築が配送バッファの walk に依存して
いた。 「再起動」 + 「配送バッファが reclaim 済み」 が揃うと tombstone が消え、 古い Tie の
再配送で削除済み entity が復活していた。 v9 で tombstone を column に永続化。

**#140 の本体 (cursor 喪失 / 履歴 reclaim 後に差分で追いつけない) は未解決**で、
anti-entropy (Phase 2) 待ち。 今回閉じたのは 「tombstone が消える」 側だけ。

### Fixed — 再利用 slot が前の住人の状態を引き継ぐ (#166)

版数 / tombstone / 翻訳写像はすべて **local slot** で index されるのに、 slot は削除後に
free list へ戻って別の entity に払い出される。 3 つとも引き継がれていた。

- **版数 / tombstone** — 払い出し口 2 つ (`entity_in` の再利用枝 / `EntitySet` の free
  stack) で落とす。 v9 の column と pre-v9 の `HlcStore` の両方
- **翻訳写像 (#166)** — `EidTranslator::remove_local` を追加。 これが無いと **削除済み
  foreign entity 宛の record が、 slot を引き継いだ無関係な entity に書き込まれる**
  (silent な cross-entity 破壊)。 master から続いていた穴で、 request17 とは独立
- 写像を消すだけだと 「破壊」 が 「削除済み entity の復活」 に化けるので、 削除版数を
  `(peer, foreign_local) -> Hlc` へ退避し、 同じ identity に新しい slot を払い出すときに
  書き戻す。 **tombstone を slot の寿命から切り離した** (`.eidmap` v3)

`EidTranslator::get_or_insert_with` が写像の write lock を保持したまま alloc を呼ぶ
設計だったので、 上記の経路で self-deadlock する。 直列化を専用 lock に移した。

### Fixed — Linux で snapshot が apparent size 全量を物理化する

`std::fs::copy` は macOS では穴を維持する (clonefile) が、 **Linux では 0 で埋めて実際に
書き出す**。 公開 API の `Engine::snapshot_export` がこれを使っていたため、 既定 capacity の
DB の snapshot が Linux で **24 GB の実書き込み**になっていた (実測: 8 GB の穴だけファイルで
macOS 0.0003 秒 / 0 MB に対し Linux 8.09 秒 / 8192 MB)。

`copy_sparse` に差し替え。 CI (ubuntu) が `No space left on device` で runner ごと落ちて
いたのも同じ原因だった (`/tmp` ピーク 20 GB 使い切り → **110 MB**)。

### Changed — 性能

`examples/write_ceiling_bench` (1M ties、 drain M/s):

| writers | 0.18.3 | 0.19.0 |
|---|---|---|
| 1 | 7.8 | **5.1** |
| 2 | 4.3 | 5.5 |
| 4 | 3.2 | **5.0** |
| 8 | 2.5 | **4.3** |

増減はすべて HLC 採番の直列化 lock に由来する。 producer を直列化した副作用で
ArrayQueue の CAS 競合が減り multi-writer が伸びる一方、 **単一 writer は −35%**。
採番順の保証 (= transport が HLC 順に並べ替えるため、 崩すと依存 record が受信側で
逆転する) を取って lock を残している。 取り戻す案は Phase 2/3 で再検討。

### Changed — 容量

既定 create (max_entities 16M / max_himos 256) の **apparent** size が 23.8 GB → 85.1 GB。
sparse なので **物理消費は不変** (数百 KB) だが、 apparent で数えるツールには効く。
気になる用途は `create_growable_*` か、 小さい `max_entities` を使うこと。

### Known limitation — ディスク満杯が SIGBUS になる (#167)

書き込みは `mmap` 経由なので、 穴に block を割り当てられない (`ENOSPC`) 時に errno を
返す先が無く **SIGBUS でプロセスごと落ちる**。 `Result` で受けられない。 `create` は
`set_len` するだけなので作成時点では必ず成功し、 落ちるのは後で書いた時。

**空きは apparent size ぶんを見込むこと。** 「`df` に空きがある」 は安全を意味しない。
0.19.0 では README と `enchudb-engine` の module doc に注意書きを入れただけで、
signal をエラーに変える対応は未着手。

### 積み残し (Phase 2 / 3)

- **#140 の本体** — cursor 喪失 / 履歴 reclaim 後の追いつけなさは anti-entropy 待ち
- 検知系 (`history_floor` / `sync_history_reclaimed` / `SyncOutcome::history_truncated`)
  の撤去
- `mmap_ahead_of_wal_silent_sync_loss` / `replica_syncs_from_origin_via_syncer` は
  `#[ignore]` のまま

## 0.18.3 — 2026-08-14

**0.18.2 の Known limitation (#149) の根治と、 それに伴って露出した sync 事故 2 件の修正。**
ack を呼ぶ主体がいない relay / gateway 経路でも reclaim が回るようになり、 WAL 満杯で
自己修復不能になる経路と、 reopen で LWW 記憶が消えて行が巻き戻る経路を塞いだ。
on-disk format は v8 のまま不変、 公開 API は追加のみ、 migration 不要。

### Fixed — ack が来ない経路で watermark が 0 固定になり ring が永久に空かない (#149)

0.18.2 の Known limitation (「ack が一切来ない構成では WAL が畳まれず full に至る」) の根治。

- `Engine::ack_sync_up_to_hlc` (新規 public): pull cursor (HLC) を consumed_lsn に写す。
  pull 済み = 到達証明なので、 ack エンドポイントを持たない relay / gateway 経路でも
  reclaim が回る
- `sync_watermark` から**自分自身の peer row を除外**。 自 peer の古い ack 残骸が
  watermark を固定し、 reclaim が一度も回らない状態を作っていた
- ack するのは**実在を確認した生存 row の lsn** であって bridge 先端
  (`current_sync_lsn`) ではない。 先端まで ack すると、 生存 row の snapshot を取った
  後に bridge が append した record — cursor より新しい = **まだ pull されていない
  record** — まで「消化済み」と記録され、 `reclaim_sync_ops` が peer に届く前に
  回収してしまう (失うと再著者でしか復旧しない)。 成立条件は「相手が追いついている
  状態で ack した最中に append が入る」 = 追いついた直後に burst が始まる瞬間

実測: 過去の誤 ack 残骸 (consumed_lsn=25099) で固定されていた store で 24,575 行を解放、
バックログ 2.4 万 record の bridge と blob 転送が再開。

**検証**: `crates/enchudb-engine/tests/ack_by_hlc.rs` に回帰 2 本 (写像の境界 = 中間 /
先端 / 証明なし、 および並行 bridge 下で未 pull record を ack しないこと)。 後者は engine の
consumer が 1ms 周期 + 実測数十 ms 遅れでまとめて流すため素の並行 write では窓を踏めず、
生存 row 5000 (snapshot 走査を ms オーダーにする) + `transfer_oplog_to_sync_ops` の直接
駆動で窓に当てている。 先端 ack に戻すと `acked=5004` (cursor より新しい record を ack)
で FAIL することを確認済み。

### Fixed — WAL 満杯で commit group の tail が孤児化し自己修復不能になる

commit group の途中で WAL が「Commit 1 個すら append できない」満杯に達すると、 閉じの
Commit が書けず tail が永久に未 commit のまま残る。 `committed_end < head` が固定されて
`wal_fold_safe` が恒久 false になり、 fold 不能 → 以後の append 全滅 (consumer が無音 drop)
→ 新規変更が sync から永久欠落 → reopen のたび旧 backlog だけを全量再 bridge、 という
自己修復不能の brick になっていた (実運用で発現: ring 満杯の backpressure 中に大量登録
burst が WAL を埋め切った)。

- `OpLog::append_dead()` (最小 record も入らない満杯か) / `free_bytes()` (新規 public)
- `wal_fold_safe` に「append_dead かつ committed 読み残しなし」の例外を追加。 その tail は
  今後 commit され得ず recovery からも sync からも不可視なので畳んでよい。 余裕のある
  書きかけ group の保護 (畳まない) は従来どおり
- consumer の WAL append 失敗を warn-once で可視化 (無音 drop が今回の障害を数時間
  観測不能にした) + 失敗 batch を append 数に計上しない

**検証**: `crates/enchudb-engine/tests/wal_full_fold.rs` に回帰 3 本。 fold 例外を外すと
恒久ブロックの assert で FAIL することを確認済み。

### Fixed — hydrate が WAL fold 済み record を見ず reopen で LWW 記憶が消える (#154)

`HlcStore` (LWW の記憶) は in-memory で、 `Syncer::new` の `hydrate_hlc_store` は engine WAL
しか歩かない。 #150 の fold ゲート以降「bridge 済み record を WAL から fold する」のは正当な
挙動になったが、 fold された record の HLC が reopen 後の hydrate で復元されない。 その状態で
cursor を持たない caller が `Hlc::ZERO` から pull すると、 相手 ring の陳腐 record が「未知」と
判定されて再 apply され、 **ローカルのより新しい行が古い値へ巻き戻る** (tombstone の記憶も
消えるので削除済み entity の復活もあり得る)。 実機発現。

- `hydrate_hlc_store` が WAL に加えて `_sync_ops` (bridge 先、 永続) も歩く。 fold で WAL から
  消えた record の HLC はそこに残っている
- eid は必ず `resolve_remote_eid_existing` を通す。 `_sync_ops` の record は逆写像で元 owner の
  世界番号に宛名が書き戻されている (request10 / #76) ため、 生の eid を key にすると apply 側
  (= local eid で lookup) と一致せず、 hydrate したのに LWW が効かない silent な取りこぼしになる
- 2 source を merge するので `force_set` → `try_set` (monotonic max) に変更

**検証**: `crates/enchudb-sync/tests/issue154_hydrate_after_fold.rs` に回帰 1 本。
`_sync_ops` 走査を外した場合と eid 翻訳を外した場合の**両方で FAIL する**ことを確認済み。

**既知の残ギャップ**: `_sync_ops` の row は ack 後に reclaim されるため、 reclaim 済み record の
HLC は依然どこにも残らない。 完全な健全性には `HlcStore` 自体の永続化 (hlc_store.rs doc の
"Phase D") が要る。 本 release は #154 の主経路 (fold 済み record) を塞ぐところまで。

なお `ack_sync_up_to_hlc` の HLC → lsn 変換は、 **`_sync_ops` 内で lsn と HLC が単調**
(自 WAL の bridge 順) であることを前提にしている。 gossip の relayed append で foreign HLC が
混ざる構成では降順走査が早期に打ち切られ、 安全側 (小さめ) の lsn に落ちる — 過剰 reclaim は
しない。

### Docs

- README を日本語から英語へ全面書き換え (内容・構成は据え置き、 訳のみ)

## 0.18.2 — 2026-08-11

**reopen した store で oplog→sync bridge が恒久停止する事故の修正 (#150)。** relay 型経路
(ack 無し) で `_sync_ops` ring を一周以上使った store を**再起動**すると、 以後の全変更が
sync から無言で欠落していた。 on-disk format は v8 のまま不変、 公開 API は追加のみ、
migration 不要。

### Fixed — reclaim 済み slot が reopen で失われ bridge が全停止する (#150)

**症状**: ring を使い切った store をプロセス再起動すると、 以後のローカル変更が一切
配布されない。 `publish` は毎周回「配った」と報告し続けるため、 外からは正常に見える。

**原因** (独立した 3 欠陥の合成):

1. `free_locals` (reclaim で空けた slot の reservoir) が in-memory のみで reopen で消える。
   `next_local` は sidecar で永続化されて range 端に居るため、 reopen 後の
   `entity_in("_sync_ops")` は**穴だらけなのに恒久 Err** になる
2. `transfer_oplog_to_sync_ops` が満杯時に cursor を `committed_end` へ進めて record を
   破棄していたため、 1 の状態では**全 batch が毎回丸ごと破棄**される
3. 0.18.1 で WAL fold のゲートが「sync は `_sync_ops` 経由で ring を直接読まない」という
   誤った前提で撤去されていた。 bridge (`transfer_oplog_to_sync_ops`) 自体が ring の
   reader なので、 fold が未 bridge 領域ごと畳んで record の現物を消していた

**修正**:

- `entity_in`: 枯渇時に EntitySet の liveness から `free_locals` を一度再構築する self-heal。
  既に毒された store も次の書き込みで自動修復される
- `transfer_oplog_to_sync_ops`: 満杯時は cursor を**進めず** retry (backpressure)。
  rate-limited warn で可視化
- `Engine::wal_fold_safe()` (新規 public): bridge 未読領域が残る間は `try_reset` しない

**検証**: `crates/enchudb-engine/tests/sync_ops_freelist_reopen.rs` に回帰 2 本。 いずれも
修正を無効化すると FAIL することを確認済み (self-heal 無効化で `lsn 637 → 637` 凍結、
backpressure 無効化で「ring を空けても待機 record が bridge されない」)。

### Fixed — 満杯 backpressure が backlog > ring 容量で進行不能になる (#152)

上記の backpressure を「cursor を一切進めない retry」で実装すると、 未転送 backlog が
`_sync_ops` の ring 容量を超えたときに**永久に前進しない**。 毎周回「先頭 K 件を挿入 →
K+1 件目で満杯 → cursor 据置」を繰り返すだけで、 K+1 件目に到達しない。 `next_sync_lsn` は
挿入のたびに増えるので外形上は「毎周 K 件配っている」= 正常に見えるのが厄介。
実測 (ring 508 / backlog 1281) で 12 周回しても末尾 marker は一度も bridge されなかった。

**修正**: 処理し切った record の**終端 offset まで cursor を進める** (partial advance)。
各 record はちょうど 1 回だけ挿入され、 重複も損失も進行不能も無い。 ring が空けば必ず
続きから再開する。

- `OpLog::iter_committed_from_with_offsets` (新規 public): `(Record, その record の終端
  offset)` の組を返す。 group の途中を指す offset から再開しても取りこぼさない
  (`out` に入る record は必ず Commit で閉じられた group の一員なので、 再 scan は残りを
  読んでからその Commit に到達して flush する)
- `transfer_oplog_to_sync_ops`: 挿入した record も skip した record も「処理し切った」
  として cursor を進める。 満杯で打ち切ったときはそこまでを store
- 併せて `count` の off-by-one を修正 (挿入できなかった record を転送数に数えていた)

**検証**: `crates/enchudb-engine/tests/sync_ops_backlog_drain.rs` に回帰 1 本
(backlog ~1280 > ring ~508 で末尾 marker が届くこと)。 partial advance を無効化すると
FAIL することを確認済み。

### Known limitation — ack が一切来ない構成では WAL が畳まれず full に至る (#149)

> **→ 0.18.3 で解消済み** (`Engine::ack_sync_up_to_hlc` により pull cursor を到達証明として
> reclaim が回る)。 以下は 0.18.2 時点の状況。

backpressure の必然として、 **ack を呼ぶ主体がいない構成** (HttpRelay / gateway 越しの
publish / pull のみ) では ring が永久に空かないため bridge が止まり続ける。 その間は
`wal_fold_safe()` が false のままなので WAL も畳まれず、 容量到達で `append` が drop
され始める (rate-limited warn あり、 ローカル store は無傷だが oplog record は消えるため
その変更は恒久的に同期不能)。

実測: ack 無しで書き続けると `wal_fold_safe=false` のまま `WAL full — append dropped`。
一方 **ack が回っていれば `wal_fold_safe=true` を保ち WAL は正常に畳まれる** (20 周回
確認済み) ので、 通常運用に影響は無い。

根治は #149 の「pull 経路を ack として扱う口」。 それまでは publish 成功後に自分の
peer_id で self-ack する回避策 (#149 に記載) を併用すること。

## 0.18.1 — 2026-08-10

**0.17.0 で入った PK bind のリグレッション修正 (#147)。 0.17.0 / 0.18.0 を使っている構成は
即座に上げること** — 別のキー同士が誤って束ねられ、 **既存 row のキーが上書きされて消える**。
engine の on-disk format は v8 のまま不変、 公開 API は追加のみ、 migration 不要。

### Fixed — PK bind が未翻訳の生 vid で lookup して無関係な row へ誤 bind する (#147)

0.17.0 で入れた PK bind pass (#141) のリグレッション。

**症状**: 実機 2 台 (fresh store 同士) で、 片側が新規 row を作ると相手に届かず、 代わりに
**無関係な既存 row が消えて scan が復活させる**「1 written, 1 removed」の恒久チャーン
ループになる。

**原因**: bind pass の lookup が `translate_remote_vid` を使っていたが、 この関数は mapping
未登録時に **author ローカルな生 vid をそのまま返す** fallback を持つ。 新規文字列の
`Vocab` record は同 batch 内でまだ適用されていないため mapping が無く、 生 vid のまま
`query_by_id` していた。 vid は peer ローカルの連番なので、 **fresh store 同士は intern 順が
対称でほぼ必ず数値衝突する** — 発症は事実上必然だった (「flake っぽい」に見えていたのは
vid 割当タイミング依存のため)。

**修正**:

- `Engine::try_translate_remote_vid` (Option 版、 fallback で生値を返さない) と
  `Engine::vocab_id_bytes` を追加
- bind pass は未翻訳なら **同 batch 内の `Vocab` record の bytes → local vocab 照合** に
  切り替える。 bytes ごと未知なら **bind しない** — その PK 文字列を持つ既存 row は
  存在し得ないので、 通常の払い出しが正しい

#141 の本来の収束 (同一キーの独立作成が 1 row になる) は保たれている。

### 検証 (0.18.1)

- `cargo test --workspace --no-fail-fast` (macOS) — **902 passed / 0 failed / 32 ignored**
- CI (ubuntu) — clippy / miri / loom / test すべて success
- **反証済み**: 修正を 0.17.0 相当に戻すと新規回帰テスト
  `different_pk_with_colliding_vid_numbers_stay_separate` が
  「A 自身のキーが 0 row に潰れる」で確実に落ちる
- 下流 syncretic のフルスイート **86 / 0**。 0.17.0 で 46 秒 timeout していた
  `symmetric_sync` が 3 連続 9 秒で安定 pass

### 影響範囲

- **0.17.0 / 0.18.0 のみ**。 PK bind pass は 0.17.0 で導入したので、 0.16.1 以前は
  この誤 bind 自体が起きない
- PK を宣言した table を p2p sync している構成が対象

## 0.18.0 — 2026-08-10

**reclaim で落ちた履歴を黙って部分適用しなくなった (#140 Phase 1)。** engine の on-disk
format は v8 のまま不変、 MSRV も 1.89 据え置き、 migration 不要。 ただし
**`SyncOutcome` に public field 追加** と **`pull_once` の挙動変更**を含むため minor。

**この release だけでは HTTP 経路には効かない。** 下記「残り」を参照。

### Fixed — 差分で追いつけない peer に部分履歴を配っていた (#140 Phase 1)

`_sync_ops` は ring buffer で、 `lsn < sync_watermark()` (= **登録済み**全 peer が
consume した境界) の row は reclaim される。 未登録の新規 peer や長期オフラインの peer は
watermark を押さえないので、 **自分が必要とする履歴が先に落ちる**。 それでも pull は成功
扱いで返っていたため、 peer は不完全な store を持ったまま同期済みだと信じていた。

実測 (`tests/issue140_history_truncation.rs`):

```
A の live state = 1 row
B の pull 結果  = SyncOutcome { received: 1, applied: 0, skipped: 1, ... }
B の最終状態    = 0 row  → エラーも警告も無く「同期成功」
```

**修正**: 配れる履歴の下限を publisher が広告し、 puller は cursor がそれより古ければ
**records を一切適用せず** truncation を通知する。 部分履歴を黙って適用するのをやめ、
Kafka の `OffsetOutOfRange` と同じ「差分では追いつけないので bootstrap し直せ」を明示する
形にした。

| 層 | 変更 |
|---|---|
| engine | `sync_history_reclaimed()` / `min_sync_ops_lsn()` を追加 |
| transport | `set_history_floor` / `history_floor` を trait に追加 (default は no-op / None) |
| sync | `publish_since` が floor を広告、 `pull_once` が `SyncOutcome::history_truncated` を返す |

判定は **`_sync_ops` の内容から導出**できるので新規の永続化は不要。 lsn は 1 始まりなので
「生存 row の最小 lsn > 1」 または 「row 空 + publish 実績あり」 で reclaim 済みと判定できる。
`next_sync_lsn` は open 時に `_sync_ops` / `_sync_peers.consumed_lsn` から rehydrate される
ため、 reopen をまたいでも成立する。

#### #140 本文の経路記述を実測で訂正

再現を作る過程で、 issue 本文の記述が実測と食い違うことが分かった (issue にもコメント済み):

- 本文は「16MiB の oplog リング一周で tombstone が脱落」だが、 consumer thread は
  `try_reset()` の **前に** `transfer_oplog_to_sync_ops()` を流し切るので、 oplog の
  巻き戻しでは失われない。 実際の retention 境界は **`_sync_ops` の reclaim**
- reclaim は `lsn` 昇順 (= 古い順) に落とすので、 素の「作成 → 削除 → reclaim」では
  **作成 record が先に落ちる**。 本文の「作成 record はあるが削除 record が無い」並びには
  素の手順ではならない。 あの並びは #141 のチャーンループ (同一 PK の重複を作り続ける) が
  生成源だった可能性が高く、 0.17.0 の #141 修正で主要な生成源は塞がっている

### Breaking

- `SyncOutcome` に public field `history_truncated: bool` を追加。 `#[non_exhaustive]` では
  ないので **struct literal で構築しているコードは影響を受ける** (`..Default::default()` を
  使っていれば無影響)
- `pull_once` は truncation を検知したとき **records を適用せずに返る**。 従来は部分履歴を
  そのまま適用していた
- `Transport` trait の追加メソッドは default 実装付きなので、 **既存の実装者は無改修**

### Known limitation — 残り (bootstrap-first の Phase 2/3)

1. **HTTP transport への floor 伝搬が未実装**。 `Transport` trait の default が `None` を
   返すため、 **HTTP 経路では検知が働かない**。 下流 bisquit に効かせるにはこれが要る
2. `GET /bootstrap` を初回 full-sync の正規経路に昇格させる Syncer フロー
3. bisquit 側のペアリング経路改修 (`/bootstrap` を未使用、 cursor 0 pull のみ)

### 検証 (0.18.0)

- `cargo test --workspace --no-fail-fast` (macOS) — **901 passed / 0 failed / 32 ignored**
- CI (ubuntu) — clippy / miri / loom / test すべて success
- **反証済み**: `history_floor` の参照を無効化すると新規テストが確実に落ちる。 また
  「この筋書きでは truncation が必ず通知される」ことも明示的に assert している
  (最初に書いた亡霊化テストが vacuous pass だったため)

## 0.17.0 — 2026-08-10

**cross-author sync のデータ破損修正 (#141)。 PK を宣言した table を p2p sync している
構成は上げること。** engine の on-disk format は v8 のまま不変、 MSRV も 1.89 据え置き、
既存の公開 API に変更なし (追加のみ)。 `.tables` sidecar は後方・前方互換のまま拡張した
ので migration 不要 — 依存を更新するだけで上がれる。

public API の追加と sidecar の拡張を含むため、 方針どおり patch ではなく minor。

### Fixed — 同一 PK の entity が cross-author apply で二重払い出しされる (#141)

2 台が**同じ自然キーの row を独立に作って**から相互 sync すると、 同一 PK の entity が
author ごとに二重化していた。 実測 (下流 syncretic) では 1 table 内 2,358 entity 中
**788 個が同一キー文字列の重複**。 二次被害が本体で、 2 つの entity が同じ外部状態を
取り合う**恒久チャーンループ** → 数時間で DB 1.7 GB / oplog リング一周 → #140 の
tombstone 消失 (削除済み entity の亡霊復活) まで連鎖していた。

原因は `Syncer::apply_one` → `Engine::resolve_remote_eid` が `(author, remote_eid)` 写像
だけで解決し、 初見の remote_eid には `alloc_translated_local` で**無条件に新規 eid を
払い出す**こと。 適用先 table に同じ PK の既存 row が居ても束ねない。

根っこは **engine が PK を知らなかった**こと。 PK は schema 層
(`TableBuilder::primary_key`) の概念で、 `enchudb-sync` と `enchudb-schema` は兄弟 crate
なので apply 側から見えない。

**修正**: PK を engine へ降ろし、 apply 前に PK で既存 entity へ束ねる。

| 層 | 変更 |
|---|---|
| engine | `TableDef.pk_himo` + `set_table_pk` / `table_pk_himo` / `is_pk_himo` / `bind_remote_eid` を追加 |
| schema | `TableBuilder::build()` が `primary_key()` 指定時に PK himo を engine へ降ろす |
| sync | `apply_records` 冒頭に **PK bind pass** — batch 内の PK himo Tie を先に走査し、 その PK 値を既に持つ local entity が居れば写像をそこへ固定 |

以降 `resolve_remote_eid` は払い出さずその entity を返すので、 LWW が himo 単位で通常
どおり効いて 1 row に収束する。

**`.tables` sidecar は version 1 のまま**。 PK は全 table を書き切った後ろの optional
trailer (`PKS1` block) として永続化する。 table のデコードは `table_count` 回で終わって
残りを読まないので、 **0.17.0 以前のバイナリはこの block を無視して従来どおり開ける**
(PK が落ちて PK-aware apply が効かなくなるだけ)。 version を 2 に上げると旧バイナリが
`unsupported version` で開けなくなり、 同じ DB を旧 enchudb で開く別プロセスを巻き込む
ため採らなかった。 PK を持たない DB では出力が 1 byte も変わらない。

#### Known limitation — 本 release だけでは直りきらない

- bind できるのは **PK Tie が同じ batch に含まれる場合のみ**。 1 row の insert は
  1 commit = 同 batch なので実運用の主経路は塞がるが、 PK Tie と非 PK Tie が別 batch に
  分かれ非 PK 側が先に届くと束ね損なう
- **既に二重化した store の修復パスは未実装**。 上げても既存の重複は解消しないので、
  修復ツールか apply 時の遅延統合が別途要る (#141 に残作業として記載)

### Fixed — feature gate で 3 ヶ月死んでいた integration test 20 file を復活 (#138)

`tests/` 29 file 中 **20 file** が存在しない feature (`v27` / `v32` / `v33`) の
`#![cfg(...)]` で丸ごと無効化されており、 2026-05-12 の `1f37678`「v## feature flag を
全廃」以降**コンパイルすらされていなかった** (src/ の 223 cfg site は inline 化されたが
`tests/` が対象外だった)。 #138 は 1 file の話として起票されていたが実スコープは 20 file。

**workspace: 729 passed → 900 passed / 0 failed / 32 ignored。**

とくに **HTTP transport の E2E 4 本が実際に走るようになった** — 0.16.1 で直した #137
(relay の途中切断) が長期間見つからなかった一因。

復活の過程で、 sync 系テストの前提が 3 つ変わっていたことが判明した (いずれも
0.8.0〜β-light の意図的な仕様変更):

- **named table 必須** — `enable_sync_tables()` が `define_table` を呼ぶため anonymous
  table が閉じ `entity()` が panic。 さらに anonymous のままだと受信 op の foreign eid を
  確保する先が無く apply が丸ごと skip される (#9)
- **cross-peer read は翻訳経由** (#9) — 送信側 eid のままでは読めない
- **`_sync_ops` への転送** (0.8.0) — background 転送待ちだと `publish_since` が空振りする

現行仕様と食い違っていた 3 test は、 assert を黙って緩めず機構を doc comment に書いて
`#[ignore]` で可視化した (replica mode と #9 翻訳の非互換 / oplog ring は session を
またぐ audit log ではない件)。

### Fixed — テストの timing 依存を除去

`issue9_foreign_eid_collision` が `_sync_ops` への非同期 bridge を 300 ms の sleep で
待っており、 復活した 168 test で並列実行の負荷が上がった結果 `publish_since` が 0 件に
なって落ちるようになった。 sleep 4 箇所を決定的な `transfer_oplog_to_sync_ops()` に
置換。 同種の sleep 依存は他 file にも残っている (別途対応)。

### 検証 (0.17.0)

- `cargo test --workspace --no-fail-fast` (macOS) — **900 passed / 0 failed / 32 ignored**
  (0.16.1 の 729 + 復活 168 + #141 新規 3)
- CI (ubuntu) — clippy / miri / loom / test すべて success
- #141 の修正は **反証済み**: PK bind pass を無効化すると最小再現が確実に 2 row で落ちる

## 0.16.1 — 2026-08-09

**HTTP relay 経由の sync が数 MB 規模で無音のまま失敗する問題の修正 (#137)。**
`enchudb-transport` の `HttpRelay` を使う構成 (特に macOS / Windows) は上げること。
engine の on-disk format は不変 (v8 のまま)、公開 API 変更なし、MSRV も 1.89 のまま、
migration 不要 — 依存を更新するだけで上がれる。

### Fixed — HTTP relay の大きな応答が途中切断される (#137)

`HttpRelay` の応答が socket 送信バッファ (loopback で数百 KB) を超えると、
**Content-Length より短い body のまま接続が閉じる**。読み手が遅いほど確実に起きる。

下流 syncretic (Mac ↔ Windows folder sync) では、片端の WAL が 4.7 MB に育った時点で
相手の「cursor 0 からの初回フル pull」が**ほぼ毎回** (実測 10 回中 8 回) 途中切断され、
pull クライアントが失敗を空 batch に落とすため **cursor が永遠に 0 のまま 1 件も同期
しない**という形で発現した。エラーログも出ないので無音の停止に見える。

原因は `start_inner` が accept 検出のため listener を `set_nonblocking(true)` にして
いること。**BSD/macOS と Windows では accept した接続 socket がこのフラグを継承する**
(Linux は継承しない)。そのまま `write_all` すると送信バッファが埋まった時点で
`WouldBlock` がエラー扱いになり、`?` でハンドラが即死して接続が閉じる。

**修正**: `handle_connection` 冒頭で `stream.set_nonblocking(false)` に戻す。read 側の
ハング防止は既存の `set_read_timeout` (10s) / `set_write_timeout` (30s) がそのまま担う。
`ws.rs` は accept 後に同じ対処を済ませており、**`http.rs` だけ取り残されていた**。

**影響範囲**: `HttpRelay` を使う全バージョン。Linux では accept が O_NONBLOCK を継承
しないため発生しない (裏を返すと、この回帰テストは Linux では fix を戻しても通る)。

回帰テストは `crates/enchudb-transport/tests/relay_large_response.rs` — 8 MB の record を
relay に積み、「request 送信後 500 ms 読まない遅い読み手」で `/pull` を受けて、body の
実長 == Content-Length を assert する。修正を戻すと確実に落ちる (切断時 327 KB / 8.4 MB)。

### Known limitation — CI がこの回帰を守れていない

`.github/workflows/ci.yml` の test job は `enchudb-oplog` / `-engine` / `-schema` /
`-sql` / `-sync` の 5 crate しか回しておらず、**`enchudb-transport` は CI 対象外**。
加えて runner が `ubuntu-latest` 単独のため、対象に加えても本件は Linux では再現せず
ガードにならない。CI の crate 追加と macOS runner の追加は別途対応する。

同様に、`tests/` 直下の **11 ファイルが存在しない feature `v32` の gate で丸ごと無効化
されている** (`v32_http_transport.rs` / `crdt_and_chaos.rs` / `change_listener.rs` /
`shard_routing.rs` ほか)。HTTP transport の E2E が一度も走っていないことが、本件が
生き延びた一因。蘇生は Syncer の現行 API との乖離が大きく、別途対応とした。

### 検証 (0.16.1)

- `cargo test --workspace --no-fail-fast` (macOS) — **729 passed / 0 failed / 27 ignored**
  (0.16.0 の 728 + 新規回帰テスト 1、回帰なし)
- CI (ubuntu) — clippy / miri / loom / test すべて success
- 新テストを単体で 4 連続実行して flake なし

## 0.16.0 — 2026-08-07

**Windows でビルドできるようになった (#133)。** engine の on-disk format は不変
(v8 のまま)、既存の公開 API に変更なし、migration 不要。**MSRV が 1.89 に上がる**ため
minor bump とした。

### Added — Windows ビルド対応 (#133)

unix 依存だった 4 箇所を解消し、`aarch64-pc-windows-gnullvm` で workspace 全体が
ビルドできるようになった (下流 syncretic の Windows 対応の前提)。

| 箇所 | 対応 |
|---|---|
| `growable_map.rs` | `#[cfg(unix)]` 化。非 unix は構築不能な stub |
| `engine.rs` / `oplog.rs` の `libc::flock` × 2 | `std::fs::File::lock` に置換 (依存追加なし) |
| `keys.rs` の `mode(0o600)` | `#[cfg(unix)]` 分岐 |
| `enchudb-oplog` の `libc` | unix 限定の依存に移動 |

- **lock の挙動は不変**。`libc::flock(LOCK_EX)` → `File::lock()`、`LOCK_UN` →
  `unlock()` で、どちらもブロッキング排他。`File::lock` は unix では内部で `flock` を
  使うので `.db.lock` による writer 排他の意味論はそのまま。
- **growable backing は unix 限定になった**。「予約 (`PROT_NONE`) → `MAP_FIXED` で
  貼り直す」手が Windows の `MapViewOfFileEx` では使えないため。ただし growable が
  要るのは create 時だけで (`Engine::open` は growable で作った DB でも常に素の
  `MmapMut` で開き直す)、非 unix では eager create で代替できる。非 unix で
  `create_growable*` を呼ぶと `io::ErrorKind::Unsupported` を返す (コンパイルは通るので
  **実行時**に出る点に注意)。
- `enchudb-schema` に eager 版 `Database::create_with_capacity` を追加。

**MSRV**: `File::lock` の安定化に合わせて **1.89**。各 manifest に `rust-version` を
明記した。`enchudb-ngram` / `enchudb-textsearch` は `File::lock` を使わず edition 2021 の
ままなので、意図的に据え置いている (この 2 crate 単体はより古い toolchain でも使える)。

### Known limitation — NTFS では create が実領域を確保する

Windows の NTFS は既定で sparse file を作らないため、`set_len(total_size)` が
**見かけサイズぶんの実領域を消費する**。`FSCTL_SET_SPARSE` の対応は未実施。

注意すべきは **`max_entities` を下げてもほとんど縮まない**こと — 支配的なのは
entity 比例部分ではなく vocab / content / leaf の**固定既定サイズ**である
(macOS で apparent size を実測):

| 構成 | apparent |
|---|---|
| `create_with_capacity(1_000_000)` | 2968.7 MB |
| `create_with_capacity(100_000)` | 1684.5 MB |
| `create_with_capacity(10_000)` | 1551.6 MB |
| `create_compact()` | 589.3 MB |

固定 region まで絞れば下がる (`Engine::create_full`):

| 構成 | apparent |
|---|---|
| `max_entities=100k` / vocab 4 MB / himos 256 / content 4 MB | 668.5 MB |
| `max_entities=10k` / vocab 1 MB / himos 64 / content 1 MB | 522.2 MB |

→ Windows で作るときは `create_with_capacity` ではなく **`create_full` で固定 region も
絞る**のが当面の指針。それでも 522 MB が下限なので、実用上は sparse 対応が本命。

### 検証 (0.16.0)

- `cargo test --workspace --no-fail-fast` (macOS) — **728 passed / 0 failed / 27 ignored**
  (0.15.4 と同数、回帰なし)
- `cargo check --workspace --target aarch64-pc-windows-gnullvm` — **error 0**
  (0.15.4 では `std::os::fd` / `libc::flock` / `mode()` / `as_raw_fd` で 10 errors)
- Windows 11 ARM64 実機 (Parallels, build 22621) で DB 作成 → scan → apply → daemon 起動
  → mDNS 広告まで通過 (PR #133 の報告)
- Windows 上での `cargo test` 実行は未検証

## 0.15.4 — 2026-08-07

**silent data corruption の修正を含む。 routed Leaf を read-while-write する構成は
上げること。** engine の on-disk format は不変 (v8 のまま)、 公開 API 変更なし、
migration 不要 — 依存を更新するだけで上がれる。

### Fixed — free した Leaf slot が corrupt payload を返す (#132)

高 churn かつ read/write 並行下で、`get_content_owned` / `get_content` /
`get_text_owned` / `get_text` が稀に **payload を 4 byte (1 word) 手前からスライスした
blob** を返していた。長さは正しいまま先頭 4 byte に gen カウンタが混入し、末尾 4 byte が
欠落する。長さ prefix 付き codec なら decode 失敗で弾けるが、**固定長 codec では silent に
誤値を掴む**。

実測 (wikipulse の torture test): owned ~1/25 万 read、借用 ~1/10 万 read。
`get_*_owned` への移行 (#119 Step 0) で頻度は下がるが根治しない。

原因は `LeafStore::free()` が hole header を **`HAS_GEN` を落とした素の `slot_size`** で
書き、かつ **先頭 4 byte しか上書きしない**こと。gen slot の `len` (+4) と `gen` (+8) が
生き残るため、その offset を掴んだ reader は `try_read` の **legacy 8 byte header 分岐**に
落ち、残った旧 `len` を信じて payload を +12 ではなく **+8 から** copy していた。
bounds check `len > slot_size - 8` は旧 `len` ≤ 旧 `slot_size` − 12 なので必ず通過し、
`ss1 == ss0` も成立するため何にも引っかからず `Ok` で返る。

gen seqlock は **gen 分岐に入って初めて効く**ので、分岐判定自体が誤る本件は seqlock の
外側。`text_owned_by_id` の外側 verify も offset だけを比べる **ABA 比較**で、best-fit
allocator + 同サイズ re-tie の churn では free した slot が即再利用されて column が
`raw → 別 → raw` と戻るためすり抜ける。借用版はこの verify すら無く、実測の約 2.5 倍差と
整合する。

**修正**: hole header を「**gen が odd の 12 byte slot**」として書く。`try_read` の gen
分岐が既に持つ「`g0` が odd なら `Retry`」にそのまま乗るので **reader 側の判定は一行も
増えていない**。`HAS_GEN` が立つので `slot_size_bytes_at` の `!HAS_GEN` マスクが効き
`rebuild_free_list` の walk も無傷。旧 binary も odd gen の扱いを持っているため
**format 互換で、version bump は不要**。

- `free()`: 冒頭で freed offset の gen を odd に publish。coalesce で merged header が
  前方へ移っても、末尾で high_water が後退して header を書かない経路でも弾かれる
- `alloc_locked()`: split remainder も同じ header で書く。印を書けない余りを作らないよう
  split 閾値を 8 byte → 12 byte に引き上げ。walk の assert が使う下限は 8 byte のまま
  (上げると payload 長 0 の legacy slot を持つ v6/v7 DB が開けなくなる)
- `flush_run()`: open ごとに全 hole の先頭が塗り直されるので、**旧 binary が書いた既存 DB
  の hole も初回 open で self-heal** する
- legacy 8 byte slot 由来の 12 byte 未満 hole だけは gen を置けないが、payload 開始位置が
  reader の legacy 分岐と一致する (+8) ため誤読は起きない

**影響範囲**: 0.14.0 (#106 で gen slot 導入) 〜 0.15.3。writer が quiesce している構成
(readonly open、バッチ後の読み出し) では `free` が走らないので発生しない。

回帰テストは **並行不要の決定的テスト 4 本** (churn に依存しないので flaky にならない):
freed slot / coalesce で header が前方へ移った場合 / split remainder が旧 gen slot の
先頭から始まる場合 / free 後の再利用が正しく読めること。fix を無効化すると前 3 本が
落ちることを確認済み。

### Removed

- repo 内の旧 LP (`lp/index.html`、2026-05-13 が最終更新、どこからも未参照) を削除。

### 検証 (0.15.4)

`cargo test --workspace --no-fail-fast` — **728 passed / 0 failed / 27 ignored**
(0.15.3 時点 724、差分は #132 の回帰テスト 4 本)。

## 0.15.3 — 2026-08-06

**ライブラリコードの変更なし** (engine on-disk format v8 不変、 公開 API 不変)。
packaging / CI のみの chore release。 依存を更新するだけで上がれる。

### Fixed — workspace が repo 外の `../sabitori` に path 依存していた (#124)

clean checkout (CI / コンテナ / 外部 contributor / crates.io 公開) では
**manifest 読み込み段階で** workspace 全体が解決できず、 `cargo test --workspace` が
`failed to read /sabitori/crates/sabitori/Cargo.toml` で落ちていた。 ローカルの
`~/myapp` レイアウトでしか workspace build が通らない状態。

```
error: failed to load manifest for workspace member `/repo/.`
Caused by: failed to load manifest for dependency `enchudb-transport`
Caused by: failed to load manifest for dependency `sabitori`
```

`sabitori` (自作 GPU GUI framework) を使っていたのは
`enchudb-transport/examples/dist_dashboard.rs` の 1 本だけ — origin + replica × 3 を
1 プロセスに同居させて 4 分割ビューで可視化する分散デモ (1030 行、 実質休眠)。
**example ごと削除**した。 `[dev-dependencies]` に置くだけでは直らない (path 解決は
dev-dep でも manifest 段階で走る) こと、 repo 外の font asset を
`include_bytes!("../../../../sabitori/assets/...")` で直参照していて feature gate では
path が残ること、 の 2 点から「隠す」ではなく「出す」を選んだ。 デモを残すなら依存の
向きが自然な sabitori 側 repo へ引っ越すのが筋 (git 履歴からはいつでも復元できる)。

- root `Cargo.toml` にも同じ dev-deps 3 行があったが、 root には使用箇所が無く
  **dead だった** (「bench 専用」 の注記に反して) ので併せて撤去。
- **CI の回避ハックを撤去**: 全 4 job (test / miri / loom / clippy) が
  `find . -name Cargo.toml -exec sed -i '/sabitori/d' {} +` で manifest を書き換えてから
  検証していた。 CI が素の repo をそのまま検証するようになった。

検証は clean 環境 (sibling に `sabitori` が無い隔離 dir へ repo を複製) で
`cargo metadata` / `cargo check -p enchudb-transport --all-targets` の通過を確認し、
**同環境で sabitori 行を 1 行戻すと manifest 解決が失敗する**ことまで実演している。
`cargo test --workspace --no-fail-fast` は 724 passed / 0 failed / 27 ignored で
0.15.2 と同数 (regression なし)。

## 0.15.2 — 2026-08-01

bug fix のみ。 **engine の on-disk format は不変** (v8 のまま)、 公開 API の追加・
変更なし。 migration 不要。

### Fixed — `where_in` (Predicate::In) が単独使用で常に空を返す

`Table::where_in(col, values)` を他の述語なしで単独で使うと、 対象 row が存在しても
常に 0 件になっていた。 `Query::find` が IN を「候補集合への post-filter (retain)」
としてしか扱っておらず、 IN が唯一の述語のとき base candidates が `query_by_id(&[])`
= 空集合になり、 空集合を retain して常に空だった。 eq_conds が空で IN があるときは
`pull_in_by_id` を候補の seed にするよう修正。

併せて `pull_in_by_idx` の `e as EntityId` (= #32 と同型の **peer prefix 抜け**) を
`make_eid(peer, e)` に修正 — `where_eq(x).where_in(y)` の組合せが peer_id ≠ 0 の
DB で空になる潜在バグも同時に消えている。

- 発見: sunsu home-timeline (fan-out-on-read) を `posts.where_in(author, followees)`
  で引いたら全 user で空。 followee ごとに `where_ref` を N 回呼ぶ回避策 (クエリ N 倍)
  から `where_in` 一括に戻せる。
- regression test: `crates/enchudb-schema/tests/issue12_where_in_standalone.rs` (3) —
  単独 IN / eq × IN intersect / `where_ref` との eid 一致。

### Fixed — readonly open の dirty index shadow が index_cap 比例の anon RSS を占有 (#127)

dirty (clean_flag ≠ 1) / legacy (VIX2) DB の readonly open は index を heap の shadow へ
rebuild するが (#77-H1)、 旧形式は index layout の byte 複製
(`vec![0u8; index_cap × 13B]`)。 確保は calloc (仮想) でも、 **#123 で hash slot が
一様分散になったため rebuild が shadow の全ページに live slot を書いて全ページを
物理化**し、 readonly open 1 回ごとに index_cap 比例の anon RSS が Engine 寿命の間
残っていた。 1 GB VPS で同一 DB を複数 layer が readonly open する構成 (naruhodo) の
boot +~300 MB / storm OOM の有力因。

shadow を `(fxhash(value), vid)` sorted の **count 比例 compact 形式**に変更:

| readonly open (max_entities=1M 実測) | 0.15.1 | 0.15.2 |
|---|---|---|
| clean | +0.05 MB | +0.05 MB (不変) |
| dirty, vocab_max_entries=4M | **+52.05 MB/open** | +0.06 MB |
| dirty, 既定式 (16M entries) | **+208.05 MB/open** | +0.06 MB |

- lookup は hash の binary search + 同 hash 区間の値比較。 「先着 vid が勝つ」 dup
  解決は旧 probe と同一。
- 計測 harness: `cargo run --release -p enchudb-engine --example issue127_open_heap`
- regression test: `tests/issue127_readonly_shadow_compact.rs` — counting allocator で
  dirty readonly open の heap 増分 < 4 MB を固定 (旧実装は 54.58 MB で fail)。
- #127 の実環境確認 (1 GB cgroup での anon 比較) は 0.15.2 適用後に要実測 —
  serving DB が dirty かは `vocab_index_rebuilt_on_load()` で判定できる。

### Fixed — Leaf read retry が並列 contention 下で silent None (#128)

`get_text_owned` / `get_content_owned` の retry は一律 256 回で give-up して None を
返していたが、 単一 cell を止めどなく re-tie する writer と CPU contention が重なると
writer loop と位相が噛み合ったまま (resonance) 256 連敗し、 **値が存在するのに
silent None** を返すことがあった (issue119 torture test の並列実行で実測 10/30 run fail)。

「進捗の無い連敗」 だけを数える方式に変更:

- column offset か slot stamp (gen/ss、 内部 `LeafStore::slot_stamp` 新設) が動いて
  いる間は writer 前進中 = 値は存在するので諦めない (seqlock reader と同じ契約)
- 同じ (raw, stamp) のまま 256 連敗 (crash 残骸の odd gen / 恒久 stale / 破損) の
  ときだけ None — escape は維持し hang しない (odd gen を汚す unit test で固定)
- yield だけでは位相が崩れないことがあるので µs sleep の階段 backoff (≤ 100 µs) を追加

issue119 binary 並列実行 30 回: **10 fail → 0 fail**。

### Tests

- issue7 (seal_integrity 後の himo append で `.crc` 追従漏れ) の reproducer を assert
  付きに強化して `#[ignore]` 撤去 — fix 自体は v29 で入っていたが test が置き去り
  だった。 stale `.crc` の unlink / reopen 成功 / 再 seal での regenerate を固定。

### 検証 (0.15.2)

- `cargo test --workspace --no-fail-fast` — **724 passed / 0 failed / 27 ignored**
  (0.15.1 時点 718)。 issue119 系 flaky の根治込み (上記 30 run 検証)。
- 各 fix とも「fix を無効化すると該当 test が fail する」 falsify を実施済み
  (where_in: 2 test fail / #127: 54.58 MB 確保で fail / #128: 10/30 run fail)。

## 0.15.1 — 2026-07-31

`enchudb-ngram` / `enchudb-textsearch` のみの変更。 **engine の on-disk format は不変**
(v8 のまま)。 既定設定 (`n = 2`) での `.etxt` 出力は #121 以前と**バイト等価**なので、
既存の索引・既存の生成パイプライン・外部 reader は一切影響を受けない。

### Added — n-gram の n を可変化 (#121)

`enchudb-ngram` は n = 2 固定で、 型・key encoding・file format・API 名まで bigram が
焼き込まれていた。 **最適な n はスクリプトとコーパスに依存する**ので、 これは「trigram の
方が良い」という話ではなく「n を選べないこと自体が gap」という話。

```rust
let mut idx = NgramIndex::with_n(3)?;   // 2..=4、 既定は 2
idx.save("corpus.etxt")?;

let idx = NgramIndex::open("corpus.etxt")?;
assert_eq!(idx.n(), 3);                 // caller は n を覚えなくていい
assert_eq!(idx.min_query_len(), 3);     // index で絞れないクエリ長を事前に判定できる
```

n は `.etxt` header に焼かれ `open` で戻るので、 **build と query で n がズレる事故が
構造的に起きない**。 `TextSearch` 側の挙動 (どの長さから `.contains()` 検証が要るか) も
index の n に自動追従する。

- key は `n` 文字を 16bit ずつ詰めた **exact な u64** (hash ではない)。 `n ≤ 4` なら
  衝突ゼロで、 「n 文字ちょうどのクエリは候補がそのまま正確一致 = 検証不要」という
  `textsearch` の最適化が n によらず成立する。 hash 案だと `keys_are_exact()` gate の
  入れ忘れが n ≥ 3 で silent な誤ヒットになる — その地雷ごと消えている。
- file format v3 (Gram Index の entry 12 B → 16 B) を追加。 **v2 は読み書きとも従来どおり**
  で、 v3 が出るのは `with_n(3)` / `with_n(4)` を明示したときだけ。 n = 2 の key は u64 でも
  上位 32 bit が 0 なので、 v2 はゼロ拡張するだけで読め昇順ソート順も保たれる。
- `NgramIndex::gram_count()` / `TextSearch::gram_count()` を追加 (旧名 `bigram_count` も残置)。
- `enchudb_ngram::gram` module (`extract_keys` / `pack` / `key_to_string` / `validate_n`) を公開。
  `bigram` module は n = 2 の薄い互換 wrapper として残置。

### Added — `TextSearch::try_search` — 「答えられない」を明示エラーにする (#121 (c))

postings-only な `.etxt` (#84) は原文を持たないので、 **クエリ長 < n** (全走査する原文が
無い) と **クエリ長 > n** (偽陽性を落とす照合ができない) では答えを出せない。 従来の
`search()` はどちらも**黙って空を返して**いて、 「該当なし」と区別できなかった。

`try_search()` はこれを `ErrorKind::Unsupported` で返す。 `search()` は互換のため
`Vec` を返し続ける (`try_search().unwrap_or_default()`)。 クエリ長 == n だけは検証が
不要なので postings-only でも `Ok`。

### 実測 — n をどう選ぶか (#121 受け入れ条件)

同梱の `cargo run --release -p enchudb-ngram --example ngram_n_bench -- <label>=<corpus>` で、
同一コーパスに対する n = 2/3/4 の index サイズ / 候補数 / 偽陽性率 / レイテンシを出せる。
クエリはコーパス自身から抜いた実在部分文字列 200 件 × 長さ別、 正解は総当たり `.contains()`。

**ASCII (この repo の `.rs` から ASCII 行のみ、 33,021 doc / 1.27 M 文字)** — n を上げると効く:

| クエリ長 | n=2 候補/件 | n=2 FP率 | n=2 search | n=3 FP率 | n=3 search | n=4 FP率 | n=4 search |
|---|---|---|---|---|---|---|---|
| 4 | 376.0 | 18.1% | 66.2 µs | 1.35% | 25.1 µs | 0.00% | 21.6 µs |
| 6 | 193.8 | 14.9% | 64.4 µs | 1.29% | 22.6 µs | 0.05% | 17.3 µs |
| 10 | 75.9 | 8.2% | 67.1 µs | 0.95% | 22.9 µs | 0.25% | 15.0 µs |

合成 ASCII (単語リストから生成、 20,000 doc / 3.68 M 文字) はもっと極端で、 10 文字クエリの
偽陽性率が **99.43% → 35.86% (n=3) → 8.68% (n=4)**、 search が **431.9 → 46.1 → 8.9 µs (48x)**。

**日本語 (この repo の `CHANGELOG.md` + `notes/` + `docs/`、 6,664 doc / 38 万文字)** —
n を上げると **2 文字クエリが死ぬ**:

| クエリ長 | n=2 | n=3 | n=4 |
|---|---|---|---|
| 2 (最頻) | **27.4 µs** | 421.8 µs (scan) | 425.5 µs (scan) |
| 3 | 21.3 µs | 9.2 µs | 451.9 µs (scan) |
| 6 | 9.7 µs | 3.0 µs | 2.2 µs |

CJK は bigram で既に偽陽性率が 18〜33% と低く、 n=3 の改善幅 (数 µs) より **`国民` `接地` の
ような 2 文字クエリが O(N) 全走査に落ちる 15 倍の劣化**が支配する。 判例 67k 件 / 3.3 GB
(#84) でこれをやると致命的。

**index サイズ**は issue の見積り (+4%) より大きい: ASCII で 8.5 → 9.1 MiB (n=3、 +7%) /
9.6 MiB (n=4)、 日本語で 2.7 → 3.5 MiB (+30%) / 4.3 MiB (+59%)。 key 幅 (12 B → 16 B) の
寄与はほぼ無く、 効いているのは **posting エントリ数**: n が小さいほど 1 doc 内で同じ gram が
繰り返され dedup で消えるため、 n を上げると posting が増える。 それでも n=3 の ASCII は
+7% で 3 倍速いので、 サイズは判断材料にならない。

→ **既定を変える理由は無い** (CJK が主戦場)。 ASCII/英語コーパスを持つ consumer が
`with_n(3)` を明示的に選ぶ、 という形が正しい。 (d) の script-adaptive n
(CJK run は n=2 / latin run は n=3 を 1 index 内で切り替え) は数字が出ているので
wontfix にはしない — 別 issue。

### 検証 (0.15.1)

- `cargo test --workspace` — **718 passed / 0 failed / 28 ignored** (0.15.0 時点 679)
- `tests/issue121_variable_n.rs` (9) — n=3/4 の file / bytes round-trip、 `open_mut` の
  rebuild が n を引き継ぐこと、 n ≥ 3 の key が hash に潰れず衝突ゼロであること
  (先頭 2 文字が同じ trigram 400 種が全て別 posting になる)、 範囲外 n の拒否
- **旧 v2 の後方互換は「crate の writer を使わない手組みバイト列」に対して固定**
  (`build_legacy_v2_bytes`)。 key の計算も `(c1 << 16) | c2` を直書きして
  `to_key` に依存しない = writer と reader が同時に壊れても気づける。 併せて
  `NgramIndex::new()` の出力がこの手組みバイト列と**完全一致**することも assert している
- `crates/enchudb-textsearch/tests/issue121_query_len_policy.rs` (9) — クエリ長 × 原文有無の
  4 象限、 `search` と `try_search` の一致、 n 文字ちょうどの結果が総当たり `.contains()` の
  正解と一致すること
- `cargo clippy --all-targets` 警告 0

### 非対応のまま残したもの (#121)

- n ≥ 5 (16 bit × 5 = 80 bit で u64 に収まらない = exact key を諦めることになる)
- (d) script-adaptive n — 上記のとおり別 issue
- 1..n の短い gram を別 posting 空間に併載して短クエリを救う案 (index 肥大とのトレードオフ)

## 0.15.0 — 2026-07-31

**on-disk format v8** (index の slot 関数変更に伴う version bump)。 公開 API 追加 1 件 +
enchudb-rag の signature 変更 1 件。 いずれも naruhodo の実ワークロードから出た 3 件。

### Fixed — vocab index の slot が hash 下位ビットで clustering する (#123)

索引の slot を `h & mask` = hash の**下位**ビットで選んでいた。 一方 `fxhash` は乗算で
終わる (`(h.rotate_left(5) ^ word).wrapping_mul(SEED)`) ため、 **積の下位 k bit は両
オペランドの下位 k bit だけで決まり**、 上位側の entropy が下位に届かない。 結果、 構造の
似たキーが同じ slot に集まって linear probing のクラスタが伸びていた。 実例 (日本法令
コーパスの実キー): `第10条` は UTF-8 で 8 byte = 1 word ちょうどなので `h = word * SEED`
そのものになり、 `第10条`〜`第13条` の**下位 32 bit が完全一致**する。

- `Vocabulary::home_slot()` で **上位ビット**を採る。 slot に格納する hash は従来どおり
  完全な 64 bit なので、 **格納形式は不変** (変わるのは probe 開始位置だけ)。
- index magic `VIX2` → `VIX3`。 VIX2 を検出したら **clean_flag が立っていても作り直す**
  (旧 slot 関数の index を新 slot 関数で引くと全 miss する)。
- `migrate_legacy_index()` が旧 probe で**旧 slot を tombstone 経由で正確に消してから**
  rebuild する。 消さずに再挿入すると no-clear rebuild (#92) の上に二重に載り、 占有率が
  最大 2 倍になってクラスタ改善が相殺される。 index 全域の zero-fill は sparse ページを
  全物理化する (#92 の回帰) ので採らない — clear / insert とも O(count)。

### Added — `GrowableOptions::vocab_max_entries` (#122)

`vocab_max_entries` が `max_entities × 16` (上限 256 M) から導出されていたが、 **vocab に
入る値の種類数は entity 数と相関しない**。 辺が entity の大半を占めるグラフ形で索引が実需の
1,000 倍以上確保される。 実測 (日本法令コーパス: 9,518 法令 / node 788,464 / 辺 20,691,521):

| | 従来 | 実需 |
|---|---|---|
| max_entities | 44,126,694 (辺が 94%) | — |
| vocab_max_entries | 256,000,000 (上限に張り付き) | 104,971 (実ユニーク Tag 値) |
| vocab_index_cap | 268,435,456 slot × 13 B = 3.49 GB | 13.6 MB 相当 |
| on-disk / maxrss | 1,260 MB / 1,252 MB | — |

充填率 **0.04%**。 `GrowableOptions { vocab_max_entries: Some(n), .. }` で consumer が実測値を
渡せるようにした (#118 と同じ opt-in 方式)。 `None` は従来式のままなので**既存 consumer の
挙動は不変**。 header 焼き込みなので既存 DB は rebuild が必要 (`max_himos` と同じ性質)。

### Fixed — Leaf re-tie の順序を全経路で `insert → publish → free` に統一 (#119)

旧 slot を **free してから** insert / publish する経路が残っていた。 同サイズの re-tie では
best-fit が「たった今 free した hole」を必ず再利用するため、 旧 offset を掴んでいる並行
reader が再利用済み slot を読む。 独立レビューで sync / async の 2 経路が漏れているのが
判明した (直接経路のみ修正した状態でリリースしかけていた)。 実測:

| 経路 | 修正前 | 修正後 |
|---|---|---|
| `tie_bytes_to_by_id` (直接 / `Engine::content` 等) | silent None 8,132 / 60,463 reads | 0 |
| `remote_tieleaf_apply` (sync = replica / gossip 受信) | silent None 59,444 / 6.6 M reads | 0 |
| `apply_op::Tie` (async = `create_concurrent*` + `tie_*_async`) | **捏造 bytes 370,471** + None 20,390 / 8.4 M reads | 0 |

async 経路の「捏造 bytes」は payload に free-list の hole header (`[36,0,0,0]` 等) が
混ざった値で、 legacy slot 経路では seqlock でも検出できない = **静かに間違った値が読める**。

あわせて `get_text_owned` の retry を時間方向に散らした (spin → yield の backoff、 上限
64 → 256)。 単一 cell を止めどなく re-tie する writer と競ると retry を間を置かずに使い切り、
「値が無い」と区別できない None を返していた (3〜9 件 / 33 万 read → 0)。

### Fixed — schema / SQL / RAG / ravn の text 読みが並行 write 中に壊れる (#119 Step 0)

上位 4 層がいずれも engine の**借用返し** `get_text` / `get_content` を呼び、 受け取った
借用を即コピーしていた。 借用版は #106 の slot gen seqlock verify を通らないため、 writer が
Leaf を re-tie している間に torn bytes を掴む。 実害は silent `None` (「値が無い」と「壊れて
読めなかった」が区別できない) だけでなく、 **host process を殺す panic** まで含む —
`LeafStore::get` が torn slot header の len で slice 範囲を作り OOB する (#59 の系統、
falsify で実演)。

- 内部呼び出しを `get_text_owned` (verify + retry 付き) へ移行。 元々コピーしていたので
  **コピー回数は不変**、 増えるのは verify の再読のみ。
- `Engine::get_content_owned` を追加 (`get_text_owned` の content 版)。

#### 検証

| 項目 | 結果 |
|---|---|
| macOS workspace (逐次) | 679 passed / 0 failed |
| Linux (OrbStack `rust:latest`、 engine/schema/sql/rag/oplog) | 519 passed / 0 failed |
| **実 v7 DB** (0.14.4 実バイナリが生成、 FILE_VERSION 7 / index magic VIX2) | readonly open は v7・VIX2 のまま 200/200、 writer open で v8・VIX3 へ移行し 200/200、 再 open も 200/200 |
| **本物の 2 プロセス** (子 writer が Leaf churn、 親が `open_readonly` を反復) | 258,869 writes と並行で lookup mismatch 0 |
| re-tie 順序 (直接 / sync / async の 3 経路) | silent None・捏造 bytes とも 0 |

falsify (修正を無効化して落ちることの実演) は #120 の align8 検証、 #123 の VIX2 migration、
#119 Step 0 の借用版差し戻し、 crash 中断 tombstone の回収、 の 4 件で実施。

`cargo test --workspace` は `enchudb-transport` が repo 外の `../sabitori` に path 依存する
ため clean 環境では manifest 解決に失敗する (0.15.0 とは独立の既知問題、 別途 issue 化予定)。
Linux 検証は該当 crate を除いた 5 crate 指定で実行した。

#### Migration

- **既存 DB はそのまま開ける**。 vocab index は derived data なので、 v7 DB を writer で
  open した時点で自動的に作り直される (`VIX2` 検出 → in-place migrate)。 手動作業は不要。
- **mixed-version 運用は非サポート**。 0.15 が書いた DB は header v8 になり、 **0.14 以前の
  binary は `unsupported EnchuDB file version 8` で open を拒否する**。 これは意図的な設計で、
  0.14 は index magic を検証しないため、 新 slot 関数で書かれた clean index を旧 slot 関数で
  読んで **silent に lookup miss** するのを防ぐ。 同じ DB を複数バージョンで開く運用がある
  場合は、 全 consumer を 0.15 へ揃えてから移行すること。
  - v7 → v8 の書き換えは **writer open 時のみ**。 `open_readonly` は共有 mmap を書かない
    ので header は据え置き (= readonly consumer だけ先に 0.15 化しても v7 のまま保てる)。
- **breaking (enchudb-rag)**: `RagStore::text` が `Option<&str>` → `Option<String>`。 借用を
  外に出さないための変更。 呼び元は `.as_deref()` で従来の比較がそのまま書ける。
- `GrowableOptions` への field 追加は `..Default::default()` 利用側は無変更。
- `vocab_max_entries` に 2^31 超を渡すと create が Err になる (`next_power_of_two` が u32 を
  溢れ、 release build では index_cap 0 が header に焼かれて open 不能な DB ができるため。
  #120 と同型を新 knob で再発させない)。 天井 hit の panic message も knob 名と
  「header 焼き込みなので既存 DB は再作成が必要」を含む actionable な形にした。
- `Engine::get_content_owned` は新経路の cell が設定されている場合、 retry 枯渇でも
  **legacy content region に fallback しない** (0.9.0 以降凍結しているアーカイブの古い値が
  蘇るため)。 fallback は「新経路に cell が無い」ときだけ。

## 0.14.5 — 2026-07-30

silent データ全損の防止 1 件。 on-disk format / 公開 API / 既定 layout は不変。

### Fixed — align8 が u32 上限を跨ぐ create が「二度と開けない DB」を作る (#120)

`create_growable_with_leaf(path, ents, Some(u32::MAX as usize), ..)` が **create もビルドも
成功する**のに、 できたストアを open すると
`vocab_data_size 4294967296 exceeds format limit 4294967295 (u32 data_end) — corrupt header`
で恒久的に開けなかった。 naruhodo の配信ストア準備で、 **7 分のフルビルドが「完走ログを
出した後に全損」**する形で実踏 (v0.14.4)。

原因は検証の当て先。 create 側は **整列前** の要求値 (`u32::MAX` = 4 GiB−1) を検証して
通すが、 layout は `align8(n) = (n + 7) & !7` で **4 GiB ちょうど (2^32)** に切り上げた値を
header に焼く。 open 側は header の値を u32 data_end 制約で検証するため、 create と open で
判定が食い違っていた。

- `Layout::try_from_params` で可変 region 3 本 (vocab / himoreg / content data) を
  **align8 後**の値で検証。 create / open が同じ関数を通るので判定が原理的に一致する。
- `Layout::compute` / `compute_with_caps` を `Result` 化し、 create 経路は
  `io::ErrorKind::InvalidInput` で伝播 (旧 `from_params` の `expect` panic を廃止) —
  **1 byte も書く前に Err で落ちる**。 どちらも private 関数なので公開 API は不変。
- 上限ちょうど (8-aligned = `u32::MAX & !7`) は従来どおり create でき、 open も通る
  (検証を厳しくしすぎて正常な最大値を弾いていないことを test で固定)。

#### Migration

- 不要。 既存の正常な DB / 公開 API / format はいずれも不変。
- **既に生成してしまった壊れた DB は開けないため作り直しが必要** (該当条件は
  `vocab_data_size > u32::MAX - 7` を渡した create のみ)。 0.14.5 では同じ引数が
  create 時点で Err になる。

## 0.14.4 — 2026-07-26

公開 API 追加 1 件 (additive・後方互換)。 on-disk format / 既定 layout は不変。

### Added — `GrowableOptions` で growable の全 layout knob を露出 (#118)

`Database` の growable create API が `max_entities` / `vocab_data_size` / `leaf_data_size`
しか露出せず、 **`max_himos`（DB 全体の himo = table × column 通し上限、 default 256）** /
`content_data_size` / `cyl_max_values` を設定する術が無かった。 himo は DB 全体で通し採番
されるため、 router+scope を 1 DB に同居させる sinfo が 40 table / 列合計 255 まで育った
ところで新列追加が `too many himos (max 256)` で失敗し、 **無関係な全 table の open を
巻き添えで殺していた**。 (queue_cap #116 に続く「schema が engine knob を出し損ねる」系の 2 件目。)

- 追加: `GrowableOptions { max_entities, max_himos, vocab_data_size, content_data_size,
  cyl_max_values, leaf_data_size, leaf_scale }` + `Default` (engine / schema で re-export)。
  `Engine::create_growable_opts(path, opts)` / `Database::create_growable_with(path, opts)`。
  気にする knob だけ `..Default::default()` で上書きでき、 **将来 knob を足しても variant が
  増殖しない** (従来は `create_growable_with_capacity` / `_with_options` / `_with_leaf` と
  部分被覆 variant が増える一方 + 組合せ不可だった)。
- `too many himos` エラーを actionable 化 (「GrowableOptions で引き上げよ、 既存 DB は rebuild」)。

**default は 256 据え置き (raise しない)**: himo 領域 = `max_himos × Column::region_size(
max_entities)` = **max_entities 比例の per-himo 列領域を max_himos 倍する**構造。 16M entity DB で
256→4096 にすると himo 領域が ~16GB→~256GB の apparent (Linux は sparse だが macOS/APFS は phys
inflate) に膨れるため、 全 DB の default 引き上げは不可。 → 必要な consumer が
`GrowableOptions { max_himos, .. }` で自 DB の apparent 増を承知の上で opt-in する設計 (#116 と同原則)。

#### Migration

- **既存 DB は max_himos が header 焼き込み**のため、 上げるには `create_growable_with` で
  新規作成 + rebuild が必要 (既存 DB を開くだけでは 256 のまま)。
- 旧 `create_growable*` は全て残置 (後方互換)。 新 knob が要らなければ変更不要。

## 0.14.3 — 2026-07-24

data corruption 1 件。 公開 API / on-disk format は不変 (patch)。

### Fixed — schema `Database` + raw `define_table` の DB が reopen で silent にデータ破壊 (#117)

`enchudb::schema::Database` を経由しつつ raw `engine_mut().define_table()` /
`define_himo_in()` で table を定義し、 `finish_*` を呼ばず `flush()` + drop する DB
(opyula の wiki route / cord junction 等) で `.tables` sidecar が永続されず、 reopen 時に
2 つの顔で壊れていた:

1. **table 定義消失** → range 再導出で既存 entity が範囲外に孤立し tie が panic
   (`tie eid N not in himo's table ... eid_range`)。
2. **`next_local` 巻き戻り** → `entity_in` が生きた eid を再払出し、 既存 entity を無警告で
   上書き破壊 (元 node と archive が同一 eid に合体する frankenstein entity)。 (2) が致命的。

root cause は `Database::wrap_new` の `defer_tables_persist=true` を、 builder の `self.tables`
に載らない raw-define table に対して Drop guard が「空 Database」と誤認し persist を skip して
いたこと。 #47 (builder 経路の同種 reissue) が別経路から再噴出した形。

- **[本修正] open 時に live bitmap から `next_local` を自己修復** (`reconcile_next_local_from_bitmap`)。
  body に msync 永続された live bitset を ground truth に、 各 table 範囲の「max live local + 1」まで
  `next_local` を前進させる。 persist 頻度非依存の self-healing で、 alloc 後 persist 前の
  **crash window の eid 再払出も塞ぐ**。 経路非依存。
- **[補助] Drop guard を engine 実態で判定**: `self.tables` ではなく `eng.list_user_tables()`
  非空で persist を判定し、 raw-define DB の sidecar 永続漏れを解消。 user table が実在する DB は
  region commit 済なので、 旧 skip が避けていた「空 growable の msync SIGBUS」経路には入らない。
- 検証: repro 2 face (table 消失 / eid 再払出) を `issue117_raw_define_reopen` で test 化 (master
  fail → 修正 pass)。 falsify matrix で両 fix が load-bearing を実証 (Drop guard 無効化 = crash
  window 相当で face2 が本修正のみで生存)。 workspace 641 test green、 open bench 有意差なし。
- **format 変更なし** (on-disk layout 不変。 既存 healthy DB は `next_local` 据え置きで挙動不変)。

#### Migration

- **≤0.14.2 で raw-define パターンを使っていた DB (opyula 等)**: 0.14.3 で open 時に `next_local` が
  自己修復されるため **今後の eid 再払出は止まる**。 ただし **既に上書き破壊された entity は復元
  されない** — 破壊が疑われる DB は snapshot / rebuild で作り直すこと。
- schema builder (`db.table().build()`) + `finish_*` のみを使う consumer (sinfo 等) は影響なし、
  upgrade で挙動不変。

## 0.14.2 — 2026-07-20

soundness 2 件。 いずれも公開 API / on-disk format は不変 (patch)。

### Fixed — `Region::slice_mut(&self)` を撤廃、 sound な write API に置換 (#83 option 2)

0.14.1 (#83) は clippy gate を `#[allow(clippy::mut_from_ref)]` で green にしただけの
band-aid だった。 本 release で soundness の本筋を完遂: `&self` から `&mut [u8]` を返す
`Region::slice_mut` (safe code から aliasing UB を作れる) を **完全撤廃**し、 `&mut [u8]` を
一切実体化しない write API へ 26 call site を移行した。

- 追加 API: `write_at` (raw ptr memcpy) / `fill_at` (memset) / `as_atomic_u8` (slot flag) /
  `as_mut_slice(&mut self)` (排他借用版、 open 時 rebuild 専用)。 いずれも `&self` からの
  long-lived な mutable alias を作らない。
- `column` / `cylinder` / `entity_set` / `content_store` / `leaf_store` / `vocabulary` の
  slice_mut を全廃。 vocabulary `rebuild_index` は `&mut self` + 分割借用に。
- `cargo clippy --workspace --all-targets` = 0 error (理由付き `#[allow]` も不要に)。
- engine.rs `Backing::slice_mut_shared` / `header_mut` の同種置換は別 follow-up。

### Fixed — #106 seqlock の残存 torn read を根絶 (#113): payload を relaxed-atomic 化

#106 (0.14.0) の gen seqlock 後も、 `issue106_leaf_torn_read` は aarch64 で **~1e-8/read**
の torn read が残っていた (reader/writer が payload を **plain memcpy** 同士で読み書き =
形式上 data race UB、 weak-memory で稀に seqlock を突破)。 seqlock が触る全 field
(`slot_size` / `len` / `payload` / free-slot header) を **relaxed-atomic** 化して data race を
除去した。 fence / gen の Release/Acquire は #106 のままなので順序契約は不変
(Boehm の fence 版 seqlock が relaxed-atomic data で成立)。

- Region に `write_atomic` / `read_atomic` (4B aligned bulk は `AtomicU32`、 端数 `AtomicU8`)。
- 検証: master ~50% run fail → 本 fix **12/12 PASS / CORRUPT 0**。 reader だけ plain へ戻すと
  CORRUPT 再発 (1/6) で load-bearing を falsify 実証。 `issue106_leaf_torn_read` が信頼できる
  regression guard に昇格。 slot layout は不変 = **format 変更なし**。

## 0.14.1 — 2026-07-19

### Added — schema `Database::create_growable_with_leaf` (#109)

schema `Database` に Leaf データ領域サイズを指定する
`create_growable_with_leaf(path, max_entities, leaf_data_size)` を追加。 vocab 版
`create_growable_with_options` に対する Leaf 版で、 大量 Leaf text (chunk 本文 /
tool 出力 / 長文備考) を持つアプリが default 512 MiB を溢れる場合に使う。
`Engine::create_growable_with_leaf` を wrap (`LeafScale::Gb16` = `leaf_data_size` は
16 GiB まで指定可)。 round-trip + reopen 永続 + cap 超過 reject の test 付き。

### Fixed — clippy gate を green に戻す (#83)

CI の `cargo clippy --all-targets` が deny lint で全滅し、 workspace の clippy gate が
赤いままだった問題を解消。 `query_lang::apply_stages` の `while` を `if` に
(許可 stage 列は先頭で分岐確定 = 構造上 1 周のみ = `never_loop`、 挙動不変)、
`Region::slice_mut` / vocabulary test helper の `mut_from_ref` を理由明記で局所
`#[allow]`。 soundness の本筋 (`&mut [u8]` 返し廃止 = `write_at`/`ptr` 置換) は #83 に
P2 として残す。 `cargo clippy --workspace --all-targets` = 0 error。

## 0.14.0 — 2026-07-18

### Fixed — `LeafStore` の read-while-write torn read を根絶 (#106): slot gen seqlock

`Leaf` himo を writer 稼働中に別 thread / 別プロセス (`open_readonly` の mmap reader)
が読むと、 torn read で **silent data corruption** か **OOB panic** が起きていた
(0.12.0〜0.13.2 全滅、 実測 violation 率 ~0.07%)。 `LeafStore` が #95 (lock-free read) /
#99 (bucket-local verify) のどちらの保護にも入っていなかったのが穴。 slot ごとの
**世代カウンタ + seqlock** で in-process / cross-process とも torn を根絶した。

- **slot header 8B→12B (gen 付き)**: 新規 slot は `[slot_size|HAS_GEN][len][gen]` の 12B
  header。 `slot_size` の bit0 を gen フラグに使う (4-align で本来 0)。 write は
  gen odd→payload→even (Release)、 read は gen Acquire で g0→copy→再読 g1 一致を確認。
  gen は store-wide 単調カウンタ由来 (offset 再利用のたび必ず変わる)。
- **read API `Engine::get_text_owned(eid, himo) -> Option<Vec<u8>>` を追加**: 並行 read
  する consumer 用の安全版。 gen seqlock + column offset 再読 + bounds clamp で torn/
  stale/OOB を封じ、 所有 `Vec` を返すので live mmap `&[u8]` の aliasing UB も無い。
  **借用を返す従来の `get_text` は single-thread / quiesce 前提** (writer 稼働中の Leaf を
  並行 read する経路は `get_text_owned` へ移行すること — 特に `open_readonly` の
  cross-process reader)。
- **format 変更 (LEF1→LEF2)**: 既存 v6/v7 DB は **read 互換** (旧 8B slot はそのまま読め、
  新規 insert のみ 12B gen slot、 **データ移行不要**)。 ただし新 DB / gen slot を書いた DB は
  magic が `LEF2` になり、 **旧 engine (≤0.13.2) は open を clean refuse** する
  (forward-incompatible)。 → minor bump。
- **leaf footprint**: gen 4B/slot 分わずかに増える (小値 slot が多いほど相対的に効く)。
- 検証: in-process 66M reads / cross-process (`open_readonly`) 27M reads とも violation 0
  (決定的、 falsify 済み)。 `tests/issue106_leaf_torn_read.rs` /
  `tests/issue106_leaf_cross_process.rs`。

## 0.13.2 — 2026-07-17

### Changed — bucket ローカルメタデータ: verify 局所化 + 正確な統計 + incremental compaction (#99)

`AppendBucket` に live counter と removed flag を持たせ、書き込み側が既に読んでいる
旧値でメンテするようにした。#95 の「churn した himo は read が全 bucket で
verify + sort + dedup を払い続ける」「unique_count / total / slice_len が stale 込み
over-count」という 2 つの構造的コストが解消され、#99 (compaction) の実体が入った。

- **verify の bucket 局所化**: churn していない bucket の read は verify-free fast path の
  まま (旧実装は himo 全体 flag `any_removed` が 1 回の値更新で恒久 ON)。判定は
  slice → flag → backing ptr 再検証の 3 段プロトコル (`read_snapshot_verify`) で、
  並行 churn 中の fast path が重複 eid を返さないことを stress テストで保証。
  churn 後の無傷 value の pull: 520ns → 57ns (-91%)。
- **統計の live 基準化**: `unique_count` / `total` / `slice_len` が「Column が現在
  指している数」を返すように (over-count 解消)。**query planner の wrong-result 修正
  を含む**: 全滅 (all-stale) bucket の raw len が entity 数を超えると 2 条件 query の
  always-true skip が誤発火し、実際には誰も match しない条件で誤ヒットしていた
  (regression テスト付き)。insert hot path の atomic RMW 数は従来と同一
  (total_live は total − stale_total の導出、per-insert 加算なし)。
- **incremental compaction (#99)**: 書き込み時に stale 率 50% && len ≥ 64 の bucket
  だけを Column 基準で組み直して epoch swap (readers 非停止)。churn した value の
  pull: 36.8µs → 1.48µs (25x)。200 eid × 400 往復 churn の backing が 512KB 単調増加
  → <32KB 有界。明示 API **`Engine::compact_himo(himo: &str) -> bool`** 追加
  (readonly / replica open では他の write 系 API と同様 panic)。
- **挙動注記 (非 breaking)**: churn 済み himo の `pull` の返却順が変わり得る。
  旧実装は churn があると常に sort 済みを返したが、新実装は無傷 / compaction 済み
  bucket で raw append 順を返す (verify 経路のみ sort+dedup)。順序が必要な場合は
  呼び出し側で sort すること (`pull_sorted` / 自前 sort — 同梱 crate は全て順序非依存
  を確認済み)。
- AppendBucket の「len 単調非減少」契約は撤回 (compaction で縮む)。

## 0.13.1 — 2026-07-15

### Fixed — graceful close で clean flag を永続化 (#101): 毎 open の vocab/himo index rebuild を解消

clean flag（index↔data 整合性マーク）を書く経路が `flush()`（`&mut`、実質 `seal_integrity`
専用）にしか無く、通常の close は dirty のまま終了 → **writer open 毎に vocab/himo_reg の
`rebuild_index` が O(count) で走っていた**（sf のような 1 コマンド 1 open の使い方で全コマンドに
乗る固定税。readonly open も shadow index を毎回 heap rebuild）。vocab は回収なし単調増加なので
税は unbounded に育つ（20万 entry で実測 +20ms/コマンド）。

- **`Engine::flush_clean(&self)`** 追加: 滞留 write を全 apply → 全 region msync →
  vocab/himo_reg の clean マーク → 再 msync。プロセス生存中の checkpoint 用（sinfo の
  `sync()` 等から呼べる `&self` 版）。readonly open では no-op。
- **`Engine::Drop` で best-effort clean-flush**: graceful close だけで次 open が rebuild を
  skip できる。panic unwinding 中 / consumer 死亡時 / readonly は書かない（= dirty のまま
  → 次 open の rebuild が正しい recovery）。
- 観測 API: `vocab_index_rebuilt_on_load()`（open で rebuild が走ったか）/
  `vocab_index_is_clean()`（disk 上の clean flag）。
- writer open が open 直後に flag を 0 へ戻す #56 の write-crash 保護は不変。
  semantics は「graceful exit → clean → 次 open skip / crash → dirty → rebuild」。
- regression test: `issue101_clean_reopen`（graceful close で skip・crash 相当 copy で
  rebuild・readonly 非破壊。Drop hook を無効化すると落ちることを確認済み）。

## 0.13.0 — 2026-07-14

### Changed — Cylinder read を lock-free 化（#95、RwLock 撤去）

`HimoStore` の `RwLock<BucketCylinder>` は read が write と per-himo で相互排他になり、
長い read（巨大 bucket の clone）が write を stall させ、read↔write が取り合っていた
（sinfo 等「開いたまま read しつつ write する」アプリで問題）。CLAUDE.md が謳う
「ロックフリー並行 read / ダブルバッファ + AtomicBool swap」は**実装されておらず**
（履歴上 double-buffer は一度も存在せず）、実物は `std::sync::RwLock` だった。

これを **append-only + epoch（crossbeam-epoch）** の `LockFreeCylinder` に置換:

- **read は完全 lock-free**（writer を一切待たない、epoch pin）。value→eid の各 bucket は
  append-only（publish 範囲は不変、contiguous `&[u32]` を維持 → query 無改造）。
- dense（value < 1M）は `Atomic<Vec<Arc<AppendBucket>>>` を epoch-swap で on-demand 成長、
  sparse（value ≥ 1M、稀）は `Mutex`。realloc の旧 backing は epoch で全 reader 通過後に解放。
- **write は per-himo writer lock で直列**（append O(1) amortized、メモリ ~1 倍）。writer の
  呼び出し元は consumer 1 本ではない — **同期 tie（`tie_to_by_id` 系）/ schema
  `RowBuilder::commit` は任意の user thread が呼ぶ**（master では RwLock write が直列化）。
  初版はここを見落として無 lock にしており、多 thread schema commit（sunsu matrix bench）で
  epoch defer_destroy の double free → malloc abort していた。writer lock で master と同じ
  write 直列度に復元（reader は lock を一切取らないまま = 本 fix の目的は維持）。
  lock は `parking_lot::Mutex`（critical section ~100ns に対し std::sync::Mutex は競合で即
  カーネル休眠 = psynch 待ちが支配項になり schema write が ~40% 落ちた。adaptive spin で回復）。
  insert の epoch pin も 3 回 → 1 回に集約（`push_in`、pin を insert 全体で共有）。

  sunsu matrix（100k posts / 4 thread、warm、同一機）で master 同等を確認:

  | 構成 | master | 本 branch |
  |---|---|---|
  | raw tie_async / oplog off | 8〜15M ties/s | 8〜26M ties/s（同等） |
  | raw tie_async / oplog on | 5.0M | 5.1M |
  | schema commit / oplog off | 5.6〜8.0M | 5.6〜7.6M |
  | schema commit / oplog on | 0.4M | 0.4M |
- 削除/更新は Cylinder を触らず（append-only）、read 側が **Column verify で stale を
  filter**（conditional: append-only himo は verify を skip = fast path）。churn 由来の
  dup は read で dedup。compaction は後付け最適化（未実装、#99 で追跡）。
- `rebuild()` は no-op 化（互換で残置）。Cylinder は open 後 lazy build + live 維持。

**互換性**: 公開 API・on-disk format・wire 不変。`unique_count` / `total` は churn した
himo で over-count になる（append 数ベース、compaction #99 まで）。append-only では従来通り正確。
読みは live semantics（snapshot なし）— read-while-write の held snapshot は #100 へ切り出し。

**観測 API 追加**: `Engine::himo_cylinder_backing_bytes(himo)` — Cylinder が確保する eid
backing の総 bytes（メモリ会計・double-buffer 検知）。

**検証**:
- **unit**: `append_bucket` / `lockfree_cylinder` の並行 test（1 writer + N reader、破損なし）、
  `no_double_buffer_backing_bound`（backing < 2× = double-buffer していない厳密証明、実測 1.28×）。
- **統合**: `issue95_lockfree_read`（並行 pull・値更新 stale の verify filter・**同期 tie
  ×4 thread の並行 write** = write_lock regression test、修正を外すと crash することを確認済み）。
- **loom model check** (`tests/loom_append_publish.rs`、`#![cfg(loom)]`): AppendBucket の
  publish protocol（writer が slot 書込→`len.store(Release)`、reader が `len.load(Acquire)`→
  prefix 読み）を de-epoch した model で全 interleaving を model check。**範囲は単一 backing の
  publish handshake のみ**（grow/swap + epoch 解放は loom 非対応につき対象外、そちらは Miri +
  `grow_under_read` stress で補完）。1 writer + 1/2 reader で torn read / data race ゼロ。
  `Release`→`Relaxed` に落とすと loom が torn read を検出して落ちることを確認済み
  （再現手順は test 冒頭コメント）。手書き model なので `append_bucket.rs` の ordering 変更時は
  model の同期が必要（相互参照コメントあり）。
  実行: `RUSTFLAGS="--cfg loom" cargo test --test loom_append_publish --release`。
- **model-based property test** (`tests/engine_model_proptest.rs`): tie/tie_text/untie/delete/
  reopen のランダム op 列（proptest、200 case × ≤40 op）を参照 oracle（`BTreeMap`）と毎 op 後に
  厳密照合。**Number と Tag himo を跨いで**（Number は Column 直値、Tag は Vocabulary intern
  した vid で read path が違う）、`pull_raw` / `get` / `get_text` / 2-cond `query` を網羅。
  値更新→削除→再 tie→reopen の組み合わせで stale/dedup/verify/rebuild を自動生成 + shrink。
  engine crate に proptest dev-dep を追加。
- **破壊テスト** (`tests/issue95_stress.rs`): `churn_storm_exact`（20k×40 round の値更新で
  bucket を stale だらけにし、並行 read の構造 invariant を保ちつつ quiesce 後の pull が
  live 集合と厳密一致）、`crash_recovery_compacts`（churn→drop→reopen で Cylinder が column
  から rebuild され stale が消える）、`grow_under_read`（dense 配列 realloc 多発 × 旧配列を
  掴む reader で epoch 解放が安全）。
- **fault-injection** (`tests/oplog_recovery_fault.rs`): ① file 縮小 truncate（7 点）— WAL は
  固定容量 pre-allocate なので capacity guard の **clean Err が仕様**（crash しないことを検証）。
  ② tail zero 化（5 点、file size 不変 = torn write の現実的模擬）/ ③ byte-flip（8 offset）—
  graceful かつ Ok なら pull が**書いた集合と完全一致**（body msync 済み = 開けた以上 1 件も
  欠けない）、**最低 1 case は Ok**（全 case Err の vacuous pass を弾く guard。この guard 導入で
  旧 truncate テストが実は全 Err の空振り passだったことを検出し、② を追加した）。
  body sync 前 crash（未 checkpoint tail の replay）は subprocess crash harness が要るので
  #98 ④ の範囲。
- **Miri UB 検証**: `append_bucket` 全 test + `lockfree_cylinder` の dense 系 test を Miri
  **Tree Borrows** で回し UB なしを確認（unsafe 表面 = `from_raw_parts` over `UnsafeCell`、
  epoch `defer_destroy` はこの範囲で全て踏む。sparse path は `Mutex` + `HashMap` で unsafe なし）。
  crossbeam-epoch 0.9 は Stacked Borrows 非互換（内部 intrusive list、既知）なので TB +
  `-Zmiri-ignore-leaks` で回す（epoch 遅延解放の exit 時 garbage を除外）。concurrent/大量 test は
  `cfg!(miri)` で縮小。
- **bench** (`examples/lockfree_engine_bench.rs`、実 Engine 経路): A. 巨大 bucket を 4 reader が
  clone し続けても drain は同オーダー（write は long read に stall しない、減少分は CPU 帯域
  contention であって lock 待ちではない）。B. writer が同 bucket を叩き続けても pull_raw の
  p50/p99 latency が idle と同オーダー（read は write に stall しない）。C. cylinder backing
  1.28×（メモリ ~1x、double-buffer なし）。
- engine lib 200 + 統合/破壊 5 + schema/sql/sync 全 green。prototype 実測（`examples/lockfree_bucket_probe`）で
  write starvation 214ms → 解消、verify tax 0.85〜4.5 ns/elem（`examples/verify_tax_probe`）。

## 0.12.2 — 2026-07-11

### Fixed — dirty open が vocab index 予約全域を物理コミットする問題 (#92 / #56 ③)

crash 相当の落ち方 (flush せず drop / `process::exit` = `clean_flag=0`) をした DB を
write open するたび、 `Vocabulary::rebuild_index_into` が **index 予約全域
(`index_cap`×13B) を zero-fill** してから live entry を再挿入していた。 index region は
fixed cluster で mmap 済みだが **sparse** (物理ブロック未確保) なので、 この全域
書き込みが sparse ページを 1 枚残らず物理化 → **live vocab 数と無関係に `index_cap`
比例の物理コミット**が起きていた (#56 で ①② は fix 済だが ③「rebuild は used slot
だけ touch する」提案が未実装のまま残存)。 `create_growable_with_capacity` は
`vocab_max_entries = cap×16` なので大 pool ほど深刻 (cap=1M で dirty reopen 一発
+~200MB、 cap=16M で ~3.5GB)。 sinfo CLI (sf) の「空 DB でも起動が pool 比例で重い」
の主因。

- **全域 zero-fill を廃止**し、 既存 on-disk index の上へ live entry (id 0..count) を
  **再挿入するだけ** に (used-slot only touch)。 append-only vocab の count が単調な
  ことを利用:
  - 通常の落ち方では on-disk index は data と consistent → 全 entry が dup 一致で
    **書き込みゼロ** (触るのは live slot が載る数ページのみ)。
  - torn write で index が count より遅れ (slot 欠落) → 空 slot へ再挿入して self-heal。
  - torn write で index が count より先行 (`vid >= count` の「未来」slot 残存) →
    live entry ではないので `slot_hash` 一致でも `get(vid)` を呼ばず読み飛ばす guard を
    `rebuild` / `lookup` / `index_insert` に一貫追加 (OOB と誤 dedup を防ぐ)。 通常運用の
    committed slot は必ず `vid < count` なので **hot path は無影響** (guard は crash
    復旧後のみ発火)。
- **readonly open の RAM 肥大も同時に解消**: dirty DB の readonly open は heap の
  shadow index に rebuild するが、 同じ zero-fill が shadow (calloc で sparse) の全
  ページを touch し `index_cap` 比例の RSS を食っていた。 zero-fill 撤廃で O(count) に。

**互換性**: on-disk format 不変 (v7 のまま)、 wire 不変、 API 不変。 純粋な内部 open
経路の fix。 既に旧挙動で膨らんだ DB の**コミット済み物理を回収**する shrink 経路は
別スコープ (本 fix は今後の bloat を防ぐ)。

**効果 (`issue92_dirty_footprint` bench, live vocab 5 件)**:

| cap | index region | dirty reopen 増分 (before → after) |
|----:|----:|---|
| 256K | ~54MB | +18,112 KB → **+0 KB** |
| 1M | ~218MB | +204,720 KB → **+0 KB** |
| 4M | ~832MB | +843,680 KB → **+0 KB** |

**検証**: `issue92_dirty_index_full_commit` (別プロセス `process::exit` で dirty 化 →
reopen 増分 < 4MB、 fix を戻すと fail する regression guard)、 `vocabulary` unit 3
(consistent 再挿入 / torn-behind self-heal / torn-ahead の OOB 回避)、 全 workspace
test green。

## 0.12.1 — 2026-07-11

### Changed — `LeafStore` の cell offset を word 単位化 + region cap を選択式に (#90)

`LeafStore` (#88 / 0.12.0) の cell 参照・`high_water`・free-list が **生 byte offset
の u32** だったため leaf region が ~4.29GB でハードキャップだった。 slot は
`2^off_shift` aligned で確保されるので **offset の下位 bit は常に 0** = **word offset
(`byte >> off_shift`)** で持てば、 列幅も indirection も増やさず cap を拡げられる。

- cell handle / `high_water` / free-list を **word 単位** に (slot header の
  `slot_size` / `len` は byte のまま)。 region cap = `u32::MAX << off_shift`:

  | scale (`LeafScale`) | off_shift / align | cap |
  |---------------------|-------------------|-----|
  | `Gb16` (default)    | 2 / 4B            | ~16GB |
  | `Gb32`              | 3 / 8B            | ~32GB |
  | `Gb64`              | 4 / 16B           | ~64GB |

- **選択式**: `create_full_with_leaf_scale` / `create_growable_with_leaf` で
  `LeafScale` を指定 (default `Gb16`)。 大きい scale ほど slot alignment (padding)
  が粗くなるので、 小さい payload は `Gb16`、 wikipulse 型の大 payload × 巨大
  working set は `Gb32`/`Gb64`。 予約 `leaf_data_size` は選んだ scale の cap 以下を検証。
- `off_shift` は leaf region header に self-describing に記録。
- `leaf_footprint()` / `MigrationStats.leaf_footprint` を **`u64` (byte)** に
  (16GB 超を表せるよう u32 から拡張 — 呼び出し側の型注釈のみ影響)。

**互換性 (patch に収まる理由)**: on-disk format は v6 → v7 に上がるが、

- **既存 v6 DB は無改変で動く**: v7 engine は v6 region を header の `off_shift == 0`
  (= byte offset) として **read-through** で開く (migration 不要、 4GB cap のまま)。
- **wire / sync は完全に不変**: offset は各 node ローカルの storage 詳細で wire に
  乗らない (`TieLeaf` は生 bytes を運ぶ)。 → **peer 同時アップグレード不要**、
  v6/v7 node 混在でも収束する。
- 既存の create API・tie/read 挙動・reclaim は不変 (additive)。
- **唯一の非互換**: v7 で作った DB は **0.12.0 engine では開けない** (word offset を
  byte と誤読するため version gate で reject)。 = 新規 DB は 0.12.1 以降が必要。
  既存 DB を 4GB 超に伸ばしたい場合は v7 で作り直す (v5→v6 migration の出力は
  従来通り byte-offset の v6)。

**検証**: `leaf_store` unit 10 (word encoding / cap 16-64GB / 5GB sparse で 4GB 超
offset の往復 / shift 2·4 の churn reclaim / v6 byte 互換)、 `issue90_leaf_scale` 3
(scale reopen 永続 / reclaim / cap 超過 reject)、 全 workspace test green。

## 0.12.0 — 2026-07-11

### Changed — `Leaf` を vocab から剥がし reclaim 対応 store に載せる (#88): high-churn Leaf の単一 DB 無限運用

`content()` → `Leaf` 統合 (0.9.0 #81) 以降、 高 churn な `Leaf` 値 (wikipulse の
毎 event content 等) が **共有辞書 vocab に単調 append され、 delete しても回収
されない** 問題 (#88)。 `Leaf` は先が無い終端ノード = 単一所有・dedup 不要なのに、
append-only-never-reclaim の vocab に入るから貯まる。 対策は「vocab に reclaim を
後付け」ではなく **`Leaf` を vocab から出し、 単一所有・reclaim 対応の専用 store に
載せる** こと。

- **`LeafStore`**: 単一所有・dedup 無し・reclaim 対応の可変長 value store。
  free-list (offset→size の BTreeMap) + 隣接空き coalesce + 末尾空きは high_water
  を retract。 live は動かさない (compaction 非搭載 = footprint 有界化には不要)。
  free-list は非永続 = open 時に live cell から再構成。
- **routing**: `Leaf` の tie / read (`get_text` / `get_content` / `get_entity`) /
  delete / untie / sync が `LeafStore` を経由。 **`Tag` は従来通り vocab** (dedup
  される共有辞書性が活きる)。 reserved-table 内の `Leaf` (`_sync_ops.payload` 等)
  も vocab 据え置き。
- **wire**: `Op::TieLeaf{eid, himo_name, himo_kind, bytes}` を新設し、 旧 `Leaf`
  sync (`Op::Vocab{vid}` + `Op::Tie`/`TieNamed`) を置換。 受信側の
  `(author,vid)→local_vid` remap が消える。
- **create tunable**: `Engine::create_full_with_leaf(.., leaf_data_size)` で
  leaf region 予約サイズを指定可 (`Some(0)` = leaf region 無し = v5 相当)。

**footprint bench** (rolling retention, 20000 rounds / window 200 / content 1KB):

| rounds | before (`Leaf`→vocab, 回収なし) | after (`LeafStore`, reclaim) |
|-------:|-------------------------------:|-----------------------------:|
|   4000 |  3.91 MiB | 0.20 MiB |
|  20000 | 19.53 MiB | 0.20 MiB |

before は round に線形増加 (retention delete でも vocab は戻らない)、 after は
live 集合 ~200×1KB で **平坦・有界** (20000 rounds で 99x)。 = 単一 DB の無限運用が
可能に。 perf: query / scan / `Number` get は不変、 text read (`Tag`/`Leaf`) のみ
routing 分岐で ~+1 ns/attr。

**format v5 → v6 / wire は breaking**: v6 は末尾に leaf region を持ち、 `Leaf` sync
は `TieLeaf` に変わる。 **全 peer 同時アップグレード必須** (0.11 peer とは Leaf 経路が
非互換)。 v5 DB は open 自体は可能だが leaf region が無いため **`Leaf` reclaim は
効かない** (= 下記 migration が必要)。

#### Migration ガイド (v5 → v6)

既存 v5 DB を移送して `Leaf` reclaim を有効化する:

```rust
// src は不変、 dst に新 v6 DB を書く (default leaf region size)。
Engine::migrate_file_v5_to_v6("old.ecdb", "new.ecdb")?;
// leaf region size を指定する版 / bytes 版もあり:
Engine::migrate_file_v5_to_v6_with_leaf(src, dst, leaf_data_size)?;
let (v6_bytes, stats) = Engine::migrate_bytes_v5_to_v6(src_bytes, leaf_data_size, &[])?;
```

- 末尾に leaf region を新設し、 各 `Leaf` himo の live cell (旧 vocab vid) を辿って
  vocab bytes を `LeafStore` へ移し、 cell を leaf offset に書換える。 vocab / entity
  / himo / content は byte 単位で引き継ぐ (in-place file 手術ではなく copy + tail
  追加)。
- `.tables` sidecar はコピーし、 **reserved-table の `Leaf` は移送しない**
  (reopen で `.tables` 復元 → vocab 経路に戻るため)。
- **旧 `.oplog` は引き継がない** (dst の stale `.oplog` は削除)。 v5 の `Leaf` tie op
  は旧 wire 形で移送後の cell と不整合になり replay が巻き戻すため、 dst は fresh
  oplog で開く (= main file の現在状態を checkpoint とみなす)。
- **既知 trade-off**: 移送した旧 `Leaf` bytes は vocab に orphan として残る
  (`stats.vocab_orphan_bytes_left`)。 vocab 自体の compaction は本 migration の
  対象外 = 目的は「以後の `Leaf` 書込みを reclaim 対象にし、 成長を止める」こと。
  既に満杯の DB は「dead vocab (一度きりの sunk) + reclaim される `LeafStore`」に
  なり成長は止まる。 dead vocab の回収が要るなら別途 vocab-compact。

**検証**: `LeafStore` churn の footprint 有界性 (free 無効化で growth 再現の
falsify 付き)、 `issue54` の orphan test 更新 (`Leaf` は vocab を汚さない)、
`issue88_migration` 4 test (移送 read 整合 / reclaim 稼働 falsify / already_v6
no-op / file 非破壊)、 sync の `TieLeaf` 収束、 全 workspace test green。

## 0.11.1 — 2026-07-06

### Added — postings-only な `.etxt` build + 生候補 API (#84 の第一歩): 索引が本文を二重化しない

全文検索の `.etxt` (ETXT) が原文を自前保持していた分を、 DB 本体の本文 (0.9.0
#81 の `_c_` Leaf 値) と二重化しない経路を追加。 driving consumer (naruhodo 判例)
では incremental index ではなく **この冗長性解消**が #84 の実要件だった。

- `TextSearch::save_postings_only` / `write_to_postings_only` (下層 `NgramIndex` /
  `storage` にも): Doc Index / Text Data を省いた原文非保持 index を書き出す。
  substring 検証は caller が DB 本体の原文で行う前提
- `TextSearch::candidates` (生 bigram 候補、 `.contains()` 検証なし) を公開 —
  postings-only index で偽陽性除去を caller 側に委ねる入口
- `has_text()` を `MappedIndex` / `NgramIndex` / `TextSearch` に追加
- postings-only index を `open_mut` / `from_bytes_mut` で in-memory rebuild
  しようとすると `Unsupported` で弾く (原文が無い = index からは再構築不可、
  source から作り直す方針)

**format 互換**: ETXT header の `reserved[0]` に `FLAG_TEXT_OMITTED` を立てるだけで
**version bump なし**。 原文保持の書き出しはバイト等価、 旧 reader も postings-only
file を `doc_count=0` として無害に読める (非 breaking = patch)。 consumer 側
(naruhodo の `build_hanrei_etxt` / search handler) の差し替えは別途。

## 0.11.0 — 2026-07-06

### Added — 逆写像 (request10 / #76 根治): write-back 正式サポート、 multi-writer p2p 完成

どの peer もどの entity に書けるようになり、 衝突はカード (himo) 単位の HLC LWW
が裁く。 0.9.0 #76 の single-writer guard (= translated foreign entity への
write は local-only) を根治で撤去。

- `EidTranslator` に逆写像 (translated local → 元 entity の世界番号) を追加。
  `.eidmap` sidecar から両方向を復元 (**sidecar format 不変**)
- 翻訳キーを record の書き手から **eid の産みの親 (`eid_peer`)** に統一 —
  `Engine::resolve_remote_eid` / `resolve_remote_eid_existing` から author 引数を
  撤去 (= engine public API breaking)
- oplog → `_sync_ops` bridge が replica への self-authored write を **元 entity の
  世界番号に宛名を書き戻して再署名・発送** (`oplog::resign_with_eid` 新設。
  lsn / HLC / author は維持 = LWW identity 不変)
- **残る制約**: Ref 値が translated local を指す write は発送されず local-only +
  一度だけ warn (wire の u32 value に世界番号 u64 が入らない、 wire 拡張の
  follow-up)。 0.10.x まではこの経路が silent に断片化していたので封鎖でもある
- **全 peer 同時アップグレード必須**: wire format は不変だが意味論が変わる
  (0.10 peer は書き手キーで翻訳するため、 非 author write を受けると断片化する)
- 検証: 3 peer 収束 / reopen 永続 / LWW 双方向 round-trip / ref guard の 4 test
  (`writeback_reverse_eidmap.rs`) + falsify 実演 (逆写像を無効化すると収束系
  3 test が「元 entity に着弾しない」で正しく落ちる)

## 0.10.0 — 2026-07-06

命名の一括整理 release。 breaking rename 2 本 (`TenantView → Scope` /
`HimoType → ValueType`) を同梱。 **file format / wire format は完全不変 —
データ migration・peer 同時アップグレードとも不要**、 ソースコードの機械的
置換のみで移行できる。

note: 0.9.0 の migration ガイドで 「0.10 予定」 と言及していた逆写像実装
([[request9]]) は本 release には**入っていない** — 0.11 以降にスライド
(replica write-back の local-only 制約は 0.9.0 記載のまま)。

### Changed — `HimoType` → `ValueType` rename

engine の `HimoType` は 「himo の型」 に見えて実は 「value (= カード裏面) の
格納方式」 を選ぶ enum だった (名前と実態の乖離、 glossary §12.10)。 全 crate で
rename、 **variant (`Number` / `Tag` / `Leaf` / `Ref`) は不変**。

| 旧 | 新 |
|---|---|
| `HimoType` | `ValueType` |
| `Engine::himo_type(himo)` | `Engine::value_type(himo)` |
| `Engine::himo_type_at(idx)` | `Engine::value_type_at(idx)` |

schema 層の `ColumnType` は名前・variant とも不変 (column は schema 層の
正規語彙のため)。

### Changed — `TenantView` → `Scope` rename (#24)

`enchudb-schema` の tenant view API を rename。 **schema crate の public API
breaking (= minor bump 必須)、 file format / wire format / engine API は完全
不変 — データ migration は不要**で、 下記のソースコード置換のみ。

「Tenant」 という use-case 名が 「engine 内に tenant 概念がある」 という誤読を
実際に引き起こしたため (#24 起票の経緯)、 機構そのものの名前に変更: 実体は
**table 名前空間の prefix レンズ**であり、 multi-tenant はそのユースケースの
1 つにすぎない。 旧 `as_view` の 「view」 も SQL VIEW (仮想 row 集合) と紛らわしい
ため同時に廃止 (「view」 という語は将来の仮想 row 集合系機能のために温存)。

#### Migration ガイド

機械的置換で完了する (挙動変更なし):

| 旧 | 新 |
|---|---|
| `TenantView<'a>` | `Scope<'a>` |
| `TenantViewMut<'a>` | `ScopeMut<'a>` |
| `db.tenant(name)` | `db.scope(name)` |
| `db.tenant_mut(name)` | `db.scope_mut(name)` |
| `db.as_view()` | `db.as_scope()` |
| `db.as_view_mut()` | `db.as_scope_mut()` |

`Scope::prefix()` / `get_table` / `list_tables` の意味論・prefix 規約
(`{name}.`)・overhead は不変。 example は `tenant_view_demo` → `scope_demo` に
rename。

## 0.9.0 — 2026-07-03

content store を Leaf himo に統合する構造改定 (#81) + 2026-07-03 全体監査
(31 findings、 GitHub #74-#79) の一括修正。 **wire format 変更 (`Op::TieNamed`
追加) と挙動変更 (create/INSERT のエラー化) を含むため minor**。 DB file format
は不変 — 既存 DB はそのまま開ける (migration 手順は下記)。

### Migration ガイド

- **全 peer 同時アップグレード必須**: sync wire に `TieNamed` (tag 6) が増えた。
  0.8 peer は未知 op として skip するため、 混在運用すると content 系 op が
  サイレントに落ちる。 wire version 混在は非サポート (運用判断 2026-07-03)。
- **既存 DB file はそのまま開ける**: 旧 content region は読み取り専用アーカイブ
  として凍結され、 `get_content` は新経路 (`_c_{key}` himo) → 旧 region の順で
  read-through する。 データ書き換え・再構築は不要。 新規書き込みは全て Leaf 側に
  入る (旧 region が育つことはもう無い)。
- **エラーハンドリングの追加が要る箇所**:
  - `Engine::create*` は既存 path に対して `io::ErrorKind::AlreadyExists` で
    失敗する (今までは既存 DB を**サイレントに clobber** していた)。 「あれば
    開く、 なければ作る」は open → 失敗時 create に書き換えること。
  - SQL の plain `INSERT` は重複 PK で `DuplicatePk` エラーを返す (今までは
    重複行がそのまま入った)。 upsert 意図なら従来通り `INSERT OR REPLACE`。
- **`_c_` prefix の列名は予約**: schema 層の列名 validation が `_c_` 始まりを
  reject する (content 互換 himo の名前空間)。
- **レプリカへの write-back は local-only (#76)**: 翻訳済み foreign entity への
  非 author peer の書き込みは、 その peer のローカルでは効くが **sync には
  流れない** (silent 発散を guard で停止 + 初回 warn)。 双方向編集が要る場合は
  0.10 予定の逆写像実装 ([[request9]]) を待つこと。
- **制約 (known limitation)**: concurrent DB への Ref 列追加は不可 (`add_column`
  が Unsupported を返す)。 `add_column` の再実行は名前一致のみで冪等判定し、
  型違いは検出しない。

### Changed — content store → Leaf 統合 (#81, #79 の根治)

- **`content()` / `content_async` / `get_content` は互換 API として維持**しつつ、
  実体を「`_c_{key}` Leaf himo の lazy 定義 + tie」に変更。 consumer 16 repo は
  再コンパイルのみで移行 (key が静的リテラルのみである事は横断調査で確認済み)。
- 専用 content store 構造 (mod-16 key hash index) を書き込み経路から撤去。
  監査で content 系に集中していた 7 findings — key hash 衝突・8B index torn
  read (#79)、 hash15 tombstone sentinel 衝突、 Content reorder buffer 非永続 +
  eviction ロスト (#78)、 sync `content()` の WAL 漏れ、 delete 時の content
  残留 — は**構造ごと消滅**。
- WAL/wire の `Op::Content` は emit 廃止 (decode は残置 = 旧 oplog は読める)。
  代わりに `Op::TieNamed` (op_type 7 / wire tag 6) を追加: himo full name +
  kind + vid を運ぶ self-describing tie で、 peer 間で himo_id 空間が揃わない
  動的定義 himo を同期できる。 受信側は `ensure_himo_named` で lazy 定義。
- sync の Content reorder buffer (0.8.19) を撤去 — TieNamed は自力で entity
  写像を作れるため退避が不要になった。
- 内部: himo registry を `AppendVec` (固定 capacity・lock-free read) 化し、
  `&self` で himo を動的定義できるようにした (`ensure_himo_dynamic`)。

### Fixed — 監査 findings (#74-#79)

- **#74 (critical)**: `GrowableMap::grow_to` の並行呼び出しで ftruncate が
  file を**縮小**しえた (mmap 済み領域が SIGBUS / silent data loss)。 grow を
  Mutex 化し、 fstat で現サイズ未満への ftruncate を禁止。
- **#75 (critical)**: oplog の同一プロセス並行 append が flock (open file
  description 単位 = プロセス内は素通し) で直列化されず、 offset 衝突で record
  を相互破壊しえた。 プロセス内 `append_lock` Mutex を flock の前段に追加。
- **#77 (durability/並行性)**: `open_readonly` が index rebuild で mmap に
  書いていたのを真の非破壊に (shadow index へ rebuild)。 recovery は body
  msync 完了後にのみ checkpoint を前進。 checkpoint head は Commit append 前に
  snapshot、 cursor は committed 終端まで。 `next_sync_lsn` を open 時に
  rehydrate。 EntitySet bitmap を AtomicU8 化 (free の二重投入防止)。 consumer
  thread panic を poisoned 状態として伝搬 (無限 spin 廃止)。 vocab 4 GiB 超の
  create を reject。 flush 後最初の write で clean flag を確実に落とす。
- **#78 (sync)**: 署名/ACL reject された record を pull cursor が飛び越えて
  永久 gap になる問題 — cursor を `min_rejected_hlc` で clamp し次回再検証。
  HTTP bootstrap に `.eidmap` / `.tables` sidecar を同梱 (`GET
  /bootstrap/{eidmap,tables}`、 旧 server は 404 → fallback)。 Delete 適用
  条件の doc を実装に合わせ訂正。
- **#76 (sync)**: レプリカ側で翻訳済み entity に書いた op が author peer の
  op として bridge され silent 発散する経路を guard (local-only 化 +
  warn-once)。 逆写像による本対応は 0.10 候補。
- **#79 / #59 (API 契約・破損耐性)**: create 系の clobber (上記 migration 参照)。
  oplog open 時の header 検証 + truncate された末尾の安全 recover。 未知
  op_type の警告を rate-limit。 engine header の sanity check (CRC=0 経路の
  bounds)。 ngram `MappedIndex` の全 bounds 検証 (panic → `io::Error`)。 blob
  put の fsync + 既存 blob の内容検証。 query_lang `~ <eid>` の存在チェック。
- **#73 (schema)**: 既存 table への列追加 — `add_column` API と、 既存 table を
  含む superset での `define_table` 再宣言が末尾列を自動 migration する経路を
  追加。 名前衝突で型が異なる再宣言は `SchemaConflict` で loud に失敗。
  concurrent (oplog) DB でも列追加可 (Ref 列を除く、 上記制約参照)。
- **`oplog_sync` の同期契約 2 件** (0.9.0 release 検証中に sync テストの稀な
  取りこぼしとして発見・根治):
  - `flush_writes` が op queue の apply しか待たず、 WAL record queue (op 先行・
    record 後追い #77-H4) に record が残ったまま返る窓があった。 直後の
    `oplog_sync` が record の入っていない WAL に Commit + fsync + checkpoint し、
    「fsync 済みのはずの write」が crash で消える / sync 転送から 1 tick 消える。
    WAL record queue にも push/append counter 対を追加し両 barrier を待つ。
  - `oplog_sync` (caller thread) は checkpoint を進めるが `_sync_ops` への
    transfer をしないため、 consumer tick の ring reset (head == checkpoint で
    発火) が bridge 未了の committed record を wipe しえた — その record は
    **sync から永久に消える**。 `oplog_sync` 内と consumer の `try_reset` 直前の
    両方で transfer を走らせ、 「bridge が追いつくまで ring を畳まない」を
    構造的に保証。 回帰テスト:
    `oplog_sync_bridges_all_records_pushed_before_it`。
- SQL: `OFFSET` を実装。 `DISTINCT` / `GROUP BY` / `HAVING` は silent に
  無視せず `Unsupported` を明示 return。
- RAG: reopen 時に BM25 index を再構築 (今までは空のまま検索 0 件)。
- ACL: 未配線なのに permission 制御があるかのように読めた docs を実挙動
  (in-memory・非永続・未 enforce) に訂正。 実装は需要が出たら別 request。

## 0.8.21 — 2026-07-03

同一プロセス writer 二重 open の無期限 flock ハングを fast-fail に変える bugfix (#80)。
file format / wire format 不変。 挙動変更は「ハングしていたケースがエラーを返す」のみで、
正常動作していたコードには影響しないため **patch** として release。

### Fixed

- **同一プロセスで writer を二重 open すると flock(LOCK_EX) で無期限ブロックする**
  ([#80](https://github.com/Mutafika/enchudb/issues/80)): flock は open file
  description 単位のロックのため、 同一プロセスからの 2 回目の writer open は
  block 検知できず、 fast-fail もタイムアウトも無い診断不能なハングになっていた。
  プロセス内 registry (canonicalize した lock path の set) を flock の前段に追加し、
  重複 open は `ErrorKind::WouldBlock` +
  `"... is already open for writing in this process"` で**即エラー**を返す。
  - **別プロセス** writer との排他は従来通り blocking flock (sqlite 互換、 不変)。
  - readonly open は writer lock を取らないので従来通り併存可。
  - migration: 同一プロセス二重 open の旧挙動は実質デッドロックなので、 依存して
    いた正常コードは無いはず。 pool / cache 層で並行 cold-open が起き得る consumer
    は `WouldBlock` を catch して既存ハンドルの reuse (single-flight) に繋ぐこと。
  - 「既存の共有ハンドルを返す」 (プロセス内 open-file レジストリの完全形) は
    第 2 段として別途検討。

## 0.8.20 — 2026-06-26

`enchudb-text` を `enchudb-ngram`(primitive) + `enchudb-textsearch`(policy) に分離する
refactor (#69/#70)。 crate 名は変わるが file format / magic `ETXT` は不変で既存 `.etxt` は
そのまま読めるため、 データ互換は保たれる **patch** として release。 downstream は dep
差し替え + 型名リネームのみ。

### Changed

- **`enchudb-text` を `enchudb-ngram`(primitive) に改名し、テキスト検索を
  `enchudb-textsearch`(policy) に分離**
  ([#69](https://github.com/Mutafika/enchudb/issues/69)): `enchudb-text` は実体が
  bigram 部分一致エンジンだが名前が「検索」という正体を隠していた。lawgraph の機械検索で
  断片 `出力` が `入出力` の部分文字列として無関係条文を引き込むノイズ調査から、用途が
  逆である事が判明（**人間の対話検索 = 部分一致が正解** `接地`→`接地極` ／ **機械 =
  フレーズ完全一致が欲しい**）。これは bug でなく substring の正しい挙動なので、関心を
  分離した:
  - `enchudb-ngram` = index プリミティブ。bigram 抽出 / posting / intersect →
    **候補 doc id**（`NgramIndex::candidates` / `scan`）。検索意味論は持たない。
  - `enchudb-textsearch` = ポリシー。候補 + `.contains()` 検証 → 正確な部分一致
    （`TextSearch::search`）。クレート名は「`text` が検索を隠す」不満を直す意図で
    `textsearch`（= search over text）。機械向けフレーズ完全一致は入力フレーズを 1 単位で
    渡せば同じ path で扱える（issue option (a)、専用 `enchudb-phrase` は未実装）。
  - file format / magic `ETXT` は不変。既存 `.etxt` はそのまま読める。
  - 旧 `TextEngine` は `TextSearch` にほぼ同型で移行（dep 差し替え + 型名リネーム）。
    downstream（`lawgraph-explorer` / `naruhodo` / `bisquit`）は別 repo で dep 差し替えが要る。

## 0.8.19 — 2026-06-23

cross-peer eid 翻訳 (#9)。 foreign eid のサイレント上書きを直す bugfix。 public API +
`.eidmap` sidecar を足すが file format / wire format は不変、 既存 DB は再 build のみで
上がれる (sidecar 不在 = 空 translator = 旧挙動)。 完全 backward-compatible・migration
不要のため **patch** として release。

### Fixed

- **cross-peer sync で foreign eid がサイレント上書きを起こす**
  ([#9](https://github.com/Mutafika/enchudb/issues/9)):
  EntityId は peer ごとの空間だが、 `Syncer::apply_one` が受信 record の eid を
  翻訳せず raw で apply していた。 foreign eid の local 部が受信側の既存 entity の
  local slot と衝突すると、 その entity をサイレントに上書きしてデータを失っていた
  (LWW の `HlcStore` も foreign eid を local として keying していた)。 engine に
  `EidTranslator` (`(author_peer, foreign_local) → local_eid` 写像) を内蔵し、 apply
  時に 4 op (Tie/Untie/Delete/Content) 全てを翻訳。 初見の foreign entity には himo の
  table 内に fresh な local eid を払い出す (= local entity と同じ allocator、 衝突しない)。
- **cross-peer ref が壊れる** (#9): Ref himo の value 自体が foreign target eid なので、
  同じ translator で ref の target table 空間に翻訳。 forward ref も target entity 自身の
  Tie と同じ local に収束する。

### Added

- `Engine::resolve_remote_eid` / `resolve_remote_eid_existing` / `himo_is_ref` /
  `resolve_remote_ref_value` / `eid_translator` — sync 層が apply 時に foreign eid を
  翻訳するための primitive。
- `.eidmap` sidecar — 翻訳写像を `.tables` と同じ trigger で atomic 永続化。 reopen /
  `snapshot_export` で復元され、 再 sync で重複 entity を払い出さない。

### Hardened (post-merge review pass)

- **破損 sidecar で open 時 OOM abort を防ぐ**: `.eidmap` / `.tables` の deserialize が
  header の `count` を信用して `Vec::with_capacity(count)` していた。 torn / 破損で巨大な
  count を引くと数 GB の確保要求で process が abort しえた。 `count` を残りバッファ長で
  cap し、 `.eidmap` は上限超過を破損とみなして空 translator に fallback。
  回帰テスト: `huge_count_eidmap_sidecar_does_not_oom`。
- **`snapshot_export` の sidecar 整合性**: body msync 後に古い on-disk `.tables` /
  `.eidmap` をコピーしていたため、 直近 consumer tick (≤100ms) 以降に翻訳された entity が
  snapshot の `.eidmap` に載らず、 restore 後の再 sync で重複 entity を払い出しえた。 copy
  前に現 in-memory 状態を再 persist して body と sidecar を整合させる。
- **`peer_id == 0` で sync する foot-gun を検知**: 未設定 (= 0) の node 同士が sync すると
  author 0 == self 0 が identity 翻訳に落ち #9 の衝突が再発する。 `apply_records` が
  self_peer == 0 で foreign record を apply する時に一度だけ警告する。 own-op replay は
  author == self が正しく identity なので翻訳 semantics は変えず、 設定漏れだけ surface する。
- **Content-before-Tie の配送順序ロスを解消**: entity の Tie より先に Content が別 pull で
  届くと、 確保先写像が無く skip → cursor 前進で永久ロストしていた。 未着 entity 宛の
  Content を `(author_peer, foreign_local)` 別の pending buffer へ退避し、 対応する Tie /
  Untie が写像を作った直後に drain して apply する。 Content は key 単位 LWW で他 op と
  独立なので遅延適用しても可換。 buffer は `MAX_PENDING_OPS` で bound。 回帰テスト:
  `content_before_tie_is_buffered_then_applied`。
- **foreign Delete tombstone を `.eidmap` v2 で永続化 (削除済み entity の復活を防止)**:
  `HlcStore` は永続化されず gossip-off では foreign op が local oplog に載らないので、
  reopen 後に foreign tombstone が消え、 削除済み entity が stale Tie で復活しえた。 `.eidmap`
  を v2 に拡張し各写像 entry に foreign Delete の HLC を載せて persist、 reopen 時 (peer_id は
  header から復元済み) に HlcStore tombstone を seed する。 v1 ファイルは tombstone 無しとして
  読める (後方互換)。 回帰テスト: `foreign_delete_tombstone_survives_reopen`。

### Notes / 残る制約

- 翻訳写像は **peer-local**。 oplog / sync wire には載らない (各 peer が独立に翻訳)。
- **`set_peer_id` を sync 前に必ず呼ぶこと**: multi-peer では各 node に非 0 の peer_id を
  設定する (未設定だと上記 foot-gun ガードが warn する)。
- **foreign LWW watermark の永続化は tombstone のみ**: 削除の復活 (resurrection) は v2 で
  塞いだが、 非 tombstone の per-himo watermark は依然 reopen 時に local oplog からのみ
  再構築する。 full re-pull は HLC 順序非依存で収束するので通常は問題ないが、 stale な
  非削除 Tie が partial に再配送されると一時的に古い値が載りうる (= 次の新しい op で
  上書きされる、 削除復活より軽微)。 完全な watermark 永続化は follow-up。
- **配送順序の残り**: Content は buffer したが、 **Tie より先に届いた Delete** (新しい
  Delete が entity の Tie より先に来るケース) は依然 skip する (Delete を buffer すると
  tombstone-slot LWW と順序干渉するため意図的に保留)。 一度も Tie されない content-only
  entity 宛の Content は cap まで buffer に残る。 汎用的な retry は follow-up。 なお Untie は
  確保 (`resolve_remote_eid`) 経由で slot を anchor し untie HLC を記録するので、 out-of-order
  な古い Tie は LWW で正しく弾かれる (skip ではない)。
- **cross-peer ref は phantom target を作る**: ref value は wire に target の local 部しか
  載らない (peer bits drop)。 ref が **author 以外の peer** の entity を指す場合、 翻訳は
  `(author_peer, local)` で誤った peer に解決し target table に phantom entity を払い出す。
  本物の target が後で sync されても別 slot に落ちて収束しない。 現状 **同一 peer 内 ref のみ**
  正しく sync される。 wire に ref target peer を載せる format 拡張は follow-up。
- `.eidmap` / `.tables` に per-file CRC は無い。 破損 / torn (bogus huge count 含む) は parse
  失敗 → 空 fallback + 再 sync で復旧 (open は abort しない)。 CRC 付与は将来候補。
- `gossip_remote_apply` が ON の場合の relayed append は現状 local_eid で append する。
  gossip 転送の厳密な正しさには元の foreign eid で append すべきで、 別 commit で body-eid /
  relay-eid を分離予定。 default は off。

## 0.8.18 — 2026-06-22

robustness fix 4 件。 **file format / wire format / public API いずれも不変**、
0.8.17 から再 build のみで上がれる patch release。

### Fixed

- **oplog ring buffer が production で reclaim されず 16MB で全 append が drop**
  ([#63](https://github.com/Mutafika/enchudb/issues/63) /
  [#64](https://github.com/Mutafika/enchudb/pull/64)):
  `try_reset` が `auto_reset` フラグで gate されていたが、 `set_auto_reset(true)`
  は test でしか呼ばれず production では常に no-op。 ring が一度も reset されず
  `head` が 16MB (= 既定 oplog capacity) に達した時点で以降の append が全て
  `WAL full` で drop され、 long-running writer の変更が静かに失われていた。
  gate を撤去し、 `head == checkpoint && pending == 0` の領域を無条件 reclaim。
  consumer tick / graceful drain の両経路で発火。
- **ring reset 後に書いた record が sync から無言で欠落** (#64 の回帰):
  `try_reset` が head/checkpoint だけ巻き戻して bridge cursor (`sync_ops_offset`)
  を放置するため、 reset 後の append が `_sync_ops` へ転送されず `publish_since`
  が取りこぼしていた。 `Engine::reset_sync_ops_offset()` を追加し try_reset 成功時に
  cursor も巻き戻す。 回帰テスト `records_after_ring_reset_are_still_synced` を追加。
- **readonly open が DB を dirty 化する** ([#56](https://github.com/Mutafika/enchudb/issues/56)):
  `open_readonly` でも共通 open path が無条件で clean flag を 0 に倒し msync して
  いたため、 read-only のはずの open が file を物理的に書き換え、 次回 open で
  full index rebuild を誘発して DB を太らせていた (wiki.ecdb: live ~70KB に対し
  physical 155MB)。 readonly では clean flag を一切触らない真の非破壊 open に。
  (② Drop で flush せず / ③ dirty rebuild の予約全域 zero-fill は別途。)
- **schema upsert の PK 一意性が並行で破れて重複行ができる**
  ([#60](https://github.com/Mutafika/enchudb/issues/60)):
  並行 mode で 2 thread が同一 PK を upsert すると、 両方が lookup→allocate で
  別 eid を払い出す TOCTOU で重複行ができていた。 `TableInner` の per-table
  `upsert_lock` で lookup→allocate→PK tie を直列化 (別 table は並行のまま)。
  16 thread が同一 PK を同時 upsert → 1 行になる回帰テストを追加。
  (SQL 層は独立した別 table 実装のため対象外。)

### Notes

- [#36](https://github.com/Mutafika/enchudb/issues/36) (`schema_meta_entity`
  panic) は 0.8.7 で既に撤去済みだったため、 再現シナリオを test で確認した上で
  close。 コード変更なし。

## 0.8.17 — 2026-06-10

全コード監査 (issue #57–#61) で洗い出した **設計判断不要・後方互換** な堅牢性
fix を 1 本にまとめた safe cluster。 file format / wire format 完全不変、
**0.8.16 から再 build のみで上がれる**。 header CRC 拡張 (#58①) / PK 一意性の
所有 (#60) / capacity panic の Result 化 (#59 の API 破壊部分) は format or API
変更を伴うため本 release から除外、 別途。

### Fixed

- **oplog append 失敗が silent な op 欠落になる** ([#57](https://github.com/Mutafika/enchudb/issues/57)):
  engine の tie/untie/delete 経路は `let _ = wal.append(..)` で失敗を握り潰す。
  oplog 容量到達時 (`OutOfMemory`) に **mmap body は更新されるが op が stream に
  載らない** ため、 `publish_since` で tail する peer が変更を恒久的に取りこぼす。
  完全な伝播 / 修復経路は別 issue だが、 まず**検知可能**にするため
  `OpLog::wal_full_err()` の単一地点で **1 秒 1 行** rate-limit の warning を
  emit (0.8.15 の persist warning と同温度)。 silent loss を止める。
- **`OpLog` の `pending_writes` が panic で leak** ([#58](https://github.com/Mutafika/enchudb/issues/58) ②):
  `fetch_add` → `append_inner` → `fetch_sub` 直列のため `append_inner` panic で
  counter が +1 のまま残り、 `try_reset` (`pending_writes == 0` 条件) が永久に
  発火しなくなる。 RAII ガード (`pending_guard`) で panic 経路でも均衡させ、
  `append` / `append_many` / `append_relayed` の 3 経路に適用。
- **schema sidecar の serialize がエスケープなし** ([#61](https://github.com/Mutafika/enchudb/issues/61)):
  `.schema` の format は table/column 名を `|` `;` `:` 改行・ relation `->` で
  連結するため、 名前にこれらが入ると round-trip で schema 破損。
  `TableBuilder::build()` で table 名 + 全 column 名を `validate_schema_name` で
  検証し、 予約文字を含む名前を `BadValue` で弾いて silent corruption を止める。
- **FFI が `catch_unwind` ゼロで panic が UB に化ける** ([#59](https://github.com/Mutafika/enchudb/issues/59) の安全部分):
  engine 層の panic (capacity / 破損 file / edge 値) が `extern "C"` 境界を
  unwind して越えると未定義動作。 engine に触れる 6 関数 (`open` / `create` /
  `close` / `exec` / `query` / `result_free`) を `catch_unwind` でくるみ、
  panic を error code に潰す。 result accessor 8 関数は bounds-check 済みの
  materialized data を読むだけで panic しないため guard 不要。

### Tests

各 fix の **新挙動**に対する回帰テストを追加 (= 既存テストの「壊れてない」ではなく
「fix が効いてる」を固定):

- #61 — `issue61_name_validation` 5 件 (予約文字を弾く / 正常名は通る)。
- #57 — `append_returns_err_when_full_not_panic`: 極小 capacity で満杯にして
  `append` が panic せず `OutOfMemory` の `Err` を返すことを固定。
- #58② — `pending_writes_balanced_on_append_panic`: `#[cfg(test)]` の fault
  injection で `append_inner` を deterministic に panic させ、 RAII ガードにより
  `pending_writes` が leak しない (counter が 0 に戻る) ことを検証。 旧来の
  `fetch_add → inner → fetch_sub` 直列ならこのテストは fail する。
- #59 — `guard_i32_converts_panic_to_error_without_aborting` /
  `guard_i32_passes_through_normal_return`: FFI guard が closure の panic を
  `ENCHUDB_ERROR` に潰し process を abort させない / 正常時は素通しすることを固定。

### 除外 (別 issue / 設計判断あり)

- #58① record header の CRC 拡張 — 既存 `.oplog` の CRC 再計算が必要で
  on-disk 互換に影響。 `REC_VERSION` gate 込みで別途。
- #60 PK 一意性の所有層決定 (SQL 素 INSERT 無検査 + schema upsert TOCTOU) —
  engine が atomic upsert primitive を持つか「保証しない」と明文化するかの判断。
- #59 capacity panic (`entity_set` / `content_store` / `vocabulary`) の Result 化 —
  public write API の signature 破壊を伴う。

## 0.8.16 — 2026-06-09

`HimoType::Leaf` の re-tie / remove で発生する vocab orphan (= 死蔵 vid) を
**読み取り専用**で実測する `Engine::vocab_orphan_stats()` API + CLI `.orphans`
を追加。 issue [#54](https://github.com/Mutafika/enchudb/issues/54) の scope に
合わせ **検出のみ**、 reclaim / compact は将来 release。 file format / wire
format 完全不変、 **0.8.15 から再 build のみで上がれる**。

### Added

- `enchudb_engine::VocabOrphanStats` struct (`vocab_total` / `live_vids` /
  `orphan_vids` / `live_bytes` / `orphan_bytes` + `dead_ratio()` helper)。
- `Engine::vocab_orphan_stats()` — Tag / Leaf 全 himo の `unique_values()` を
  union して live vid 集合を作り、 `(0..vocab.count())` との差を orphan として
  返す。 vocab / himo は一切変更しない pure read-only。 計算量は
  `O(vocab_total + Σ unique_values.len())`。
- CLI `.orphans` — REPL の dot command として上記 stats を表示。

### 背景

`vocab.insert` (= Leaf 用の dedup なし append) は re-tie / remove で旧 vid を
回収しないため、 long-lived な curated store (= 元ソースから rebuild しない
タイプ、 例: opyula の memory / room store) で vocab data が単調増加。 opyula
`wiki.ecdb` は live 45 entity に対し物理 155 MB (~3.4 MB/entity) と観測され
ており、 大半が orphan と推定 (= この API で初めて実測可能に)。

### scope 外 (follow-up)

- vocab `compact()` API: live vid だけ残して data / offsets / lookup index を
  詰め直す reclaim 経路。 oplog watermark との grace (= 未消化 WAL が旧 vid を
  replay に要する間は free 不可) が前提。 別 issue で扱う。
- 大規模 DB 向けの BitVec / streaming 化: 現実装は `Vec<bool>` で
  `vocab.count()` bit 確保するため、 vocab 1B vid で 1 GB RAM。 巨大 DB は
  別 issue で対応。

## 0.8.15 — 2026-06-04

ENOSPC 起因の warning スパムと sidecar 破損時の DB 読取不能 (issue
[#52](https://github.com/Mutafika/enchudb/issues/52)) を fail-readable / self-heal
で根治。 file format / wire format 完全不変、 **0.8.14 から再 build のみで
上がれる**。

### Fixed

- **persist warning の rate-limit** (= ターミナル不能化): `try_persist_tables`
  の失敗時 `eprintln` を **1 秒 1 行**に rate-limit (`Engine.last_persist_warn_ms:
  AtomicU64` + CAS 前進)。 disk full 等で consumer thread が毎 batch 失敗 →
  ターミナルに warning が秒間数百件流れる現象を抑止。 メッセージ末尾に
  ` (rate-limited to 1/s)` を付与して抑止中であることを明示。
- **sidecar 破損で全 DB 読取不能** (= 致命的 fail-closed):
  - `.tables` (engine sidecar) parse 失敗を `InvalidData` で識別、
    `.tables.corrupt-<unix_ts>` に rename して退避 → anonymous fallback で
    engine open を続行 (`crates/enchudb-engine/src/engine.rs`)。
  - `.schema` (schema sidecar) も同様に parse 失敗を catch、
    `.schema.corrupt-<unix_ts>` に rename → 下流の legacy blob / engine
    synthesize fallback に流す (`crates/enchudb-schema/src/lib.rs`)。
  - これまで `.schema` 破損は `Database::open` 全体を fail させていたため、
    sinfo / opyula 等の consumer が **disk full からの recovery 後も DB を
    全く開けなくなる** 状態に陥っていた。 今回 fail-readable 化で engine の
    `list_user_tables` から table 定義を再 synthesize できるようになり、
    破損 sidecar を「警告 + 退避ファイル」 として処理して継続。
- **`.tables.tmp` / `.schema.tmp` self-heal**: open 時に残骸 tmp file を明示削除。
  既存 persist 経路は `truncate(true)` で上書きするため通常は不要だが、 disk
  full → recovery 後の確実な clean state を保証する safety net。

### 影響範囲

すべての user が恩恵。 特に:
- 高頻度 write workload (= opyula / sinfo / suzukapulse 等 ingest 系) が
  ENOSPC を踏んでも terminal を失わない。
- disk full → recovery で `.schema` 破損が起きた DB を、 再 deploy なしで
  そのまま再 open 可能 (= synthesize で table 復元)。

### 残課題 (将来 release)

- mmap body (`.ecdb`) は default 16M entities で予約サイズ ~25 GiB (sparse)。
  ENOSPC で touched page が全 reserve に行き渡って疎→密に化けるため、
  default を小さく (= `create` の最小値を採用) するか lazy growth に
  変更する検討は別 issue 推奨。
- WAL append が ENOSPC で partial write を残す可能性 → atomic-rename と同等の
  fsync-before-checkpoint pattern は維持されているが、 head pointer の rollback は
  入っていない。 mid-record corruption は WAL recover (`OpLog::recover`) が
  最後の commit までで truncate するので read path には影響しない。

## 0.8.14 — 2026-06-03

`TableBuilder::cardinality(n)` を追加。 schema 列に distinct 値数の hint を渡せる
ようになり、 **その列を group key にした集計 (`group_sum` / `group_min` /
`group_max` / `histogram`) が dense + 並列 fast path に乗る** ([#46])。

これまで `Database::build()` は全列を `define_himo(.., 0)` で定義していたため、
engine の `group_dense_cap` が常に `None` を返し、 table 層の group 集計が HashMap
fallback + 並列無効に固定されていた (= 1M 行の全件 GROUP BY で ~5.2ms)。
`.cardinality(20)` で hint を渡すと dense `acc[g] += v` path に乗り、 同 workload で
**~443µs (seq dense) / ~152µs (par dense)** に短縮、 in-process DuckDB
(~580µs–1.6ms) を上回る。 `examples/vs_db.rs` の 4-way bench (enchudb / sqlite /
duckdb / lmdb) で GROUP BY を含む全 10 項目を enchudb が制覇。

cap=0 (= hint 未指定) の挙動は不変で **後方互換 100%、 既存コードは影響なし**。
reopen でも `max_values` は engine metadata に persist + restore されるため、 build
時に `.cardinality()` を渡せば hint は保持される。

file format / wire format 完全不変、 **0.8.13 から再 build のみで上がれる**。

### Added

- `enchudb_schema::TableBuilder::cardinality(n)` — 直前に宣言した列の cardinality
  hint。 `BucketCylinder` の size hint になり、 `n ≤ 65536` のとき group dense path
  を有効化する。
- `examples/vs_db.rs` に LMDB (heed, in-process mmap KV) を追加して 4-way 化
  (enchudb / sqlite / duckdb / lmdb)。
- `examples/group_sum_cap_probe.rs` — cap 有無での `group_sum` 速度差の probe。

### 既知の制約

- dense path は `n ≤ 65536` (= `cyl_max_values`) の low-cardinality group key 限定。
  高 cardinality group key では HashMap path のまま ([#46] で議論)。
- `Table::add_column` / reopen 経路は依然 cap=0 で define するが、 build 時に
  `.cardinality()` を渡せば reopen は engine 側 persist で hint を保持する。

[#46]: https://github.com/Mutafika/enchudb/issues/46

## 0.8.13 — 2026-06-03

`TableBuilder::build()` を reopen 時 idempotent に。 issue
[#50](https://github.com/Mutafika/enchudb/issues/50) で sinfohub-server が踏んだ
crash-loop bug を根治。 file format / wire format 完全不変、 **0.8.12 から再
build のみで上がれる**。

### Fixed

- **`TableBuilder::build()` が "already exists" を recoverable に扱わない** (=
  migration crash): `load_schema` (`crates/enchudb-schema/src/lib.rs:622-630`)
  は `define_table` の `"already exists"` を recoverable として handle してたが、
  public `TableBuilder::build()` 経路は同じ error を bail させていた非対称。
  multi-tenant shared-pool で deploy 越しに engine sidecar (`.tables`) が
  table 定義を蓄積していく場合、 schema blob (`.schema`) と divergence した
  状態で `db.table("foo").build()` が
  `define_table(foo) failed: table 'foo' already exists` で fail して
  server crash-loop に陥っていた (sinfohub production で観測)。
  既に `v0.8.2-flush-patch` branch に `5ebc5b6` として fix 済だったが master
  に merge 漏れ。 今回 cherry-pick で master に取り込み。
- regression test を `crates/enchudb-schema/tests/issue50_build_idempotent.rs`
  に追加 (= 修正前 fail 確認済み、 修正後 pass)。

### 影響範囲

`Database::open` (or `open_with_oplog`) 直後に `db.table(...).build()` を呼ぶ
migration / re-declare pattern を使う user のみ。 single-shot `Database::create`
+ declare + finalize の通常 flow は影響なし。

## 0.8.12 — 2026-06-03

**CRITICAL data-loss fix**。 sync 経路で foreign peer から届く Tie / Content op を
apply する際、 `next_local` を前進させていなかったため、 後続の `entity_in` が
既に live な local id を再払出 → user の新規 save が既存 entity を上書きする
silent data loss。 issue [#47](https://github.com/Mutafika/enchudb/issues/47) で
bisquit が踏んだ症状。 0.8.5 以降の全 sync user に影響、 0.8.12 へ即時更新を推奨。

file format / wire format 完全不変、 **0.8.11 から再 build のみで上がれる**。

### Fixed

- **`remote_tie_apply` / `remote_content_apply` が `next_local` を進めない** (=
  data loss): WAL recover 経路 (`apply_oplog_op`) は 0.8.1 で
  `advance_table_next_local_for` を呼ぶよう修正済みだが、 sync 経路
  (`remote_*_apply`) には同 fix が入っておらず非対称だった。 結果として、
  Mac↔Android 等の peer sync で foreign eid が `entities.ensure_live(local)`
  された後も `table.next_local` が古いまま据置 → 次の `entity_in` が
  `fetch_add(1)` で live local を払出し、 schema 層の `tie_value` で既存 entity
  に被せ書きしてしまう。 両 `remote_*_apply` に
  `Self::advance_table_next_local_for(&self.tables, local)` を 1 行追加して根治。
  regression test を `crates/enchudb-engine/tests/issue47_remote_tie_next_local.rs`
  に追加 (= 修正前 `new_locals=[20, 21, 22]` collide with `foreign_locals=[20, 21]`
  で fail、 修正後 fresh local を返して pass)。

### 影響範囲

sync mode (`open_with_oplog` + `enable_sync`) で、 自 peer が複数の foreign peer
から記録を受信している環境のみ。 単一 peer / 非 sync 利用は無影響。 production
で bisquit (Mac-Android 2 peer) が「Share Intent で URL 追加するたびに 1 件
silent loss」 として観測した重大 bug。

## 0.8.11 — 2026-05-31

`transfer_oplog_to_sync_ops` の **lock 不在** と **自己再帰 sync 循環** の
2 件の sync bug を fix。 stress_10k_cycle test の flaky 化 (= 0.8.0 以降ずっと
潜在化) を根治。 production sync running で `_sync_peers` / `_sync_ops` 自身の
write が peer に飛ばないよう正しく除外される。 file format / wire format
完全不変、 **0.8.10 から再 build のみで上がれる**。

### Fixed

- **`transfer_oplog_to_sync_ops` の race condition** (= 重複転送): 0.8.0 で
  `concurrentize_with_oplog` の background consumer thread が自動 transfer を
  呼ぶようになったが、 手動呼び出しと並列実行すると `from = sync_ops_offset.load`
  → records pull → row insert → `offset.store` の 4 step が race し、 同じ
  records が複数回 row insert される (= reclaim 後の残骸として残る) bug。
  `Engine` に `transfer_lock: Arc<Mutex<()>>` を追加し、 transfer 全期間を排他化。
  lock 競合は per-fsync 頻度 (= 100ms 周期) で hot path 影響なし。

- **自己再帰 sync 循環** (= `_sync_peers` / `_sync_ops` 自身の write が queue に
  入る): `ack_sync` の watermark update が `_sync_peers` table への row write、
  および `transfer_oplog_to_sync_ops` 自体が `_sync_ops` table への row write
  を生み、 これらが WAL に積まれて次の自動 transfer で `_sync_ops` queue に
  入る循環構造だった。 結果として:
  - `_sync_peers` 残骸が peer に sync record として配信される (= local-only
    state なのに transmit、 設計上意味なし)
  - `_sync_ops` 自身が無限ループ的に self-transfer される可能性
  - reclaim 後の残骸蓄積 (= stress_10k_cycle の `final_pending` が ~2500 に
    膨れる根本原因)

  fix: transfer 内の records loop で `_sync_ops` / `_sync_peers` の eid_range に
  入る `Tie` / `Untie` / `Delete` / `Content` op を skip。 `Commit` (= barrier)
  と `Vocab` (= global sync 必須) はそのまま通す。

### Tests

- `crates/enchudb-engine/tests/destructive_0_7_0.rs` の `stress_10k_cycle`:
  手動 transfer の `transferred >= 10_000` assert を `pending_sync_ops().len()`
  ベースに書き換え (= 0.8.0+ semantics で 手動 transfer の return value は race
  で 0 になりうるため)。 10/10 連続実行で安定 pass。

### 互換性

- **file format / wire format 完全不変**
- **0.8.10 から再 build のみで上がれる**
- 既存の `transfer_oplog_to_sync_ops` 公開 API は不変、 内部実装のみ変更
- ack の `_sync_peers` write は変わらず local 永続化される (= 復旧用)、
  ただ peer に sync record として配信されない (= 正しい挙動)

### sync 経路への影響 (= positive)

- production sync running で peer 間 traffic から無駄な `_sync_peers` /
  `_sync_ops` records が消える (= sync queue の純度向上)
- reclaim path の残骸蓄積が解消、 long-running sync で 安定的に sync queue
  size が抑えられる


## 0.8.10 — 2026-05-31

#43 対応。 **Schema `Query` の終端に集計 chain API を追加**。 `where_*` で絞った
sub-set に対する scalar 集計 (`min` / `max` / `sum` / `count_col`) と GROUP BY
集計 (`group_sum` / `group_min` / `group_max`) と `histogram` が、 schema 層の
fluent chain として完結できるようになった。 これまで sub-set 集計をしたいアプリは
engine 直叩き (= cylinder + `pull_himo_stored_many_into` + 手書き loop) に堕ちて
いたが、 schema が責務として持つ layout / 並列化を吸収できる。 file format / wire
format 完全不変、 **0.8.9 から再 build のみで上がれる**。

### Added (Engine、 9 API)

既存 eids 版 (`sum` / `count` / `min` / `max` / `group_*`) は seq のみだったので、
0.8.10 で並列版を追加 + histogram_eids 系を新規追加:

- `Engine::sum_eids_par(himo, eids) -> u64`
- `Engine::count_eids_par(himo, eids) -> u32`
- `Engine::min_eids_par(himo, eids) -> Option<u32>`
- `Engine::max_eids_par(himo, eids) -> Option<u32>`
- `Engine::group_sum_eids_par(group, sum, eids) -> Vec<(u32, u64)>`
- `Engine::group_min_eids_par(group, val, eids) -> Vec<(u32, u32)>`
- `Engine::group_max_eids_par(group, val, eids) -> Vec<(u32, u32)>`
- `Engine::histogram_eids(himo, eids, vmin, vmax, n_buckets) -> Vec<u32>` (seq)
- `Engine::histogram_eids_par(himo, eids, vmin, vmax, n_buckets) -> Vec<u32>`

### Added (Schema `Query` 終端、 8 API)

`where_*` chain の終端として:

- `Query::count_col(col) -> Result<u32>` — sub-set 内で col が tie された数
- `Query::sum(col) -> Result<u64>`
- `Query::min(col) -> Result<Option<u32>>`
- `Query::max(col) -> Result<Option<u32>>`
- `Query::group_sum(group, sum) -> Result<Vec<(u32, u64)>>`
- `Query::group_min(group, val) -> Result<Vec<(u32, u32)>>`
- `Query::group_max(group, val) -> Result<Vec<(u32, u32)>>`
- `Query::histogram(col, vmin, vmax, n_buckets) -> Result<Vec<u32>>`

使用例 (= suzukapulse dominance v3 の書き換え想定):

```rust
// 0.8.9 までは engine 直叩きが必要だった (= schema 層を素通り)
// 0.8.10:
tel.where_eq("session", sess).min("speed")?
tel.where_eq("session", sess).group_min("driver", "speed")?
tel.where_eq("session", sess).histogram("speed", 0, 360, 30)?
tel.where_range("speed", 100, 300).group_max("lap_no", "elapsed_ms")?
```

### 実装方針

- 並列化は `eids.par_chunks(16k)` で chunk 並列、 stored_slice 直 view を
  indirect access (= `col[eid_local(e)]`) で scatter read
- `_range_par` (= sequential SIMD) と違い cache-unfriendly な scatter access に
  なるが、 thread 並列度で稼ぐ
- 閾値 `PAR_RANGE_THRESHOLD = 64_000` 未満では seq fallback (= API 透明)
- Schema `Query` 終端は `find()` で eids を取得 → engine `_eids_par` に bind の
  薄い wrapper、 col 名は `{table}.{col}` の himo 名に解決

### Tests

- `crates/enchudb-engine/tests/eids_par_aggregations.rs` (7 件): par == seq、
  連続 eid 集合では `_range_par` と一致、 飛び飛び (= 不連続) 動作、
  histogram edge case、 閾値以下で seq fallback。
- `crates/enchudb-schema/tests/query_aggregations.rs` (6 件): scalar /
  GROUP BY / histogram の各終端、 空 sub-set、 unknown col の Err、
  `where_range` との組み合わせ。

### 互換性

- **file format / wire format 完全不変**
- **0.8.9 から再 build のみで上がれる**
- 既存 `Query::find` / `find_one` / `count` / `limit` は変更なし
- 既存 eids 版 `Engine::sum/min/max/group_*` は引き続き seq 版として使える

### Reference

- [#43] design: enchudb-schema の Query 層に集計 chain API を追加


## 0.8.9 — 2026-05-31

#39 対応。 bulk column scan の **rayon 並列化** 系 API を `_par` suffix で追加。
12M row scan で `min_range` / `max_range` が **9-15x** 高速化、 `histogram_range`
が **6.2x**、 reduce 系 (`sum` / `count`) が 2x。 callsite が並列で OK と分かって
る場面 (= 大規模 read-only scan、 suzukapulse / mlbpulse の analytical hot path)
向け。 file format / wire format 完全不変、 **0.8.8 から再 build のみで上がれる**。

### Added (Engine、 9 API)

- `Engine::sum_range_par(himo, lo, hi) -> u64`
- `Engine::count_range_par(himo, lo, hi) -> u32`
- `Engine::min_range_par(himo, lo, hi) -> Option<u32>`
- `Engine::max_range_par(himo, lo, hi) -> Option<u32>`
- `Engine::group_sum_range_par(group, sum, lo, hi) -> Vec<(u32, u64)>`
- `Engine::group_min_range_par(group, val, lo, hi) -> Vec<(u32, u32)>`
- `Engine::group_max_range_par(group, val, lo, hi) -> Vec<(u32, u32)>`
- `Engine::range_scan_par(himo, lo, hi) -> Vec<EntityId>`
- `Engine::histogram_range_par(himo, lo, hi, vmin, vmax, n_buckets) -> Vec<u32>`

### 実装方針

- 閾値 `PAR_RANGE_THRESHOLD = 64_000` 要素未満では **seq fallback** (= 既存
  `_range` API を呼ぶ)。 thread spawn overhead が利益を上回らないため、 callsite
  は規模を意識せず `_par` を呼んで良い (= API として透明)。
- chunk 粒度は `16_384` 要素 (= 64KB、 L2 cache friendly)。 `par_chunks` または
  `chunk index` の `par_iter` 経由で並列。
- HimoStore は内部に `RwLock<BucketCylinder>` を持つが、 `stored_slice` は
  immutable な mmap view (= read 中 lock 不要) なので thread-safe。
- group 系の sparse path (= HashMap merge コストが重い) は seq fallback。
  dense path のみ並列化 (= thread-local `Vec<u64>` で scatter add → reduce で
  要素ごと加算)。

### 実測 (12M row、 M2 Max、 12 hardware thread)

| query | seq | par | speedup |
|---|---|---|---|
| `sum_range` | 1.3 ms | 0.7 ms | 1.96x |
| `count_range` | 1.2 ms | 0.7 ms | 1.68x |
| `min_range` | 16.6 ms | 1.8 ms | **9.33x** |
| `max_range` | 8.5 ms | 0.6 ms | **14.79x** |
| `group_sum_range` (8 group) | 193.8 ms | 184.9 ms | 1.05x |
| `group_min_range` (8 group) | 198.7 ms | 188.3 ms | 1.06x |
| `group_max_range` (8 group) | 162.1 ms | 157.4 ms | 1.03x |
| `range_scan` (hit ~10%) | 9.3 ms | 4.9 ms | 1.90x |
| `histogram_range` (10 bucket) | 38.6 ms | 6.3 ms | **6.17x** |

`group_*` 系は cap = 8 で per-chunk acc 構築 + reduce merge の orchestration cost
が支配的、 並列メリットが小さい (regress していないので OK 扱い)。 `min`/`max`
は branch ありで NEON auto-vec が効かないため seq が遅く、 並列化で局所 reduce
ができて大爆速。 `sum`/`count` は 12M row を 1ms で完走するほど NEON が効いて
おり、 並列化の orchestration が 50% 食う。

### Tests

- `crates/enchudb-engine/tests/range_par.rs` (8 件): par 結果が seq と一致、
  閾値以下で seq fallback、 空範囲 / 大規模での動作確認。

### bench

- `examples/par_scan_bench.rs`: 12M row で 9 query の seq / par 比較を一発実行。
  `cargo run --release --example par_scan_bench` で再現可能。

### 期待 impact (= suzukapulse / mlbpulse)

- suzukapulse dominance: lap 別 column scan の min/max 系が dominant cost
  だったので、 9-15x の improvement で全体 1.95s → 数百 ms 級になる見込み。
- mlbpulse 球種別 max velo / 投手別 min ERA: 同様に大幅改善。

### Reference

- [#39] perf: bulk column scan の rayon 並列化


## 0.8.8 — 2026-05-31

#38 対応。 0.8.6 の `sum_range` / `group_sum_range` pattern を **min / max /
group_min / group_max / histogram** に拡張。 suzukapulse / mlbpulse で callsite
に散在していた手書き min/max loop を engine primitive に集約できる。 file
format / wire format 完全不変、 **0.8.7 から再 build のみで上がれる**。

### Added

- `Engine::min_range(himo, lo, hi) -> Option<u32>` — `[lo, hi)` eid 範囲の
  column 直 scan で最小値を求める。 全 missing なら None。 stored 形式の
  `0 = missing` を skip する以外は branchless tight loop。
- `Engine::max_range(himo, lo, hi) -> Option<u32>` — 同様に最大値。
- `Engine::group_min_range(group, val, lo, hi) -> Vec<(u32, u32)>` — 2 column
  lockstep scan で group 別 min。 `group_sum_range` と同じ dense / sparse
  切替 (= `group_dense_cap` 経由)。 dense path では `mins_stored[g] != u32::MAX`
  を「データ有り」判定に使い、 seen tracking を省略。
- `Engine::group_max_range(group, val, lo, hi) -> Vec<(u32, u32)>` — 同様に max。
  dense path は `maxs_stored[g] != 0` を判定に使用。
- `Engine::histogram_range(himo, lo, hi, vmin, vmax, n_buckets) -> Vec<u32>` —
  値域 `[vmin, vmax]` を `n_buckets` 等分した頻度ヒストグラム。 範囲外の値は
  drop (clip ではない)、 戻り値長は常に `n_buckets`。 `n_buckets == 0` /
  `vmin > vmax` のときは空 Vec。
- Schema `Table` API:
  - `Table::min(col) -> Option<u32>`
  - `Table::max(col) -> Option<u32>`
  - `Table::group_min(group, val) -> Vec<(u32, u32)>`
  - `Table::group_max(group, val) -> Vec<(u32, u32)>`
  - `Table::histogram(col, vmin, vmax, n_buckets) -> Vec<u32>`

  いずれも `sum` / `group_sum` と同じく、 `table_eid_range` を auto-bind して
  engine の `_range` primitive を呼ぶだけの薄い wrapper。

### Tests

- `crates/enchudb-engine/tests/range_min_max_histogram.rs` (8 test): 基本動作、
  値 0 を含むケース、 dense path、 部分範囲、 histogram edge case
  (`n_buckets == 0` / `vmin > vmax` / 値域外 drop / `vmin == vmax`)、
  eids 版 `min` / `max` との整合性。
- `crates/enchudb-schema/tests/aggregations_min_max_histogram.rs` (5 test):
  Table API wrapper の基本動作、 空 table、 group_min / group_max、 histogram
  基本、 histogram edge case、 sum / count_col との整合性。

### Performance impact

- suzukapulse dominance (lap 別 segment min / corner max): callsite の per-lap
  手書き loop が 1 関数呼び出しに圧縮可能。 30〜50% の追加短縮見込み (= [#38])。
- mlbpulse (球種別 max velo / 投手別 min ERA 等): 同様に手書き loop 撲滅。
- 詳細 bench は次 patch で計測予定。

### Reference

- [#38] feat: group_min / group_max / histogram の column scan primitive 追加


## 0.8.7 — 2026-05-30

schema 永続化を **`.schema` sidecar に移行**、 0.6.x 以来の `schema_meta_entity`
(= anonymous entity に blob を載せる方式) を撤去。 mlbpulse 等の engine 直構築
DB を `Database::open` した時の panic を根治。 file format / wire format 完全
不変、 **0.8.6 から再 build のみで上がれる**。

### Fixed

- **engine 直構築 DB を `Database::open` で開くと panic** (= mlbpulse の 4.5M
  pitch DB で表面化): 旧実装は `__enchu_schema_meta__` marker himo の存在を
  前提に `ensure_schema_entity` で `eng.entity()` (= anonymous) を呼んでいたが、
  `define_table` 後 anonymous は close されており panic していた。
  `.schema` sidecar に永続化方式を切替えることで marker 依存を撤廃、
  engine 直 DB でも `.tables` + himo_types から fallback 復元できる
- **schema 永続化が anonymous entity 経由だった構造問題**: 0.7.0 で table API
  が engine 層に降り `.tables` sidecar 化された時点で本来撤去すべき残骸 (=
  issue note "schema_meta_entity は 0.7.0 の名残") を解消

### Added

- **`.schema` sidecar** (= `{db_path}.schema`): schema 情報を tmp file → fsync →
  rename で atomic write、 `Database::create` / `finish_*` / Drop で書き出し、
  `Database::open` で読み込み。 PK / column type / relations を完全に持つ
- **`Engine::db_path() -> &str`**: schema crate が sidecar path を組み立てる用
- **`Engine::himo_count() / himo_name_at(idx) / himo_type_at(idx)`**: engine 直
  DB からの schema synthesize fallback で iterate するための accessor
- **`Engine::fk_refs_for_table_named(name)`**: relation 復元用 accessor

### Changed (= 旧 API 全部内部撤去、 公開 API 不変)

- `Database` struct から `marker_himo_id` / `schema_meta_entity` field 削除
- `SCHEMA_META_HIMO` / `SCHEMA_BLOB_HIMO` / `SCHEMA_MARKER` 定数を `LEGACY_*`
  にリネーム (= 0.8.6 以前で書かれた DB 読み込み path 専用)
- `ensure_schema_entity` 削除 (= panic source 撤去)
- `Database::create*` / `open*` / `wrap_concurrent` の `define_himo(SCHEMA_META_HIMO, ...)`
  撤去
- `TableBuilder::build` の eager `ensure_schema_entity()` 呼出撤去

### Migration (= 自動)

| 経路 | 動作 |
|---|---|
| 0.8.7 で新規作成 | `.schema` sidecar に保存 |
| 0.8.6 以前で作成 → 0.8.7 で open | legacy blob 読み込み → 次 persist で `.schema` sidecar に migrate |
| engine 直構築 → 0.8.7 で open | engine `.tables` + himo_types から fallback 復元 (= **PK は不明扱い**、 schema rebuild で upsert 可) |

### Unchanged

- file format / wire format / `WireRecord` encode / HLC / signature 完全不変
- 0.8.6 で追加された Table::sum / group_sum / count_col API 不変
- sidecar fsync coalesce (= 0.8.2) の挙動も不変

### test

- workspace 全体: **439 passed / 0 failed / 26 ignored** (= 0.8.6 比 +2)
- 新規 `engine_built_db_open.rs` 2 件: engine 直 DB の Database::open / 新規 DB
  の `.schema` sidecar 書き出し検証

### 0.8.6 consumer 向け migration

なし。 `cargo build` で 0.8.7 binary。 既存 DB は自動 migrate (= legacy blob
読み込み → `.schema` sidecar 書き出し)。 mlbpulse のような engine 直 DB は
fallback 復元で開けるようになる。

## 0.8.6 — 2026-05-30

table-scoped 集計の primitive と schema API を整備、 vs DuckDB bench を
3-way で実測。 「DuckDB に負ける」 と言われていた **範囲 BETWEEN / SUM** を
enchudb 上回りに反転、 GROUP BY だけ scatter write の NEON 制約で
DuckDB に届かず (= 別 algorithmic work)。 **file format / wire format 完全
不変、 0.8.5 から再 build で上がれる**。

### Added — Engine 層 (primitive)

- **`Engine::sum_range(himo, lo, hi) -> u64`**: `[lo, hi)` eid 範囲の
  column 直 scan で sum。 stored_slice (= mmap u32 view、 zero-alloc) を
  branchless tight loop で reduce、 LLVM が NEON 4-wide SIMD に
  auto-vectorize。 1M rows / M2 Max で ~100µs
- **`Engine::group_sum_range(group_himo, sum_himo, lo, hi)`**: 同 range で
  GROUP BY + SUM。 dense cap 経路は acc[g] += v の scatter accumulate
  (= NEON native scatter なし、 algorithmic 制約あり)
- **`Engine::count_range(himo, lo, hi) -> u32`**: stored != 0 を branchless
  cast で popcount
- **`Engine::range_scan(himo, lo, hi) -> Vec<EntityId>`**: column 直線 scan
  で範囲 filter (= BucketCylinder reverse union を避ける fast path)。 hit 率
  高い range query で 18x 高速化
- **`Engine::table_eid_range(name) -> Option<(u32, u32)>`**: table 名で
  eid range を引く schema 連携用

### Added — Schema 層 (= README 推奨 user-facing API)

- **`Table::sum(col) -> u64`**: 当該 table の column 合計。 内部で
  `engine.sum_range(table_himo, eid_range_lo, eid_range_hi)` に bind
- **`Table::group_sum(group, sum) -> Vec<(u32, u64)>`**: 同じく
  group_sum_range に bind
- **`Table::count_col(col) -> u32`**: count_range に bind

→ user code は 1 行: `orders.sum("amount")` / `employees.group_sum("dept", "salary")`

### Added — 基盤 primitive

- **`Column::values_u32() -> &[u32]`**: packed mmap → u32 slice view (= zero
  copy、 pointer cast)
- **`HimoStore::stored_slice() -> &[u32]`**: stored 形式 (0 = missing) のまま
  callsite に露出。 SIMD 集計の入口

### Bench (= 真の vs DuckDB)

- `examples/vs_sqlite.rs` を `examples/vs_db.rs` に rename + DuckDB
  in-process (= duckdb crate bundled feature) を追加。 旧 stale な
  `crates/enchudb-engine/examples/battle_vs_duckdb.rs` (= CLI subprocess、
  公正でない) は削除
- 9 query を schema 層 API 経由で 3-way 比較 (= enchudb / sqlite / duckdb)
- **8/9 で enchudb 勝利** (= filter / lookup / 範囲 / SUM / COUNT / MIN/MAX)、
  GROUP BY のみ DuckDB が 8x ↑ (= scatter write 制約、 別 work)

### Measurements (M2 Max / 1M rows / same thermal state)

| query           | 0.8.5     | 0.8.6     | duckdb     | 変化         |
|-----------------|----------:|----------:|-----------:|--------------|
| 範囲 BETWEEN    |  14.62ms  |  897µs    |   7.96ms   | **18x ↑** (DuckDB を 8.9x 上回り) |
| SUM (table)     |  ~1.65ms  |   99µs    |   508µs    | **30x ↑** (DuckDB を 5x 上回り) |
| GROUP BY        |  9.70ms   |  12.24ms  |   1.47ms   | ~noise (DuckDB 未達) |

### Unchanged

- file format / wire format / 公開 API (= 既存 sum / group_sum / where_range
  は不変、 新 API は追加)
- 0.8.5 で追加された sync vocab dedupe / query_by_id peer prefix 不変

### 0.8.5 consumer 向け migration

なし。 `cargo build` で 0.8.6 binary。 bisquit / sinfo / suzukapulse / mlbpulse
等の consumer も再 build のみで上がれる。 集計が遅かった code は
`Table::sum` / `Table::group_sum` に書き換えで 10-100x 改善見込み。

## 0.8.5 — 2026-05-30

sync 経路の 2 件の bug fix patch release。 bisquit (dogfood) の Mac ↔ Android
mesh sync で表面化した amplification loop と、 schema 層の `where_eq().find_one()`
が壊れた eid を返してた cast bug を fix。 file format / wire format / 公開 API
変更なし、 **0.8.4 から再 build のみで上がれる**。

### Fixed

- **#30 `apply_one::DecodedOp::Vocab` の HLC dedupe 欠落** (= bisquit dogfood で
  amplification loop): 旧 behavior では同じ vocab record の再受信を毎回
  `applied++` 扱いし、 `gossip_remote_apply` ON 構成で WAL 再追記 → 再 publish
  → 再受信 の cycle に見える状態だった。 受信前に `Engine::has_remote_vocab`
  で `(author_peer, vid, bytes)` 一致を check、 既登録なら `skipped++` に振り分け
- **#32 `Engine::query_by_id` の peer prefix 落ち** (= schema 層 `where_eq` 系の
  PK lookup が壊れた eid 返却): 旧 behavior は `query_resolved -> Vec<u32>` を
  `as EntityId` (= u32→u64 widen) で変換、 高 32bit (= peer_id) が 0 のままで
  `engine.get(eid, ...)` 等が dangling になる。 `entities_with_himo` と同じ
  `make_eid(self.peer_id(), e)` で peer prefix を付与

### Added

- **`Engine::has_remote_vocab(author_peer, remote_vid, bytes) -> bool`** public
  API: 受信 vocab record の dedupe 判定用 (= sync crate が呼ぶ)。 `(author_peer,
  remote_vid)` が `peer_vocab_map` に登録済みかつ map 先 local_vid の bytes が
  受信 bytes と一致するなら true

### test

- workspace 全体: **437 passed / 0 failed / 26 ignored** (= 0.8.4 比 +4)
- `enchudb-engine/tests/query_by_id_peer_prefix.rs` 2 件 (= #32 検証)
- `enchudb-sync/tests/vocab_dedupe.rs` 2 件 (= #30 検証)

### Unchanged

- file format / wire format / `WireRecord` encode 形式 不変
- 0.8.4 で追加された `create_growable_with_options` / 同一 himo bulk column
  scan API は無触
- HLC / signature / pubkey_fp layout 不変

### 0.8.4 consumer 向け migration

なし。 `cargo build` で 0.8.5 binary になる。 bisquit / sinfo / suzukapulse /
mlbpulse 等の consumer は再 build のみで上がれる。 `gossip_remote_apply(true)`
構成は 0.8.5 以降で amplification loop 解消。

## 0.8.4 — 2026-05-25

`Database` / `Engine` から `vocab_data_size` を明示できる
`create_growable_with_options` を公開。`Leaf` 列の値も `vocab.insert`
経由で vocab data に積まれる仕様のため、大規模 text を持つアプリ
(議事録 / 論文 / 全文 archive 系) で `create_growable*` 系の default
512 MiB cap に当たって `vocabulary.rs:175` で panic していたが、
明示指定で回避可能になった。新規 method の追加のみで既存 API には
触らない (後方互換 100%、再 build のみで上がれる)。

### Added

- `Engine::create_growable_with_options(path, max_entities, vocab_data_size)`
- `Database::create_growable_with_options(path, max_entities, vocab_data_size)`

setagaya-pwa の世田谷区議会議事録 archive (4,844 会議 / 554,092 発言 /
466,383 theme、本文 vocab ~531 MB) で検証。`vocab_data_size = 2 GiB`
で全量 import 2.4 秒 (230K rows/sec) 完走、search レスポンス 0.8 秒。
closes #26

## 0.8.3 — 2026-05-25

`wasm32-unknown-unknown` build が 0.8.2 で壊れていた問題を 1 行修正。 native
build / 公開 API / file format / wire format は完全不変、 wasm consumer
(naruhodo/web 等) は再 build のみで上がれる。

### Fixed

- **wasm32 build E0560**: `Engine::load_from_backing` 内の `_writer_lock: None`
  初期化に `#[cfg(not(target_arch = "wasm32"))]` が漏れていた。 field 定義
  (`engine.rs:923`) は wasm32 で除外されているが、 `load_from_backing`
  (`engine.rs:1699`) の初期化は無条件で field 代入していたため、 wasm32 target
  では `struct \`Engine\` has no field named \`_writer_lock\`` で fail
  (issue #22)。 closes #22

## 0.8.2 — 2026-05-23

`Database::create → build×N → finish_with_oplog` の cold-open perf を
N table 数 linear から定数時間に圧縮。 sinfo (sinfohub-server) の multi-tenant
scope DB cold-open がボトルネックで、 60 user 同時 push の bench で 5+ 秒
latency 出てた issue #19 を fix。 file format / wire format / 公開 API
変更なし、 0.8.1 から **再 build のみで上がれる**。

### Fixed

- **`Database::create → build×N` の N×fsync 問題**: `TableBuilder::build()` の
  末尾で呼んでた `persist_schema()` (= `eng.flush()` = body msync ≒ 47ms on
  APFS) が N 回走ってた。 build 中の schema blob は誰も読まない (= `load_schema()`
  は open path でのみ) ので中間 persist は無駄。 `finish_with_oplog` /
  `finish_concurrent` / Drop の 1 箇所に coalesce
- **`define_table` / `define_himo_in` の per-call sidecar fsync**: engine 側
  `try_persist_tables()` が毎回 `f.sync_all()` を呼んでた (= 各 ~5ms × 105 call
  for N=15 × 7 col = ~600ms)。 build phase 中は `defer_tables_persist` flag で
  skip、 finish 時に 1 度 explicit に persist

### Added

- **`Engine::set_defer_tables_persist(&self, bool)` API**: build phase の
  sidecar fsync を抑止する toggle。 schema crate が `wrap_new` で立てて、
  `finish_*` / Drop で解除して explicit fsync を 1 度走らせる。 Engine 直利用
  (= schema 層なし) で叩く必要は無い、 default は false (= 既存 behavior 維持)
- 回帰防止 test `cold_open_coalesce.rs` 3 件: declare phase が 200ms 以下、
  finish 経由で schema が disk に persist、 Drop safety net が機能

### perf 確認 (M2 Max / APFS, declare phase / N=15 table × 7 col)

| | 0.8.1 | 0.8.2 | 改善 |
|---|---:|---:|---|
| declare phase | 663.9 ms | 1.1 ms | **600x** |
| per-table | 44.3 ms | 0.07 ms | 600x |
| finish | 13.6 ms | 37.6 ms | -2.8x (= 1 回に集約された fsync 分) |
| **total cold-open** | **677 ms** | **39 ms** | **17x** |

issue #19 の予測値 (~720ms → ~70ms, 10x) を更に上回る改善。 sinfo の 60 user
同時 push bench は scope DB cold-open がボトルネックだったので、 これで unblock。

### Unchanged

- file format / wire format / 公開 API 完全不変、 0.7.x / 0.8.x DB は全て open 可
- `add_column` (= alter path) は引き続き per-call で persist_schema (post-build
  なので fsync 1 回が正しい挙動)
- HLC / signature / pubkey_fp layout 不変

### 0.8.1 consumer 向け migration

なし。 `cargo build` で 0.8.2 binary になる。 sinfo / opyula 等の schema 経由
consumer も再 build のみ。

## 0.8.1 — 2026-05-22

short-lived CLI consumer 連携で表面化した recover 不完全の patch release。 sinfo
の sf CLI (= open → 1 write → drop) で entity 状態 (`next_local` + `entities`
live bitmap) が次 open に持ち越せず eid 衝突が出ていた。 file format / wire
format / API 変更なし、 **0.8.0 から再 build のみで上がれる**。

### Fixed

- **`apply_oplog_op` の recover 不完全**: Tie / Content op で `entities.ensure_live`
  + table `next_local` の max 推進が呼ばれていなかった (= 0.8.0 以前から続く
  defect)。 crash recovery (= consumer shutdown 前に kill された場合) で oplog
  replay は走るが、 entity 状態が body の himo data と整合せず、 次 `entity_in`
  が重複 eid を払い出す問題を fix
- **graceful shutdown で tables sidecar 未 persist**: 0.8.0 consumer thread の
  shutdown path は `body_msync` のみで `tables` sidecar (= `next_local` の永続化先)
  を更新していなかった。 short-lived CLI で flush(&mut) を呼ばずに drop すると
  次 open で `next_local=0` のまま戻り、 既存 eid と衝突する root cause。 shutdown
  + 周期 fsync (= 100ms) の両方で `try_persist_tables()` を呼ぶように変更

### Added

- **`Engine::persist_tables(&self) -> io::Result<()>`** public API: `Arc<Engine>`
  (= concurrent mode) でも tables sidecar を強制 persist できる。 既存 `flush(&mut)`
  が取れない context (= sinfo 等の embed consumer で long-lived process が任意
  tick で固めたい場合) 用、 wasm / memory-only では Ok(()) no-op
- **`apply_oplog_op` 内で `advance_table_next_local_for`**: recover 中に与えられた
  global eid を含む table の `next_local` を `(eid - lo) + 1` まで前進させる
  private helper、 上記 fix の実装本体

### Unchanged

- file format / wire format / 公開 API 完全不変、 0.7.0 ↔ 0.8.1 wire 互換は
  0.8.0 と同条件 (= 非互換)
- 0.7.x consumer 経路 (`pending_sync_ops` etc) は前 release 通り
- HLC / signature / pubkey_fp layout 不変

### 0.8.0 consumer 向け migration

なし。 `cargo build` で 0.8.1 binary になる。 sinfo / opyula 等の schema 経由
consumer も再 build のみ。

## 0.8.0 — 2026-05-22

sync 並走の解消 — oplog publish path 撤去 + `_sync_ops` 一本化 + ring buffer
化。 0.7.0 で並走可能化した `_sync_ops` reserved table を sync 配信の primary
source にし、 oplog は local crash recovery 専用に役割を絞る。 0.7.0 で「移行
猶予期」 として残してた並走モードを 0.8.0 で完全解消。 計画書: `notes/requests/request8.md`。

### Breaking

- **sync wire format 変更**: `_sync_ops.payload` の wire layout を
  `signature(64) + pubkey_fp(8) + signed_bytes(rest)` の concat 形式に拡張
  (= 0.7.0 では signed_bytes のみだったが、 sync 経路で署名検証できなかった
  defect を fix)。 **0.7.x peer との sync 互換は失う**
- **file format 互換**: 維持 (= 既存 v6/v7 DB はそのまま open 可能)、 ただし
  0.7.x で書いた `_sync_ops` row は 0.8.0 で peer publish 時に rejected (=
  signature 抜きで verify 不可)。 既存 row は手動 reclaim or DB 作り直しで対処

### Added

- **`Engine::transfer_oplog_to_sync_ops` の自動化**: consumer thread が fsync
  interval (= 100ms) 経過時 + shutdown 時に `sync_tables_enabled()` なら自動で
  bridge を発火。 user は手動呼び出し不要、 0.7.0 互換のため API は idempotent
  に残る
- **`TableDef.free_locals`**: reclaim で解放された local id の reservoir。
  `entity_in(table)` は free list 優先で payout (= ring buffer 化)、 `_sync_ops`
  の長期運用で eid 飽和を防ぐ
- **`enchudb_oplog::decode_sync_ops_payload(bytes)`**: `_sync_ops.payload` の
  concat 形式を `Record` に復元する公開関数。 sync crate の publish path で使う
- **`enchudb_oplog::SIGNED_PAYLOAD_HEADER_SIZE` / `SYNC_OPS_PAYLOAD_PREFIX`**:
  wire layout の sized const、 transport / sync 層が固定 offset で parse する用

### Changed

- **`Syncer::publish_since` / `publish_since_for_peer` の内部実装**: `_sync_ops`
  経由 (= `pending_sync_ops` + `decode_sync_ops_payload`) に切替。 公開 API は
  不変、 既存 consumer は再 build で済む。 `sync_tables_enabled()` 未呼出の DB
  では legacy oplog iter 経路に自動 fallback (= 0.7.x DB 互換)
- **`reclaim_sync_ops` が free list に push**: 解放 row の local id を `_sync_ops`
  table の `free_locals` に積む (= 次回 `entity_in("_sync_ops")` で再利用)
- **`Syncer.published_lsn: AtomicU32`** field 追加: 将来 (= 0.9.0) で
  watermark-driven reclaim を Syncer 経由で駆動する準備

### Unchanged

- `WireRecord` encode 形式 (= peer 間 transport の wire schema) 不変
- HLC / EntityId / PeerId / ed25519 signature / pubkey_fp layout 不変
- crash recovery semantic (= oplog commit marker + recover replay) 不変
- file magic `ECDB` / version 5 / 全 region layout 不変
- schema crate 公開 API (`Database::table().build()` 等) 完全不変

### 0.7.x consumer 向け migration

[`docs/migration-0.7.0-to-0.8.0.md`](docs/migration-0.7.0-to-0.8.0.md) (=
local 専用) に詳細あり、 要約:

- **schema 経由 consumer** (opyula / bisquit / sinfo / matcha / t5ug3 等):
  再 build で済む、 公開 API 完全不変。 `Database::enable_sync()` 呼んでいた
  consumer は自動 transfer 化により `transfer_oplog_to_sync_ops()` の手動
  呼び出しが不要に (= 残しても idempotent で no-op)
- **`enchudb-sync` 直接 consumer**: `Syncer::publish_since` の戻り値は同じ、
  ただし source が `_sync_ops` 経由に。 sync_tables_enabled な engine では
  watermark + reclaim が効くので長期運用で `.oplog` の線形成長が止まる
- **peer 同士**: 0.7.x ↔ 0.8.0 sync は wire 互換切れ、 同時に upgrade すること

## 0.7.0 — 2026-05-22

mini-RDB semantics の **actually 確立** ([issue #11](https://github.com/Mutafika/enchudb/issues/11) +
[issue #15](https://github.com/Mutafika/enchudb/issues/15))、 加えて `enchudb-schema`
に **deployment topology を隠す view layer** ([issue #12](https://github.com/Mutafika/enchudb/issues/12))
を導入。 0.5.0 で engine に追加した table API を `enchudb-schema` crate / consumer 層が
**1 度も使ってなかった** (= 死荷物)、 同時に `enchudb-oplog` が 「local 耐久 log」
と 「sync 配信 stream」 を兼任していた構造問題を一括解消。 計画書: `notes/requests/request7.md`。

### Breaking

- **schema crate の hot path が engine table API 経由に**: 新規 `Database::table().build()`
  は `define_table` + `define_himo_in` + `define_ref_in` を engine に発火、
  `RowBuilder::commit` は `entity_in(table_name)` で eid_range 内払出。 既存 v5/v6 DB
  は透過 open + lazy migrate (= 過去 anonymous entity は eid 不変で読める、 新規 row は
  table 内 eid_range から払出)
- **`TableDef.next_local` が `AtomicU32` 化、 `Engine::entity_in` が `&self` 化**:
  schema crate / Arc<Engine> 経由の concurrent mode から row insert で CAS-safe 払出
- **`define_table("_*")` を reject**: `_` 始まり名前は reserved namespace、 user 経路は
  `String` Err を返す
- **既存 `Engine::list_tables()` は reserved table も含む**: user code は 0.7.0 から
  `list_user_tables()` を使うべき (= reserved を除外)
- **semver**: 0.6.0 → 0.7.0

### Added

- **`TenantView` / `TenantViewMut`** ([issue #12](https://github.com/Mutafika/enchudb/issues/12)、 PR #13) — `enchudb-schema` に「物理 layout を隠す view layer」 を追加。 `Database::tenant(name)` / `tenant_mut(name)` で tenant scope view、 `as_view()` / `as_view_mut()` で root view を取り出す。 内部で table 名に `{name}.` prefix を自動付与する薄い wrapper、 storage layout は変えない。
  - 不変式: `tenant("alice")` から取った view を pattern A (centralized container) でも pattern B (per-user DB ファイル) でも同じ closure で操作できる。 deployment topology を app に隠す。
  - 既存 API は完全不変、 追加のみ。
  - overhead: `tenant().get_table()` ≈ 50 ns/op (format! 1 回)、 `as_view().get_table()` ≈ 18 ns/op、 raw 7 ns baseline。 schema-layer `get_table` は hot path じゃない (起動時 1 回引いて handle 保持) ので実用上 0 影響。
  - example: `crates/enchudb-schema/examples/tenant_view_demo.rs`
  - test: `crates/enchudb-schema/tests/tenant_view.rs` (6 件、 invariant / isolation / round-trip / multi-tenant scenario / interleaved build-read / root view)
- **`TableBuilder::with_capacity(n)`**: 1 table に大量 row (= 1M+) を入れる workload で
  eid 空間を明示確保。 省略時の default は `remaining / 4` で 4 table 分残す妥協値
  (= multi-table workload 向け)。 1M entity を 1 table に入れる bench 系で
  `entity_in() failed: eid range exhausted` を防ぐため必須
- **`_sync_ops` / `_sync_peers` reserved table** ([issue #11](https://github.com/Mutafika/enchudb/issues/11)):
  - `Engine::enable_sync_tables()` / `Database::enable_sync()`: opt-in で sync 経路の
    reserved table を auto-define (= sync 不要な単独 DB は eid 空間も浪費しない)
  - `Engine::transfer_oplog_to_sync_ops()`: oplog の commit 済み record を `_sync_ops`
    table へ転送 (consumer thread から定期実行する想定)
  - `Engine::ack_sync(peer, lsn)`: peer の watermark を `_sync_peers` に upsert
  - `Engine::sync_watermark()`: 全 peer min(consumed_lsn) (= reclaim 安全点)
  - `Engine::reclaim_sync_ops()`: `lsn < watermark` の row を lazy purge
    (0.7.0 では entity delete のみ、 eid 空間は再利用せず — 0.8.0 で ring buffer 化検討)
  - `Engine::pending_sync_ops(since_lsn)`: peer publish 用、 (since, current] の payload bytes
  - `Engine::current_sync_lsn()`: snapshot 取得時の 「ここまで配信済み」 マーカー
  - `Syncer::mark_initial_sync_complete(peer, lsn)`: snapshot 後の watermark 初期化
- **engine table API 拡張**:
  - `define_reserved_table(name, size_hint)`: `_` 始まり強制の internal table API
  - `list_user_tables()`: anonymous + reserved を除外する user 向け列挙
  - `has_reserved_table(name)`: 状態判定
  - `vocab_intern_text(text)`: entity 経路を一切触らずに vocab inject (= schema crate
    の `intern_table_name` で dummy entity → delete の roundtrip を排除)
  - `remaining_eid_space()` / `max_entities()`: schema crate が `define_table` size_hint
    を auto-clamp する用
  - `is_readonly()`: panic せず bool で返す getter
  - `tie_bytes_to_by_id(eid, himo_id, &[u8])`: Leaf himo に任意 binary を tie
    (= UTF-8 制約のない wire bytes 用、 `_sync_ops.payload` で使用)
- **`snapshot_export` が `.tables` sidecar も含める**: receiver で table 構造 +
  reserved table を復元可能
- **3 deployment pattern reference example** ([issue #11](https://github.com/Mutafika/enchudb/issues/11)):
  `examples/sync_centralized.rs` (中央集権) / `sync_per_user.rs` (per-user DB) /
  `sync_local_first.rs` (privacy-first + blob offload)

### Changed

- **0.5.0 / 0.6.0 CHANGELOG 文言の訂正**: 「mini-RDB semantics の確立」 →
  「engine 基盤の確立」 (= consumer 層への配線は 0.7.0 で完成、 という事実を反映)
- **`tie_to_by_id` / `tie_text_to_by_id` / `tie_bytes_to_by_id` の reserved table skip**:
  `_*` table への write は oplog 再 append を skip (= `_sync_ops` への内部 mirror が
  oplog → `_sync_ops` → oplog の無限ループにならない設計)
- **`enable_sync_tables` の reserved table サイズを auto-clamp**: `remaining_eid_space`
  ベース、 tiny preset でも overflow しない

### Migration

[`docs/migration-0.6.0-to-0.7.0.md`](docs/migration-0.6.0-to-0.7.0.md) に既存 v6 DB の
透過 open / consumer code への影響 / sync 経路の opt-in 化手順あり。

API 不変 (= schema crate 公開 API は 0.6.0 から変わらない) なので、 schema crate
経由の consumer (opyula / bisquit / sinfo / matcha / t5ug3 / sinfohub-server 等) は
**再 build で済む**。 sync 経路を活用する consumer は `Database::enable_sync()` を
build phase で呼ぶ opt-in 切替で `_sync_ops` table 機構の恩恵を受けられる。

### Unchanged

- wire record format (v2 layout) 不変
- file magic `EWAL` 不変 (= 0.6.0 と binary-compat)
- HLC / EntityId / PeerId / keys / 署名 layout 不変
- sync 経路 (publish_since / pull_since) の wire protocol 不変 (oplog 経路で並走)

## 0.6.0 — 2026-05-20

`enchudb-wal` crate を `enchudb-oplog` にリネーム ([issue #8](https://github.com/Mutafika/enchudb/issues/8))。
実態が write-ahead log ではなく oplog (MongoDB oplog と同パターン: mmap が primary state、
oplog は peer sync 配信 + audit + crash recovery 用の append-only op stream) なので、
命名と実装の乖離を解消。 wire format / record encoding / file magic は不変、
file 拡張子と API 名のみ変更。

### Breaking

- **crate rename**: `enchudb-wal` → `enchudb-oplog` (`Cargo.toml` の dep 名 + import 全置換)
- **API rename** (主要):
  - `Wal` → `OpLog`、 `WalOp` → `Op`、 `WalRecord` → `OwnedOp`、 `RecoveredRecord` → `Record`
  - `wal_sync()` → `oplog_sync()`、 `wal_commit()` → `oplog_commit()`、 `wal()` → `oplog()`、 `wal_arc()` → `oplog_arc()`
  - `create_concurrent_with_wal*` / `open_concurrent_with_wal*` / `concurrentize_with_wal` → `*_with_oplog*`
  - schema 層 `Database::open_with_wal` / `finish_with_wal` → `open_with_oplog` / `finish_with_oplog`
  - 公開フィールド `Stats { wal_head, wal_checkpoint, ... }` → `oplog_head, oplog_checkpoint, ...`
  - 詳細マッピングは [`docs/migration-wal-to-oplog.md`](docs/migration-wal-to-oplog.md)
- **file 拡張子**: `{db_path}.wal` → `{db_path}.oplog`、 fallback open なし (clean break)
  - 既存 `.wal` ファイルが居る場合は手動 rename (`mv x.wal x.oplog`) で OK、 中身 binary は不変
- **semver**: 0.5.0 → 0.6.0

### Unchanged

- wire record format (v2 layout) 不変
- file magic `EWAL` (歴史的経緯で binary-compat、 0.5.0 で書いた `.wal` を `.oplog` に rename すればそのまま読める)
- HLC / EntityId / PeerId / keys / 署名 layout 不変
- sync 経路 / publish_since / pull_since 等の wire protocol 不変

### Migration

[`docs/migration-wal-to-oplog.md`](docs/migration-wal-to-oplog.md) に consumer crate
向けの完全 import 置換 sed + ファイル拡張子 rename 手順あり。 enchudb meta crate の
re-export (`enchudb::{EntityId, Hlc, PeerId}` / `enchudb::keys::*`) は path 不変なので、
これらだけ使う consumer は import 修正不要。

## 0.5.0 — 2026-05-20

β-light: engine 自身が **table 概念** を持つ。 旧 flat な eid 空間 + himo 群の上に、
名前付き table の `eid_range`、 table-namespaced himo、 FK validation を engine 直下に降ろした。
mini-RDB semantics の **engine 基盤**。 consumer layer (`enchudb-schema` / SQL / FFI /
RAG) への配線は 0.7.0 で完成 ([request7](https://github.com/Mutafika/enchudb/issues/15))。

### Breaking

- **file format v4 → v5**: header の version 値が 5 に上がる。
  - v4 DB は **透過 open** (= 旧 DB は何もせずそのまま使える、 anonymous-only として扱う)。
  - 0.5.0 で作った v5 DB を 0.4.x で open はできない (= `unsupported file version` で reject)。
  - ダウングレードしたい場合は `snapshot_export` → 0.4.x で recreate の手動 flow。
- **`entity()` が `define_table` 呼出後に panic**: anonymous table を 1 度 close した DB
  では、 旧 API `entity()` は使えなくなる (= 新 API `entity_in(table)` に統一が必要)。
  define_table を呼ばないコードは引き続き旧 API で動く。

### Added

- **engine が table を認知** (新 API):
  - `define_table(name, size_hint)`: named table を作る (`size_hint` 個分の eid range を予約、 0 で 1M default)。
  - `entity_in(table)`: 指定 table の eid_range 内に entity を allocate。
  - `define_himo_in(table, himo, ht, mv)`: table-namespaced himo (`"users.age"` のような `{table}.{himo}` 命名)。
  - `define_ref_in(table, himo, target_table)`: `HimoType::Ref` の FK 宣言、 tie 時に target eid が target_table 内かを engine が validate。
  - `list_tables()`: 全 table メタデータ列挙 (`Vec<(TableId, name, lo, hi)>`)。
- tables 定義の永続化: 新 sidecar file `{path}.tables` に binary encode (table 数 × ~64 byte)。
  - sidecar 不在 = anonymous-only (v4 DB 互換動作)。
- `EntitySet::allocate_at(eid)`: 任意位置 mark + CAS-safe next_eid 進行。 table-aware allocation の bottom-half。
- bench `scale_tables` group を core suite に追加 (= 10 table × 5 himo × 10k entity scale を `bench_scale` (anonymous flat) と A/B 比較する用)。

### Internal

- `TableDef { name, himo_ids, eid_range_lo/hi, fk_refs, next_local }`: 1 table 分のメタ。
- `Engine.tables: Vec<TableDef>` + `himo_to_table: Vec<TableId>`: index 0 が anonymous (open-ended)、 1+ が named table。 anonymous は `define_table` 初回呼出で `eid_range_hi = cur_next_eid` で close される。
- tie hot path に `validate_eid_for_himo` + `validate_ref_tie` を `#[inline(always)]` で追加。 table 数 ≤ 1 (= anonymous only) 時は 1 atomic load の fast path で抜ける。

### 性能影響

bench (criterion、 baseline = 0.4.x = master `51ee42e`):

| bench | 0.4.x | 0.5.0 | Δ |
|---|---|---|---|
| `tie/plain_value` | 18.6 ns | 18.6 ns | ±0% |
| `tie_async/wal_signed_off` | 52.6 ns | 46.4 ns | -12% |
| `pull_raw/single_value` | 82.5 ns | 82.7 ns | +0.2% |
| `query/two_cond_and` | 429.9 ns | 433 ns | +0.7% |
| `scale_*` 群 | baseline | ±2% 内 | -- |

hot path に新規分岐を入れない設計のため regression は noise floor 内。

### Migration: 0.4.x → 0.5.0

#### パターン A: そのまま動かす (推奨、 既存コード)

旧 API は anonymous table へ自動 dispatch される。 何も書き換えなくて OK、 v4 DB はそのまま open できる。

```rust
let mut eng = Engine::create_standalone("db")?;
eng.define_himo("age", HimoType::Number, 100);
let e = eng.entity();
eng.tie(e, "age", 30);
```

#### パターン B: table-aware に書き換え

```rust
// 旧 flat
eng.define_himo("user_age", HimoType::Number, 100);
let alice = eng.entity();
eng.tie(alice, "user_age", 30);

// 新 table-aware
eng.define_table("users", 100_000)?;
eng.define_himo_in("users", "age", HimoType::Number, 100)?;
let alice = eng.entity_in("users")?;
eng.tie(alice, "users.age", 30);
```

#### パターン C: FK 付き

```rust
eng.define_table("users", 10_000)?;
eng.define_table("posts", 100_000)?;
eng.define_himo_in("users", "name", HimoType::Tag, 1000)?;
eng.define_ref_in("posts", "author", "users")?;  // FK 宣言

let alice = eng.entity_in("users")?;
let post = eng.entity_in("posts")?;
eng.tie(post, "posts.author", alice as u32);  // alice が users 範囲外なら engine が panic
```

#### file 互換性まとめ

- v4 DB を 0.5.0 で open: **可** (透過 migrate、 anonymous-only として扱う)
- v5 DB を 0.4.x で open: **不可** (`unsupported file version`)
- WAL record format は不変 → 0.4.x peer と sync 可
- `EntityId = (peer:32, local:32)` bit layout 不変

#### sync 互換性

`Engine::audit` / `Wal` 経路は 0.4.x と完全互換。 dual-engine 運用 (= 0.4.x ↔ 0.5.0 peer 間 sync) は WAL format 不変なので可。 ただし 0.4.x 受信側は table 概念を持たないので、 0.5.0 peer が `entity_in("users")` で作った eid (= eid_range 内の値) を anonymous な eid として扱う (= flat な eid 空間としては整合)。

### Future work (0.6.0 候補、 branch `feat-engine-heavy` で実験中)

- **column file table 別分離** (`{path}.t.{name}.col`): drop_table O(unlink) 化
- **positions の mmap-back** (`{path}.positions`): 100M scale で RSS 削減見込み
- **EntityId bit layout 変更** (`peer:24, table_id:16, table_local:24`): sync 経路まで table 認知
- 全 3 phase 完走済み (branch f5d9be7) だが hot path に regression あり (tie +34%、 query +12%、 scale_tables_open +224%)、 master merge 未決。 詳細は [notes/requests/request6.md](notes/requests/request6.md)。

## 0.4.0 — 2026-05-18

### Breaking

- **file format v3 → v4 hard break**: `UndoLog` 撤去に伴い undo region を layout から削除。
  旧 v3 DB を open すると `unsupported EnchuDB file version 3 (expected 4)` で弾かれる。
  pre-1.0 / pre-public release window で許容、 自動 migration は未提供
  (旧 DB は recreate して `snapshot_export` 経由で持ち越し)。
- **standalone mode の crash semantics 変化**: 旧コードは flush 済み未 `commit()`
  の書き込みを open 時に undo replay で巻き戻していた。 v4 では undo log
  自体が無いので **standalone (WAL 無効) では crash 時に途中状態が残る**。
  巻き戻しが必要な caller は `Engine::create_concurrent_with_wal` 経由で WAL
  有効化を推奨 (Commit marker 未到達なら open 時の WAL recover で drop される)。
- **`Engine::rollback()` を削除**: workspace 内に caller ゼロ (engine 内テスト
  3 件 + `enchudb-rag` の pass-through wrapper 1 個のみ) のため breakage 実害なし。
  `enchudb-rag::RagStore::rollback` も同時撤去。
- **API 削除**: `create_full_with_cyl_undo` / `create_concurrent_with_wal_undo_cap`。
  `create_concurrent_with_wal_queue_cap` は `undo_max_entries` 引数を落とした形で残る。
- header offset `72..76` (旧 `H_UNDO_MAX_ENTRIES`) は予約済み (zero 保持) — 後続
  フィールドを追加するなら 80.. を使うこと。

### Removed

- `crates/enchudb-engine/src/undo.rs` を完全削除 (175 行)。
- 旧 test (`rollback_reverts` / `rollback_insert` / `crash_recovery_rollback` /
  `prefix_sum_rollback` / `tests/undo_overflow.rs`) を削除。 削除対象の保証は
  もう存在しない (WAL Commit marker による drop で代替)。

### Perf

- **`BucketCylinder::positions` に `eid_offset` を導入** (`cylinder_v27.rs`):
  - 旧: `positions: Vec<(u32, u32)>` を eid で直 index → 「最初に tie される eid が
    N」 の himo は 0..N の prefix が空気で確保される (= モデル違反)
  - 新: `positions[i]` が eid `(i + eid_offset)` を表す。 各 himo が自分の
    tied range だけ確保
  - bench (6 table × 23 himos × 6M entities, table-segmented insert):
    ```
    before: 861 MB RSS
    after : 336 MB RSS   (−61%)
    ```
  - 副次: undo region 削除と合わせて `snapshot_export` が 270 µs → 151 µs (−44%)

### Fixed

- **issue #1: `UndoLog::record` の backpressure spin が standalone mode で
  permanent hang する**。 consumer thread を持たない構成では `force_commit`
  signal を立てても誰も commit しないので無限 yield に入る。 undo log 自体を
  削除することで根本解消。 issue #1 の再現ケース (10M entity insert × 7 ties)
  が **72+ 分 hang → 2.13 秒で完走**。

### Internal

- `Op::EntityCreated` は v4 以降 no-op、 但し `flush_writes` の barrier counter
  (`push_count` / `apply_count`) と整合を取るために queue を通す経路は残す
  (issue5 対応の延長)。
- `examples/workload_rss_1m.rs` / `workload_segmented_rss.rs` / `workload_sparse_rss.rs`
  を追加 (RSS 計測 / モデル検証 repro)。
- regression test `tests/eid_offset_descending.rs` 追加 — 100k entity を eid
  降順 tie してもO(N²) realloc に落ちず query 結果が正しいことを確認。

## 0.3.0 — 2026-05-17

### Breaking

- **schema layer: row 識別 marker convention を完全廃止**。 「table = 紐の束
  declaration」 という enchudb 本来の世界観に揃え、 row への明示的 table 名
  marker tie / query 毎の marker cond を削除。 column 名は元から
  `{table}.{col}` で内部 prefix されてて他 table と衝突しないので、 marker は
  本質的に不要だった。
  - 公開 API `Database::marker_himo_id() -> u16` を削除
  - 公開 API `Table::table_vid() -> u32` を削除
  - 内部 const `TABLE_MARKER_HIMO` → `SCHEMA_META_HIMO` rename (schema blob
    persistence 専用、 row には触れない)
  - `RowBuilder::commit()` での marker tie 削除
  - `Query::find()` の eq_conds に marker push する経路削除
  - `Query::all()` (eq_conds 空ケース) は PK or first col の
    `entities_with_himo` で代用
  - storage 互換性: 既存 DB の `__enchu_table` himo は無視されるだけで害なし
    (= read 経路は marker を参照しない)。 新規 DB ではそもそも作られない

### Added

- **engine: `get_by_id(eid, hid)` / `entities_with_himo(hid)`** を新規 export
  (`enchudb-engine/src/engine.rs`)。 schema layer の bindings 経路 (= 起動時に
  himo_id を pre-resolve した hot path) で名前 lookup を完全に skip 可能
- **engine: `entities_with_himo`** は「ある himo に値が tie された全 entity」
  を column 走査 (`himo_store::entities_with_value`) で O(next_eid) で列挙。
  schema の `Query::all()` 経由で使う
- **examples 2 個追加**:
  - `enchudb-engine/examples/local_ns_bench.rs` — single get / 結果サイズ別
    query / list UI (1000 行 × 5 attr) を計測。 ns 級性能を可視化
  - `enchudb-engine/examples/battle_vs_duckdb.rs` — 1M rows / 5 query
    (point lookup / filter list / filter SUM / full SUM / GROUP BY SUM) で
    enchudb vs DuckDB vs sqlite3 を CLI 直接呼びで benchmark
  - `enchudb-schema/examples/schema_overhead_bench.rs` — 4 経路 (raw name /
    raw id / schema DSL / schema bindings) で schema layer が zero-cost か
    実測

### Perf

- **`group_*` aggregation を dense Vec / HashMap 化** (`engine.rs:2330+`):
  - 従来: `Vec<(u32, T)>::find` 線形探索 (100 group × 1M eid = 100M 比較)
  - 修正後: himo の `max_values` を見て dense path (Vec 直接 index, cap ≤ 64K) /
    sparse path (HashMap) で切替え。 dense 時は per-eid 2 命令
  - `group_sum / group_count / group_min / group_max / group_avg` 全部対象
  - bench (1M rows / 100 dept で GROUP BY SUM):
    ```
    before: 58.35 ms
    after :  2.38 ms   (24×)
    ```
  - これにより DuckDB (2.05 ms) と互角ラインまで詰まる
- **engine query_column_filter で全件相当 cond を always-true として skip**
  (`engine.rs:3033+`):
  - slice_lens を pre-compute して per-eid 呼ばないように外出し
  - `slice_lens[i] >= total` (= cond の slice が全 entity 数以上) なら
    必ず true なので filter から除外。 marker 廃止後の意味薄まったが
    汎用 cardinality 最適化として残す
- **schema layer の filter query が raw と完全同等** (zero-cost) に:
  ```
  bench (1M rows / 100 dept、 dept_id=42 で 10K filter):
                            before    after
    raw(id 経路)             4.7 μs    4.55 μs   (= 同等)
    schema (DSL)           46.3 μs    4.68 μs   (10×)
    schema (id 経路)        46.0 μs    4.61 μs   (10×)
  ```

### Benchmarks (1M rows / Apple Silicon, 全文 battle_vs_duckdb.rs より)

| Query | enchudb | DuckDB | sqlite3 | enchudb 倍率 (vs sqlite) |
|---|---|---|---|---|
| Q1 point lookup | 8.6 ns | 212 μs | 13.3 μs | **1547×** |
| Q2 filter list 10K | 3.7 μs | 5.1 ms | 15.6 ms | **4217×** |
| Q3 filter SUM | 18.1 μs | 2.6 ms | 14.7 ms | **810×** |
| Q4 full SUM 1M | 1.46 ms | 0.82 ms | 64.6 ms | **44×** |
| Q5 GROUP BY SUM | 2.38 ms | 2.05 ms | 1591 ms | **668×** |

→ **sqlite3 互換目標 (= compatibility ではなく性能上のリプレース) として全勝、
44× 〜 4200×**。 DuckDB は OLAP 専 (full-scan SIMD) のみ Q4/Q5 で僅差勝ち。

### Tests

- `bindings_extract_table_vid_and_himo_id` を `bindings_extract_himo_id_and_engine_direct_write`
  に rename + marker 抜きに書換え。 「column 名 → himo_id」 だけで engine 直叩き
  write/read が schema find に揃うことを検証
- 既存 schema layer test 15 個 全 pass、 marker 削除による regression なし

### Docs

- `enchudb-schema/README.md` を marker 廃止後の世界観に全面更新。 bindings
  例から marker / table_vid を除去、 「table = 紐の束 declaration、 row 識別
  marker は存在しない」 を冒頭に明記

## 0.2.8 — 2026-05-16

### Added

- **request4: `SubscriptionFilter` trait と per-peer publish** — partial sync
  (SNS の followee 限定配信等) を `Syncer` の hook として policy 化。 既存
  caller は API 不変、 default `AllRecords` で旧 broadcast 挙動を維持
  - `enchudb-sync/src/subscription.rs` 新規:
    - `pub trait SubscriptionFilter { fn should_send(&self, target_peer, record) -> bool }`
    - `pub struct AllRecords` (default impl)
  - `Syncer::set_subscription_filter(Arc<dyn SubscriptionFilter>)`
  - `Syncer::publish_since_for_peer(target, since)` — single-target publish
  - `Syncer::publish_since(since)` を `known_peers().for_each(publish_since_for_peer)`
    で per-peer 経路にラップ。 known_peers が空なら旧 broadcast 経路 (= backward
    compatible)
- **`Transport` trait に 3 method 追加** (`enchudb-engine/src/transport.rs`):
  - `publish_to(from, to, records)` — single-target、 default は broadcast
    フォールバック
  - `pull_as(to, from, since)` — to peer 視点で broadcast + targeted log を merge、
    default は broadcast pull フォールバック
  - `known_peers() -> Vec<PeerId>` — default 空
- **`InMemoryTransport`** で 3 method を実装、 `targeted: HashMap<(from, to),
  Vec<WireRecord>>` で per-target log を保持

### Tests

- `subscription.rs` の 2 test (AllRecords 全送り、 author 別 drop filter)
- `sync.rs` の 3 test (default filter backward compat、 自前 filter で peer 別
  partition、 `publish_since_for_peer` 単独呼び)

### Migration

- 既存 caller (`sinfo` / `matcha` / `bisquit` / sunsu の broadcast 経路) は
  **API 完全不変**。 何もしなくても旧挙動で動く
- SNS partial sync を作りたい caller (sunsu の次の段階) は:
  ```rust
  struct SnsFilter { /* peer 別 follow set 等 */ }
  impl SubscriptionFilter for SnsFilter {
      fn should_send(&self, target: PeerId, rec: &WireRecord) -> bool { /* ... */ }
  }
  syncer.set_subscription_filter(Arc::new(SnsFilter::new(...)));
  ```
- 自前 `Transport` 実装を持ってる caller は `publish_to` / `pull_as` /
  `known_peers` の default impl で旧挙動を維持。 partial sync を機能させたい
  なら override (HTTP/WS push 系で必要)

## 0.2.7 — 2026-05-16

### Fixed

- **issue6: 0.2.6 の dirty range tracking が writer hot path で cache line
  contention → 18-34% スループット退化** — 0.2.6 で導入した `mark_dirty` を
  writer thread の `EntitySet::allocate` / `set_bit` / `free` からも呼んで
  いたため、 256 writer + 1 consumer が共有 atomic (`dirty_lo` / `dirty_hi`)
  の cache line を激しく bouncing
  - `GrowableMap::mark_dirty` を CAS loop から `fetch_min` / `fetch_max` 1 命令
    + fast-path skip (両 atomic が既に範囲をカバーしてれば atomic op を完全 skip) に変更
  - `EntitySet` writer paths から `mark_dirty` 撤廃 (`allocate` / `set_bit` /
    `free` / `allocate_from_free_stack`)
  - 代わりに `Engine::body_msync` で `entity_set` region を **無条件 msync**
    (small fixed region なので cheap)。 `GrowableMap::flush_aligned` を
    hardware page 境界に揃える helper として追加
  - `Vocabulary::insert` の `mark_dirty` は残置 (数値書き込み hot path には
    出ない、 text-based caller のみ)
  - `dirty_lo` / `dirty_hi` は consumer thread (apply_op 経由の `Column::set` /
    `UndoLog::record_unchecked` / `ContentStore::set`) のみが書くので
    single-thread atomic で contention が消える

## 0.2.6 — 2026-05-16

### Performance

- **request3: `body_msync` を dirty range 限定化** — 旧実装は consumer thread の
  `body_msync` が `flush(0, committed)` で committed 全体を msync。 sustained
  workload で committed が伸びるたびに線形に遅くなる症状 (sinfohub-server 10K user
  ×100KB load test で body_msync **6 ms → 3.6 s** に増大、 fsync_interval=100 ms
  が実質機能せず producer 全体が consumer に律速)
  - `GrowableMap` に `dirty_lo` / `dirty_hi` の atomic ペアを追加。 hot write path
    が `Region::mark_dirty(off, len)` で union、 consumer は `flush_dirty` で
    swap+reset して [lo, hi) だけ msync
  - 計装した write 経路: `Column::set` / `Column::clear` / `UndoLog::record_unchecked` /
    `UndoLog::commit` / `EntitySet::{allocate, set_bit, free, allocate_from_free_stack}` /
    `Vocabulary::insert` (+ index_insert) / `ContentStore::set` / `ContentStore::remove`
  - sustained throughput は dirty 化 rate の関数になり、 committed 全体の大きさには
    依存しなくなる (= 100K user スケールでも 1000 push/sec 維持を期待)

### Fixed

- **Apple Silicon (16 KB hardware page) での msync EINVAL** — 旧 `PAGE_SIZE=4096`
  compile-time const で 4 KB-aligned 境界に揃えていたが、 macOS arm64 の hardware
  page size は **16 KB** なので msync が EINVAL を返していた (request3 実装中に踏んだ)。
  `sysconf(_SC_PAGESIZE)` で起動時に取って cache する `runtime_page_size()` を導入

### Tests

- `tests/dirty_range_msync.rs::body_msync_handles_dirty_range_correctly` —
  writes + body_msync 交互 5 batch + 連続 (idempotent) 呼びで pass
- `tests/dirty_range_msync.rs::wal_sync_with_dirty_range` — wal_sync 経由でも
  dirty range path が正しく動く

## 0.2.5 — 2026-05-16

### Fixed

- **issue5: `flush_writes()` が live query barrier として機能していなかった** —
  0.2.3 (issue3) で `entity()` から `Op::EntityCreated` を WriteQueue に逃がした
  際、 push_count の counter 連動を入れ忘れていた。 apply_count は EntityCreated
  でも +1 されるので、 `applied >= pushed` が Ties 未 apply の段階で成立 →
  早期 return → flush_writes 直後の live query が 5-12% の write を見落とす
  bug (sunsu Docker scenario 01 medium/large で panic していた症状)
  - `entity()` で `Op::EntityCreated` を push した直後に
    `push_count.fetch_add(1, Ordering::Release)` を呼ぶように修正
  - durability は **影響なし** (WAL は正しく書かれていたので drop+reopen で正しい
    count が出ていた)。 影響は live read のラグだけ

### Tests

- `tests/flush_writes_barrier.rs::flush_writes_waits_for_all_ties_including_entity_created_path` —
  queue_cap=1024 (極小、 backpressure 強制) で 4 writer × 5K iter (= 20K entity +
  40K ties) を流して、 `flush_writes()` 後の `query_by_id` が **20K entity 全部
  返す** 事を検証

## 0.2.4 — 2026-05-16

### Fixed

- **issue4: sustained async writer で queue が unbounded で OOM** — option 1
  (bounded queue + producer block) で対応。 旧 unbounded `SegQueue` では
  writer >> consumer rate になると queue 内 record が線形成長 → RSS 線形成長
  → OOM kill (sunsu Docker scenario 03 で 14s / 8M posts / 3.38 GB → 4 GB 突破)
  - `WriteQueue` を `crossbeam_queue::ArrayQueue` に変更、 push 満杯時は
    `std::thread::yield_now` ループで consumer の drain を待つ (自然な
    backpressure)
  - `wal_record_queue` も `ArrayQueue` 化、 helper `push_wal_record_blocking`
    を経由
  - **新 API**: `Engine::create_concurrent_with_wal_queue_cap(path, wal_cap,
    undo_cap, queue_cap)`。 default は 1 M ops (= 旧挙動に近い緩い setting)

### Added

- `WriteQueue::with_capacity(cap)` / `WriteQueue::capacity()` 公開
- 定数 `write_queue::DEFAULT_WRITE_QUEUE_CAP = 1_048_576`

### Tests

- `tests/queue_backpressure.rs::small_queue_cap_does_not_hang` — queue_cap=64
  (極小) で 8 writer × 200 ops が hang せず完走する事を確認

### Consumer migration notes

- 既存 caller (`create_concurrent_with_wal` 等) は API 不変、 内部だけ
  bounded queue 化。 default 1 M cap が旧 unbounded 挙動の近似なので、 100K
  ops/sec 級の writer なら latency 体感不変
- writer rate が consumer 上限を恒常的に超える app (sustained SNS post 等) は
  **producer 側で push が block する** ようになるので、 throughput が
  consumer 上限に張り付く (= sunsu 等で実測 ~500K posts/sec)
- RSS を更に絞りたいなら `create_concurrent_with_wal_queue_cap(.., queue_cap=10_000)`
  等で明示

## 0.2.3 — 2026-05-16

### Fixed

- **issue3: sustained 並列 sync writer で undo region (16 M) overflow → panic** —
  3 段階で対応。 sinfohub-server の 100K user load test / sunsu scenario 03 が
  完走できる
  - **Phase 1**: `Engine::entity()` の `undo.record` を consumer thread に逃がす。
    新規 `Op::EntityCreated { local }` を WriteQueue に push、 consumer thread の
    `apply_op` が `undo.record_unchecked` で serial に記録 (writer thread を
    速い側に保つ)
  - **Phase 2**: `UndoLog::record` に backpressure。 count が `max_entries` の
    90% 超で writer thread が `force_commit` AtomicBool を立てて `yield_now`
    ループ、 consumer は loop 先頭でこの signal を check し fsync_interval を
    待たず即時 fsync→commit。 consumer 自身は `record_unchecked` 経由なので
    self-deadlock しない
  - **Phase 3 (new API)**: `Engine::create_concurrent_with_wal_undo_cap(path,
    wal_capacity, undo_max_entries)` 追加。 default 16 M で足りない sustained
    workload は 64 M / 128 M 等に上げられる (1 entry = 10 B、 64 M で 640 MB)
- **entity-only ops で undo が clear されない bug** — 多 `entity()` 経路では
  `wal.head` が動かないので、 consumer の fsync 節が `wal.head() > checkpoint`
  だけ見ていた旧 path だと undo.commit が永久に走らず over_threshold が解除
  されなかった。 `pending_count > 0` なら `body_msync + undo.commit` で undo を
  clear する path を追加
- **`tie_to_by_id` の debug_assert を緩和** — Tag/Leaf 型 (vocab_id を value と
  して持つ) の himo を hot path で直接張る用途を許可。 schema 層の marker tie
  を起動時 pre-resolve した `table_vid` で書ける (= request2.md の README 例が
  debug build で panic していたのを解消)

### Tests

- `tests/undo_overflow.rs`:
  - `entity_undo_offloaded_no_overflow` — 4 writer × 2K entity + 2 ties で
    undo cap 4096 (= default の 1/4096) に絞っても panic しない
  - `sync_writer_backpressure_no_overflow` — 16 writer × 1K `tie_text_to` で
    backpressure path を 4+ 回踏ませても panic しない

## 0.2.2 — 2026-05-15

### Performance

- **`Engine::open` が 1219 ms → 6 ms (200× 速い)** — `Vocabulary` データ領域 header
  に「clean shutdown 後の index は無事」 マーク (= clean flag) を追加。 前回の
  graceful close 後に再 open する時、 これまで毎回走っていた 3.49 GB の index
  zero-clear + rebuild を skip するようにした。 crash 後 open は従来通り rebuild
  (= 安全性は不変)
  - default max_entities (16M) で `vocabulary.rs:97-98` の `for b in &mut xm[..] { *b = 0 }`
    が memory bandwidth で律速されて 500-700 ms 消費していたのが原因
  - cap65k 等の小容量でも 75 ms → 1 ms に縮む
- **`define_himo` 直後の heap RSS が 25 GB → 5 MB (5000× 縮)** — `BucketCylinder::positions`
  の eager allocation (`vec![..; max_entities]` = 16M × 8 byte = 128 MB / himo)
  を lazy 化 (`Vec::new()` start、 `ensure_positions` で on-demand 伸長)。 同時に
  v33 以降 dead weight になっていた `PairTable` (= `ensure_himo` で card_a × card_b
  cells を pre-allocate していた、 ~4.6 GB / 200 himos) を全削除
  - sinfo (26 tables / ~156 himos) の OOM kill が解消
  - on-disk layout / API は不変、 consumer 側は `cargo update` だけで効果あり
- **WAL append を consumer thread で batch 化** — 従来 record 1 件ごとに `flock` を
  取って `head` を進めていたのを、 consumer thread が複数 record をまとめて 1 度の
  flock + head 更新で flush するように変更。 raw `tie_async` 経路で per-record flock
  コストが消えて **`tie_async_by_id` 1.42 M op/sec を実測** (WAL on)
- **`tie_*_by_id` / `untie_*_by_id` の 8 関数追加 (request.md)** — 高頻度 writer が
  起動時に解決済みの `himo_id: u16` を持ち回ることで、 per-call の `HashMap<String, u16>`
  lookup を完全に skip。 既存の string 版 8 関数 (`tie_async` 等) は内部で
  `himo_id(name)` を 1 度だけ resolve → `_by_id` 版に委譲する thin wrapper に書き換え
  (API は壊れない)。 同様に `query_by_id(&[(u16, u32)])` も使える

### Added

- **`Engine::open_readonly(path)` + `Database::open_readonly(path)`** — writer lock を
  取らない read 専用 open。 別 process が writer として開いていても並行 open 可、
  reader 同士も無制限。 write API を呼ぶと panic
  - 用途: GUI の表示専用 process、 監視ツール、 backup-reader
- **writer 排他 lock** — `create_*` / `open_standalone` / `open_concurrent_with_wal`
  が `.db.lock` sidecar に `flock(LOCK_EX)` を engine 寿命中保持。 2 つ目の
  writer process は block する (sqlite WAL モード相当の挙動)
- **`Database::create_growable_with_capacity(path, max_entities)`** — default 16M
  (= layout 25 GB、 apparent file 24 GB) を絞れる。 sinfo 等の中規模 app で
  65K 程度に指定すると layout 1.3 GB / apparent 765 MB に縮む
- **WAL `append_inner` に `flock` 排他** — 同 .wal を別 process が直接開いて append
  する場合の data race を防ぐ defense-in-depth
- **schema 層に bindings 取り出し API (request2.md)**:
  - `Table::himo_id(col: &str) -> Option<u16>` — build 時に解決済みの himo_id
  - `Table::table_vid() -> u32` — `__enchu_table` marker に張る vocab_id
  - `Database::marker_himo_id() -> u16` — schema 全体の table marker himo_id
  - これで app は schema layer の private const (`__enchu_table` 等) に触らずに
    bindings struct を組める。 起動時に 1 度抽出 → runtime は engine 直叩き
- **dev tools (examples)**:
  - `growable_rss_repro` — mode 別 (default/cap1M/cap65k/tiny) で VSZ delta /
    drop 時間を計測
  - `open_profile` — `ENCHU_OPEN_PROFILE=1` で各 load step の Δreclaim / Δt を
    eprintln (clean / dirty 両 path)

### Tests

- `engine::readonly_does_not_block_other_opens` — writer + 3 reader 並行
- `engine::readonly_write_panics` — readonly で write API 呼ぶと panic
- `engine::writer_blocks_concurrent_writer` — 2 writer 同時起動で 2nd が block
  (200 ms timeout 後 1st drop で unblock)
- `schema::open_readonly_coexists_with_writer` — schema 層から writer + 3 reader 共存
- `schema::create_growable_with_capacity_apparent_size_scales_down` — cap=65K で
  apparent file が default の 10× 以上縮む事を検証
- `wal::multi_process_append_no_offset_collision` — 同 .wal を 2 Wal instance で
  交互 append しても record 破壊なし

### Internal

- `Backing::flush_range(offset, len)` 追加 — page-aligned で targeted msync。
  clean flag のような小領域 (16 byte) を 25 GB 全体 msync せずに 4 KB だけに絞る
  用途
- `Vocabulary::mark_index_clean(bool)` 公開 method — Engine が flush / open
  境界で flag を書き換える
- `Engine` に `is_readonly: AtomicBool` 追加。 既存 `is_replica` パターンと並列。
  `check_writable` で両方チェック

### Internal docs / files

- `docs/concurrency.md` — writer / reader / multi-process モデルを 1 枚で
- README に **「並行アクセス」 章** 追加 + concurrency.md への link
- `tests/wal_mmap_race.rs` — WAL-vs-mmap race の deterministic 再現テスト
  (`#[ignore]`、 fix 未着手の known issue として固定)

### Positioning / docs

- **schema 層を declarator + bindings 専門に位置付け直した** — README / schema crate
  README を rewrite。 高頻度 writer / reader (sunsu の SNS bench、 sinfo の concurrent
  job 等) は **「起動時に schema declare → bindings 抽出 → runtime は engine 直叩き」**
  が公式推奨。 schema 層の `insert().commit()` / `where_eq().find()` は declarative
  convenience として残るが、 hot path で経由する想定ではない
- runtime hot path 推奨形:
  ```rust
  // 起動時 1 回
  let users = db.get_table("users").unwrap();
  let marker_hid = db.marker_himo_id();
  let table_vid  = users.table_vid();
  let name_hid   = users.himo_id("name").unwrap();

  // runtime: bindings + engine 直叩き
  let eng = db.arc_engine();
  let e = eng.entity();
  eng.tie_to_by_id(e, marker_hid, table_vid);
  eng.tie_text_to_by_id(e, name_hid, "Alice");
  ```

### Consumer migration notes

- **schema commit 経路を使ってる app は何もしなくていい** (内部で `_by_id` 経路に
  切り替え済み、 API 不変)
- **hot path で perf を出したい app** は次に bindings 抽出 + engine 直叩きに移行:
  - `sunsu` の concurrent_posts: 113k posts/sec → ~1.4 M posts/sec (estimate ~12×)
  - sinfo の SNS 系 writer 全般
- `sinfo` の sf CLI が持っている `fs2::FileExt::lock_exclusive` (acquire_db_lock)
  は本 release の enchudb 内蔵 lock と二重になる。 動作は壊れないが、 redundant
  なので sinfo 側で別途 cleanup PR を出す予定
- `Sinfo Studio` は `Database::open` を `Database::open_readonly` に切り替えれば、
  sf CLI 起動中でも block されない (= 既存 race を完全解消)

## 0.2.1 — 2026-05-13

### Internal

- example: dump CLI 追加 (DB 中身を markdown / json で stdout dump)
- transport: relay log で (peer, hlc) dedupe (gossip 増殖防止)
- sync + engine: gossip 整合性修正 (delete 復活防止 + identity 保持)
- wal: append_relayed + RelayedHeader 追加 (gossip 用)
