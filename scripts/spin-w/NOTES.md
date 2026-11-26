# スピン 案 W の試作 — 最初の一切れ (proto/spin-w)

v0.38.0 (496301cf) の上。 master / version / CHANGELOG / release には触らない。

## 進み

- [x] 宣言: `Engine::declare_history` / `drop_history` / `history_declarations`、 schema の `.history(col)` (足すだけ)。
      保存は `{db}/tables` の `HIS1` (ANS1 の後ろ = 書いた物の末尾)。 宣言の無い DB は書かない (1 byte も変わらない)
- [x] 置き場: `{db}/hist/NNNN.seg` (header 1 page + entry 32 B) と `hist/NNNN.head.seg` (場所ごとの最新 entry、 4 B / 場所)
- [x] 書き手の道: `live_set` / `live_remove` に時刻 (HLC) を運ぶ引数。 宣言した列なら cell を書けた後に 1 件足す
- [x] 読み: `value_at(eid, col, t)` / `value_at_seq(eid, col, n)` / `history(eid, col)` / `pull_at(col, v, t)` / `history_clock` / `history_stats`
- [x] 書き出し: `write_out_body` の最後 (tomb.seg の後) に置き場を書き出す。 crashsim の段 `Phase::BeforeHistory`
- [x] 開く時: きれいに閉じなかった後は WAL の再生の前に entry から鎖を繋ぎ直し、 再生の後に cell と突き合わせる
- [x] 試験: `crates/enchudb-engine/tests/spin_w_history.rs` (模型との比較・断り・HIS1・電源断)
- [x] 変異: `scripts/spin-w/mutations.json` (結果は下)
- [x] 測り: `crates/enchudb-engine/examples/spin_w_bench.rs` + `scripts/spin-w/bench.sh` (結果は下)

## 形

entry (32 B、 page をまたがない。 flags を先頭に置き最後に書く):

| off | 欄 | |
|---|---|---|
| 0 | flags (8 bit) \| 深さ (24 bit) | VALID / OBSERVED (遅くとも) / FIRST / DELETE / BASE (宣言の時の値) / VOID (突き合わせで捨てた) |
| 4 | eid (世代 \| 場所) | identity |
| 8 | prev (位置 + 1) | 同じ identity の 1 つ前 |
| 12 | jump (位置 + 1) | skew binary の飛び糸 (Myers 1983)。 FIRST の entry では前の住人の最後の entry |
| 16 | 時刻 | WAL の HLC を詰めた数 (wall ms << 20 \| logical) |
| 24 | 値 | u64::MAX = 値が無い (untie / 削除) |

- 時刻 = 変更時刻 (`history::pack(hlc)`) + 更新の回数 (深さ = その identity のその列の何回目の entry か、 宣言の時の値 / 最初 = 0)。 どちらでも引ける
- 同じ書き込みは (identity, 列, 時刻) で 1 つ: 足す前に鎖で同じ時刻を探す (時刻の順の鎖なので飛び糸で O(log n))
- 値が変わらない書き込み (書き直し) は変更ではないので足さない (更新の回数は変更の回数)
- デッキ (`pull_at`): 値ごとの差分の並び (時刻, 入った/出た, eid) を、 その値を最初に見た時に entry を全部なめて作り、 後は書き手が足す。
  見ていない値は並べない (書き手は 「見た値があるか」 の読み 1 回)
- 場所が使い回されたら、 次の住人の最初の entry の飛び糸が前の住人の最後の entry を指す。 古い eid で読むと場所の鎖を住人ごとに飛んで探す

## 決めたこと (3. の断る / 下限の印)

- **ユーザの書き込みは断る**。 WAL の無い session (`open_standalone` / build phase / schema の `Database::open`) で、 履歴を宣言した列を持つ表の行への
  tie / untie / delete は何も変えずに断る (`FaultKind::NoWal`、 #1066 と同じ入口 `refuses_walless_sync_write` の頭)。 理由: 時刻の無い書き込みを
  入れると、 その前後の時点の読みが嘘になる (時計の今で置くと 「その時刻に変わった」 と答える)。 印つきで入れても、 アプリの普通の書き込みが
  毎回 「時刻不明」 になる道を開けておく意味が薄い。 build phase で流し込みたい時は、 流し込んでから宣言する (宣言の時の値は地平の entry になる)
- **engine の中の時刻の分からない道は 「遅くとも」 の印 (`OBSERVED`)**。 開く時の後始末 (gen.seg の突き合わせで完了させた死 `drop_leftover_cells`、
  ring の死んだ場所)、 sync の畳み・改名の分け (sync する DB は宣言を断るので今は通らない)、 開く時の cell との突き合わせ。 時刻は列の時計
  (見た最大 + 1、 WAL の時計も `observe_hlc` で進める) で、 意味は 「遅くともこの時刻にはこうなっていた (本当の変更はそれ以前)」。 読みは、 答えた entry の
  次がこの印なら `uncertain` を立てる。 「下限」 と書いた依頼文の言葉とは向きの読みが逆かもしれない (値がこうである時刻の下限 = 変更の時刻の上限)。 要確認
- 前の住人の削除を突き合わせで足す時は、 時刻を次の住人の最初の時刻にして印を付ける (場所を使い回す前には死んでいた)

## 電源断の後 (7.)

門に頼らず開く時に揃える:

1. 再生の前: entry をなめて鎖と heads を作り直す (`relink`、 header の len より後でも flags の立った entry は拾う。 同じ identity の同じ時刻は 1 つにする)
2. 再生: 書き手の道のとおり entry を足す。 加えて **cell が LWW で弾いた record も履歴に足す** (cell の版数だけ先に届いた像では、 再生が cell を書き直さないので
   書き込みの entry が戻らなかった — 電源断の試験で落ちた)。 Delete の再生も宣言した列ごとに 「消えた」 を足す。 時刻の戻る entry を足したら、 その場所の鎖を
   その場で繋ぎ直す (`relink_slot`)。 時刻の前の値と同じなら足さない
3. 再生の後 (`settle_history_at_open`): 場所ごとに cell と集まりの最新を突き合わせる。 cell が正しい:
   - 版数のある DB: cell の版数より新しい entry は cell に届かなかった書き込み → VOID。 残りの最新が違えば cell の版数の時刻で足す (印なし)
   - 版数の無い DB: cell と同じ値の一番新しい entry より新しい entry を VOID。 無ければ 「遅くとも」 で足す
   - 死んだ場所の最後の行 / 前の住人の最新に値が残る → 削除の 「遅くとも」
   - VOID を出したら全体を繋ぎ直す
- 突き合わせは表の場所を全部見る (O(容量)) が、 きれいに閉じなかった後だけ

## 数

(下に表。 VM: 4 vCPU / 16 GB、 x86 Linux。 書き手 1 本)

## 線を外れた所と見立て

(下)

## 決めずに置いた所 / 次の切れで要ること

(下)
