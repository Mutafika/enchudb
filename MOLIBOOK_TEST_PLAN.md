# MoliBook — EnchuDB 実負荷テスト用 SNS

## 目的
Twitter/Molibook 風の架空 SNS を EnchuDB で構築し、実データ規模の負荷テストを行う。
v26 ペアテーブルの実用性・限界を検証。

## データモデル（全て紐）

### entity 種別
- User entity
- Post entity
- Comment entity
- Like entity (user → post の関係)
- Follow entity (user → user の関係)

### 紐の定義
```
User:
  tie("type", TYPE_USER)       # entity 種別
  tie("status", 0|1|2)        # active/suspended/deleted
  tie_text("username", "...")

Post:
  tie("type", TYPE_POST)
  tie("author", user_eid)      # entity 参照
  tie("visibility", 0|1|2)    # public/followers/private
  tie("year", 2024..2026)
  tie("month", 1..12)
  tie_text("content", "...")

Comment:
  tie("type", TYPE_COMMENT)
  tie("post", post_eid)
  tie("author", user_eid)

Like:
  tie("type", TYPE_LIKE)
  tie("post", post_eid)
  tie("user", user_eid)

Follow:
  tie("type", TYPE_FOLLOW)
  tie("follower", user_eid)
  tie("following", user_eid)
```

## スケール

| レベル | Users | Posts | Likes | Follows | Comments | 合計 entity |
|---|---|---|---|---|---|---|
| Small | 1K | 10K | 100K | 5K | 20K | 136K |
| Medium | 10K | 100K | 1M | 50K | 200K | 1.36M |
| Large | 100K | 1M | 10M | 500K | 2M | 13.6M |
| Twitter | 1M | 10M | 100M | 5M | 20M | 136M |

## テストクエリ

### タイムライン（最頻出）
```
# user_42 がフォローしてる人の public posts
1. follow: type=FOLLOW, follower=user_42 → following のリスト
2. posts: type=POST, author=following[i], visibility=PUBLIC, year=2026
→ ペアテーブル (type, author) + (type, visibility) が効く
```

### いいね数カウント
```
type=LIKE, post=post_123 → count
→ ペアテーブル (type, post) で O(1)
```

### ユーザー検索
```
type=USER, status=ACTIVE → entity リスト
→ ペアテーブル (type, status)
```

### トレンド（重い）
```
type=LIKE, year=2026, month=4 → group by post → top N
→ ペアテーブル (type, year) + Column 直読み
```

### フォロワー数
```
type=FOLLOW, following=user_42 → count
→ ペアテーブル (type, following)
```

### コメント取得
```
type=COMMENT, post=post_123 → entity リスト + author の get
→ ペアテーブル (type, post)
```

## ベンチ項目

### 書き込み
- [ ] 初期データ投入速度 (bulk insert)
- [ ] 投稿追加 (1件の post + tie 5本)
- [ ] いいね追加 (1件の like + tie 3本)
- [ ] フォロー追加/解除
- [ ] rebuild_pairs の時間

### 読み取り
- [ ] タイムライン取得 (フォロー100人のposts)
- [ ] いいね数カウント
- [ ] フォロワー数カウント
- [ ] ユーザー検索 (status=active)
- [ ] コメント取得
- [ ] トレンド集計

### 破壊
- [ ] 100万いいね一括投入
- [ ] ユーザー削除 (cascade: posts, likes, follows)
- [ ] 大量フォロー/アンフォロー
- [ ] flush/open サイクル
- [ ] 差分更新 10万件 vs フル rebuild 整合性

### メモリ
- [ ] 各スケールでのペアテーブルメモリ使用量
- [ ] author の値域が大きい場合のペア爆発

## 懸念点

### author の値域
- 100万ユーザー → author のカーディナリティ = 100万
- (type, author) ペアのセル数 = entity種別数 × 100万
- define_himo で max_values 指定しないとペアテーブル対象外
- **author はペアテーブルに入れない方がいい？** → Cylinder 直読みに fallback

### タイムライン
- フォロー100人の posts を取得 → 100回の query
- 1回 100ns でも 100回 = 10μs
- これで十分か？

## 技術スタック
- Rust (bin crate)
- enchudb = { path = "../enchudb", features = ["v26"] }
- データ生成: rand
- ベンチ: std::time::Instant

## ディレクトリ
```
/Users/kubo/Desktop/mutafika/molibook/
  Cargo.toml
  src/
    main.rs       # データ生成 + ベンチ実行
    model.rs      # entity 種別定数、紐名定義
    generate.rs   # ランダムデータ生成
    bench.rs      # 各種ベンチ
```
