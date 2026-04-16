# 分散 EnchuDB — delta レプリケーション設計メモ

## 核心

v27 のダブルデルタ(writer queue + consumer)をノード間に伸ばす。
Op ログがそのままレプリケーションメッセージになる。

```
tie_async → [local queue] → [local consumer] → EnchuDB(ns 反映)
                           → [replication]    → network → remote consumer
```

## 紐独立 = conflict が閉じる

- 異なる紐の同時書き込みは conflict しない
- 衝突は「同じ entity × 同じ紐」だけ
- last-writer-wins(HLC タイムスタンプ)で十分
- 分散トランザクション(2PC)不要

## Op ログ

```rust
struct ReplicatedOp {
    op: Op,          // Tie/Untie/Delete(既存、12-16 byte)
    hlc: u64,        // Hybrid Logical Clock
    node_id: u32,    // 発信元ノード
}
```

## Entity ID の分散

ノードごとに range 分割:
- Node 0: entity 0..1M
- Node 1: entity 1M..2M
- ...

または上位 8 bit = node_id、下位 24 bit = local eid。

## アーキテクチャ

```
Edge Tokyo          Edge Osaka          Edge NY
  EnchuDB             EnchuDB             EnchuDB
  consumer            consumer            consumer
     ↕                   ↕                   ↕
  [delta log]  ←→  [delta log]  ←→  [delta log]
              gossip / push
```

各 edge に EnchuDB(in-process)。書き込みは local に ns 反映、delta を他ノードに配信(ms〜秒)。

## WASM + WebRTC = サーバーレス P2P

```
ブラウザ A (EnchuDB WASM)  ←WebRTC→  ブラウザ B
     ↕ delta                              ↕ delta
ブラウザ C (EnchuDB WASM)
```

サーバーなし。DB クエリは µs(ブラウザ内)。データ同期は delta gossip。

## 必要な追加要素

| 要素 | 現状 | 分散に必要 |
|---|---|---|
| Op ログ | SegQueue(揮発) | WAL 永続化 + 送信 |
| 順序付け | push_count(ローカル) | HLC |
| Entity ID | ローカル連番 | ノードごと range |
| Conflict | なし(single writer) | LWW(紐単位) |
| 転送 | なし | TCP/QUIC/gossip |

## delta が分散に向いてる理由

1. Op が小さい(12-16 byte) → 帯域最小
2. 紐独立 → conflict が紐単位に閉じる
3. consumer パターン → remote delta を local consumer に流すだけ
4. idempotent → 同じ Op を 2 回適用しても結果同じ(tie は上書き)

## 実装段階(将来)

1. Op ログの WAL 永続化
2. HLC + ノード ID + entity ID range
3. 2 ノード間 TCP レプリケーション(最小 PoC)
4. gossip / 多ノード
5. WASM + WebRTC P2P
