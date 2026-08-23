//! peer 試験環境 (`common::peer_sim`) の受け入れ基準そのもの。
//!
//! 判定は 1 本: **アプリ層の再 author / bootstrap を一切挟まずに peer が収束するか**。
//!
//! sync の欠陥は長らく実アプリ (syncretic) の症状から逆算していたが、 アプリ固有の
//! 復旧手段 (disk からの再 scan) を前提にすると、 library として壊れていても
//! 気付けない。 ここではその逃げ道を塞いだ状態で判定する。

mod common;

use common::peer_sim::PeerSim;

/// 正常系。 `Vocab` と `Tie` が同じ pull に入れば収束する。
#[test]
fn text_converges_when_vocab_and_tie_arrive_together() {
    let mut sim = PeerSim::new("together", 2);

    let a = sim.author_text(0, "name", "alpha");
    sim.deliver(a.all());
    sim.pull_all();

    assert_eq!(sim.read(1, a.eid, "name").as_deref(), Some(&b"alpha"[..]));
    sim.assert_converged();
}

/// **再起動を跨ぐ vocab 写像の欠落**。
///
/// `peer_vocab_map` は memory 上にしか無く、 WAL からも `_sync_ops` からも
/// 再構築されない (`Syncer::hydrate_one` は `DecodedOp::Vocab` を捨てる)。
/// 一方 pull cursor は `with_cursor_path` で永続する。 この非対称のせいで
/// 「cursor は Vocab を消費済みと言うが、 写像はもう無い」窓ができる。
///
/// syncretic の実地発現 (15962 行中 12 行が破損、 再起動 28 回、
/// `history_truncated` は 0 回) と同じ形。
#[test]
fn text_converges_across_peer_restart() {
    let mut sim = PeerSim::new("restart", 2);

    // peer B 側の vid 空間を埋めておく。 fresh store 同士は intern 順が対称で
    // vid 番号が衝突するので、 生の remote vid をそのまま書く実装では
    // **B 自身の無関係な文字列**が peer A の値として見えることになる
    // (syncretic の `path` 列に別行の PK が入った症状と同じ)。
    for i in 0..8 {
        sim.write_local_text(1, "name", &format!("B-LOCAL-DECOY-{}", i));
    }

    // 1. peer A が author し、 `Vocab` だけが先に B へ届いて消費される。
    //    実運用では commit group の途中で batch が切れる / ring 窓の reclaim で
    //    `Vocab` だけ落ちる、 に相当。
    let a = sim.author_text(0, "name", "alpha");
    sim.deliver(vec![a.vocab.clone()]);
    let from_a = sim.peer_id(0);
    sim.pull(1, from_a);

    // 2. B を再起動。 写像は消え、 cursor は残る。
    sim.restart(1);

    // 3. `Tie` が届く。 B にはもう `(A, vid) → local vid` が無い。
    sim.deliver(vec![a.tie.clone()]);
    let out = sim.pull(1, from_a);
    eprintln!(
        "[restart] pull: received={} applied={} skipped={}",
        out.received, out.applied, out.skipped
    );
    eprintln!(
        "[restart] peer B が見ている値: {:?}",
        sim.read(1, a.eid, "name").map(|b| String::from_utf8_lossy(&b).into_owned())
    );

    // 4. 以降いくら sync を回しても、 アプリ層が介入しない限り直らない。
    for _ in 0..3 {
        sim.pull_all();
    }
    sim.assert_converged();
}

/// **根因の pin**: 消費済みの `Vocab` 写像は再起動で消えるが、 cursor は消えない。
///
/// `peer_vocab_map` は memory 上にしか無く、 WAL からも `_sync_ops` からも
/// 再構築されない (`Syncer::hydrate_one` は `DecodedOp::Vocab` を捨てる)。
/// 同種の `(peer, remote_eid) → local` 写像が `.eidmap` で sidecar 永続化
/// されているのと非対称。
///
/// この非対称が消えれば `text_converges_across_peer_restart` も自然に通る。
/// 逆に、 apply 地点で「翻訳できないなら書かない」に変えるだけでは通らない —
/// 消えた写像は戻らないので、 cell が壊れる代わりに永久に古いままになる。
#[test]
fn consumed_vocab_mapping_survives_restart() {
    let mut sim = PeerSim::new("vocabgap", 2);
    let from_a = sim.peer_id(0);

    let a = sim.author_text(0, "name", "alpha");
    sim.deliver(vec![a.vocab.clone()]);
    sim.pull(1, from_a);
    assert!(
        sim.has_vocab_mapping(1, &a, from_a),
        "Vocab を適用した直後は写像がある"
    );

    sim.restart(1);

    assert!(
        sim.has_vocab_mapping(1, &a, from_a),
        "再起動後も写像が残っていること — 消えると後続 Tie を翻訳できず、 \
         cursor は Vocab を消費済みと言うので二度と再配送されない"
    );
}
