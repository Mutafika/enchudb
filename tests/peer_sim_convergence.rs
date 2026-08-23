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

/// **削除が受信側の再起動を跨いで効くか** (syncretic の「亡霊」観測の切り分け)。
///
/// `Delete` は himo を持たないので、 受信側は `resolve_remote_eid_existing`
/// (= 既存の eid 写像) でしか宛先を引けない。 引けなければ `apply_one` は false を
/// 返し、 `skipped` に 1 数えられて **cursor はそれを越えて前進する** =
/// 二度と再配送されない。 相手は消したのにこちらは行が生きたまま残る。
///
/// eid 写像は `.eidmap` sidecar に永続する設計なので、 clean な再起動なら効くはず。
/// ここが通れば「写像の喪失」は亡霊の原因から外れる。
#[test]
fn delete_applies_after_receiver_restart() {
    let mut sim = PeerSim::new("delrestart", 2);
    let from_b = sim.peer_id(1);

    // 1. peer B が author → peer A が materialize
    let b = sim.author_text(1, "name", "doomed");
    sim.deliver(b.all());
    sim.pull(0, from_b);
    assert_eq!(sim.read(0, b.eid, "name").as_deref(), Some(&b"doomed"[..]));

    // 2. 受信側 (A) を再起動。 memory 上の写像は消えるが `.eidmap` は残る。
    sim.restart(0);

    // 3. B が削除 → A が pull
    let del = sim.author_delete(1, b.eid);
    sim.deliver(vec![del]);
    let out = sim.pull(0, from_b);
    eprintln!(
        "[delete] pull: received={} applied={} skipped={}",
        out.received, out.applied, out.skipped
    );

    assert_eq!(
        sim.read(0, b.eid, "name"),
        None,
        "削除が届いていない = 亡霊。 skipped に紛れて cursor が越えると二度と来ない"
    );
    sim.assert_converged();
}

/// **eid 写像は clean close を挟まずに耐えるか** (SIGKILL 相当)。
///
/// `delete_applies_after_receiver_restart` が通るのは in-process の drop が
/// 必ず clean shutdown になるから。 syncretic の chaos harness は SIGKILL を
/// 混ぜているので、 「apply した直後に電源が落ちたら `.eidmap` に載っているか」
/// が本当の分かれ目になる。
///
/// 載っていなければ: 写像を失う → 後続の `Delete` が
/// `resolve_remote_eid_existing` で外れる → `skipped` に紛れて cursor が越える
/// → 二度と再配送されない = 相手は消したのにこちらは行が生きたまま。
#[test]
fn eid_mapping_is_durable_without_clean_shutdown() {
    let mut sim = PeerSim::new("crashdur", 2);
    let from_b = sim.peer_id(1);

    let b = sim.author_text(1, "name", "doomed");
    sim.deliver(b.all());
    sim.pull(0, from_b);
    assert_eq!(sim.read(0, b.eid, "name").as_deref(), Some(&b"doomed"[..]));

    // clean shutdown を挟まずに、 今 disk にある物だけで開き直す。
    let crashed = sim.crash_snapshot(0);
    // harness の copy 忠実度の問題ではないことを先に確認する。
    eprintln!(
        "[crash] copied sidecars: {:?}",
        std::fs::read_dir(std::path::Path::new(&sim.db_path(0)).parent().unwrap())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.contains("crashcopy"))
            .collect::<Vec<_>>()
    );
    eprintln!(
        "[crash] eidmap sidecar bytes: live={:?} copy={:?}",
        std::fs::metadata(format!("{}.eidmap", sim.db_path(0))).map(|m| m.len()).ok(),
        std::fs::metadata(format!("{}.crashcopy.eidmap", sim.db_path(0))).map(|m| m.len()).ok(),
    );
    let durable = crashed.resolve_remote_eid_existing(b.eid).is_some();

    // 回避策の確認: `persist_tables()` は `&self` で eidmap も一緒に fsync する。
    // pull 直後にこれを呼べば窓を閉じられるなら、 caller 側の当座の手当てになる。
    sim.engine(0).persist_tables().unwrap();
    let after = sim.crash_snapshot_at(0, "afterpersist");
    let durable_after_persist = after.resolve_remote_eid_existing(b.eid).is_some();
    eprintln!(
        "[crash] durable: apply 直後={} persist_tables()後={}",
        durable, durable_after_persist
    );

    assert!(
        durable,
        "apply 直後に落ちると eid 写像が消える — 以降その entity 宛の Delete は \
         宛先不明で捨てられ、 cursor が越えるので二度と届かない \
         (persist_tables() 後は {})",
        durable_after_persist
    );
}

/// **cursor は、それが消費した state より先に durable になってはいけない**。
///
/// `Syncer::pull_once` の中の順序は
///
/// 1. `apply_records` — eid 写像は **memory だけ**
/// 2. `save_cursors()` — **cursor は disk**
/// 3. return → caller が `persist_tables()` — ここでやっと写像が disk
///
/// 2 と 3 の間で落ちると「cursor は進んだが写像は無い」が確定する。 caller から
/// は直せない (return 時点で cursor は既に落ちている)。 差分 pull でも埋まらない
/// — cursor が越えているので該当 record は二度と来ない。
///
/// これが `Delete` に当たると、 相手は消したのにこちらは行が生きたまま残る。
#[test]
fn cursor_never_outlives_the_state_it_consumed() {
    let mut sim = PeerSim::new("cursororder", 2);
    let from_b = sim.peer_id(1);

    let b = sim.author_text(1, "name", "doomed");
    sim.deliver(b.all());

    // pull_once から戻った直後 = caller がまだ何もしていない時点。
    sim.pull(0, from_b);

    let cursor = sim.persisted_cursor(0, from_b);
    let crashed = sim.crash_snapshot(0);
    let mapping = crashed.resolve_remote_eid_existing(b.eid);
    eprintln!("[order] 永続 cursor={:?}  crash 後の eid 写像={:?}", cursor, mapping);

    assert!(
        !(cursor.is_some() && mapping.is_none()),
        "cursor は {:?} まで永続しているのに、 その record が作った eid 写像は \
         disk に無い。 ここで落ちると当該 record は永久に失われる",
        cursor
    );
}
