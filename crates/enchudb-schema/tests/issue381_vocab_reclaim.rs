//! #381: 辞書 (Tag の値) の語の回収。 一意な ID を Tag の主キーに持ち、 古い行から消す表でも、 辞書は生きている
//! 行の分しか使わない。 回収した番号は世代が進むので、 古い番号は別の値に当たらない。

use enchudb_schema::{Database, GrowableOptions, Value};
use std::collections::VecDeque;

fn tmp(tag: &str) -> String {
    let p = format!(
        "/tmp/enchudb-schema-issue381-{}-{}-{}.db",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    );
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn opts(reclaim: bool) -> GrowableOptions {
    GrowableOptions { max_entities: 1_000, vocab_max_entries: Some(16_000), vocab_reclaim: reclaim, ..Default::default() }
}

/// issue の再現: 生きている行は常に 100 行、 一意な id を 5 万個。 回収しない辞書は 16,000 個目で止まる。
#[test]
fn rolling_unique_ids_stay_within_the_vocab_budget() {
    for reclaim in [false, true] {
        let path = tmp(if reclaim { "reclaim" } else { "plain" });
        let mut b = Database::create_growable_with(&path, opts(reclaim)).unwrap();
        b.table("events").tag("id").tag("owner").primary_key("id").build().unwrap();
        let db = b.finish_with_oplog(16 << 20).unwrap();
        let t = db.get_table("events").unwrap();
        let mut live = VecDeque::new();
        let mut failed_at = None;
        for i in 0..50_000u64 {
            match t.insert().set("id", format!("ev_{i:016x}")).set("owner", "u1").commit() {
                Ok(eid) => live.push_back((i, eid)),
                Err(_) => {
                    failed_at = Some(i);
                    break;
                }
            }
            while live.len() > 100 {
                t.entity(live.pop_front().unwrap().1).delete().unwrap();
            }
        }
        let u = db.engine().vocab_usage();
        if !reclaim {
            assert!(failed_at.is_some_and(|i| i < 16_000), "回収しない辞書は上限で止まる: {failed_at:?} {u:?}");
            continue;
        }
        assert_eq!(failed_at, None, "回収する辞書は止まらない: {u:?}");
        assert!(u.reclaim && u.reclaim_ready, "{u:?}");
        assert!(u.entries < 1_000, "使う場所は生きている行の分 + 余裕: {u:?}");
        assert!(u.reclaimed > 40_000, "{u:?}");
        // 生きている行は id で引ける、 消した行の id は引けない
        for &(i, eid) in &live {
            assert_eq!(t.where_eq("id", format!("ev_{i:016x}")).find_one().unwrap(), Some(eid), "{i}");
            assert_eq!(t.entity(eid).get("owner"), Some(Value::Text("u1".into())));
        }
        assert_eq!(t.where_eq("id", "ev_0000000000000000").find_one().unwrap(), None);
        assert_eq!(t.where_eq("owner", "u1").count().unwrap(), 100);
    }
}

/// 既存 DB を後から回収する DB にする: 印を立てて開き直すと、 開く時に既存の cell を数えて使い回す。 生きている値は
/// 使い回さない。
#[test]
fn enabling_on_an_existing_db_takes_effect_on_reopen() {
    let path = tmp("enable");
    {
        let mut b = Database::create_growable_with(&path, opts(false)).unwrap();
        b.table("kv").tag("k").tag("v").primary_key("k").build().unwrap();
        let t = b.get_table("kv").unwrap();
        for i in 0..300 {
            t.insert().set("k", format!("k{i}")).set("v", format!("v{i}")).commit().unwrap();
        }
        // 半分消す (その値の語は参照 0 になる)
        for i in 0..150 {
            let e = t.where_eq("k", format!("k{i}")).find_one().unwrap().unwrap();
            t.entity(e).delete().unwrap();
        }
        b.engine().enable_vocab_reclaim().unwrap();
        assert!(!b.engine().vocab_usage().reclaim, "開き直すまでは回収しない");
    }
    let db = Database::open(&path).unwrap();
    // 開く時に既存の cell を数え終えている (#385)
    let u = db.engine().vocab_usage();
    assert!(u.reclaim && u.reclaim_ready, "{u:?}");
    // 消した 150 行の k と v + 表名 (schema が辞書に入れるが cell からは参照しない)
    assert_eq!(u.reclaimable_entries, 301, "{u:?}");
    let before = u.entries;
    let t = db.get_table("kv").unwrap();
    for i in 1000..1300 {
        t.insert().set("k", format!("k{i}")).set("v", format!("v{i}")).commit().unwrap();
    }
    let u = db.engine().vocab_usage();
    assert_eq!(u.entries, before + 299, "空いた 301 か所を使い回し、 残りの 299 語は新しい場所: {u:?}");
    assert_eq!(u.reclaimed, 301);
    for i in (150..300).chain(1000..1300) {
        let e = t.where_eq("k", format!("k{i}")).find_one().unwrap().unwrap_or_else(|| panic!("k{i}"));
        assert_eq!(t.entity(e).get("v"), Some(Value::Text(format!("v{i}"))), "{i}");
    }
}

/// 行の無い値の購読は、 その値の番号が使い回された後も、 その値の行が入れば届く (購読は番号を押さえている)。
#[test]
fn subscription_on_a_text_survives_reuse() {
    let path = tmp("live");
    let mut b = Database::create_growable_with(&path, opts(true)).unwrap();
    b.table("events").tag("id").primary_key("id").build().unwrap();
    let db = b.finish_with_oplog(16 << 20).unwrap();
    let t = db.get_table("events").unwrap();
    let e = t.insert().set("id", "watched").commit().unwrap();
    let live = t.where_eq("id", "watched").subscribe().unwrap();
    assert_eq!(live.poll().added.len(), 1);
    t.entity(e).delete().unwrap();
    assert_eq!(live.poll().removed.len(), 1);
    // 他の値を作っては消して、 空いた場所を使い回す
    for i in 0..2_000 {
        let x = t.insert().set("id", format!("other{i}")).commit().unwrap();
        t.entity(x).delete().unwrap();
    }
    assert!(db.engine().vocab_usage().reclaimed > 1_000);
    let again = t.insert().set("id", "watched").commit().unwrap();
    let d = live.poll();
    assert_eq!(d.added.len(), 1, "使い回しの後も届く: {d:?}");
    assert_eq!(t.where_eq("id", "watched").find_one().unwrap(), Some(again));
}

/// 否定の条件 (`where_ne`) の購読が覚えた番号も、 使い回しの後に引き直す (覚えたままだと、 入れ直した値の行を
/// 「その値でない」 と数える)。
#[test]
fn negated_subscription_rereads_a_reused_text() {
    let path = tmp("ne");
    let mut b = Database::create_growable_with(&path, opts(true)).unwrap();
    b.table("events").tag("id").tag("kind").primary_key("id").build().unwrap();
    let db = b.finish_with_oplog(16 << 20).unwrap();
    let t = db.get_table("events").unwrap();
    let keep = t.insert().set("id", "keep").set("kind", "a").commit().unwrap();
    let x = t.insert().set("id", "x").set("kind", "hot").commit().unwrap();
    let live = t.all().where_ne("kind", "hot").subscribe().unwrap();
    assert_eq!(live.poll().added, vec![keep]);
    t.entity(x).delete().unwrap();
    for i in 0..2_000 {
        let y = t.insert().set("id", format!("y{i}")).set("kind", format!("k{i}")).commit().unwrap();
        t.entity(y).delete().unwrap();
    }
    live.poll();
    assert!(db.engine().vocab_usage().reclaimed > 1_000);
    let again = t.insert().set("id", "x2").set("kind", "hot").commit().unwrap();
    live.poll();
    assert_eq!(live.count(), 1, "入れ直した hot の行は 「hot でない」 に入らない");
    assert_eq!(t.all().where_ne("kind", "hot").find().unwrap(), vec![keep]);
    let _ = again;
}
