//! 球 (試作、 `enchudb_engine::ball`、 履歴は球の面とデッキの差分の帳面に残す): schema の書き込み (insert / upsert / untie / delete) を回ごとに重ね、 回の終わりの時刻を
//! 外から控える。 全部の回の時刻で、 デッキの答え (`ball_pull_at`) と球の答え (`ball_value_at`) を手元の写しと比べる。
//! 最後の時刻の答えは今の読み (`where_eq(..).find()`) とも比べる。 schema の表はこの回数では消した eid を使い回さない
//! (使い回しは engine の `ball::tests::wide_values_and_reused_eids` が見る)。

use enchudb_schema::Database;
use std::collections::BTreeMap;

fn tmp_path(name: &str) -> String {
    format!("/tmp/enchudb-ball-{}-{}", name, std::process::id())
}

fn cleanup(path: &str) {
    let _ = enchudb_engine::db_files::remove_db(path);
}

const CITIES: [&str; 4] = ["Tokyo", "Osaka", "Kyoto", "Fukuoka"];

struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// eid → (city, age)。
type Rows = BTreeMap<u64, (Option<&'static str>, Option<i64>)>;

#[test]
fn as_of_answers_match_a_log_of_schema_writes() {
    let path = tmp_path("asof");
    cleanup(&path);
    let mut db = Database::create(&path).unwrap();
    db.table("users").number("id").tag("city").number("age").primary_key("id").build().unwrap();
    let users = db.get_table("users").unwrap();
    let eng = db.engine();
    let mut rows: Rows = BTreeMap::new();
    let mut next_id = 0i64;
    let mut ids: BTreeMap<u64, i64> = BTreeMap::new();
    // 宣言の前からある row
    for i in 0..200i64 {
        let (c, a) = (CITIES[i as usize % 4], i % 50);
        let e = users.insert().set("id", i).set("city", c).set("age", a).commit().unwrap();
        rows.insert(e, (Some(c), Some(a)));
        ids.insert(e, i);
        next_id = i + 1;
    }
    assert!(eng.declare_ball("users.city").unwrap());
    assert!(eng.declare_ball("users.age").unwrap());
    assert!(!eng.declare_ball("users.city").unwrap(), "2 回目は何もしない");
    let mut checks: Vec<(u64, Rows)> = vec![(eng.ball_now(), rows.clone())];
    let mut r = Rng(0x2545_F491_4F6C_DD1D);
    // 値が変わった書き込みの差分の数 (古い値があれば 出た 1、 新しい値があれば 入った 1) を手元で数える
    let (mut city_d, mut age_d) = (0u64, 0u64);
    let diffs = |old: Option<&str>, new: Option<&str>| if old == new { 0 } else { u64::from(old.is_some()) + u64::from(new.is_some()) };
    let diffs_n = |old: Option<i64>, new: Option<i64>| if old == new { 0 } else { u64::from(old.is_some()) + u64::from(new.is_some()) };
    for _round in 0..60 {
        for _ in 0..40 {
            let live: Vec<u64> = rows.keys().copied().collect();
            let e = live[r.below(live.len() as u64) as usize];
            match r.below(10) {
                0..=3 => {
                    let c = CITIES[r.below(4) as usize];
                    users.upsert().set("id", ids[&e]).set("city", c).commit().unwrap();
                    city_d += diffs(rows[&e].0, Some(c));
                    rows.get_mut(&e).unwrap().0 = Some(c);
                }
                4..=5 => {
                    let a = r.below(50) as i64;
                    users.upsert().set("id", ids[&e]).set("age", a).commit().unwrap();
                    age_d += diffs_n(rows[&e].1, Some(a));
                    rows.get_mut(&e).unwrap().1 = Some(a);
                }
                6 => {
                    eng.untie(e, "users.city");
                    city_d += diffs(rows[&e].0, None);
                    rows.get_mut(&e).unwrap().0 = None;
                }
                7 => {
                    users.entity(e).delete().unwrap();
                    city_d += diffs(rows[&e].0, None);
                    age_d += diffs_n(rows[&e].1, None);
                    rows.remove(&e);
                    ids.remove(&e);
                }
                _ => {
                    let (c, a) = (CITIES[r.below(4) as usize], r.below(50) as i64);
                    let e = users.insert().set("id", next_id).set("city", c).set("age", a).commit().unwrap();
                    assert!(!rows.contains_key(&e));
                    city_d += 1;
                    age_d += 1;
                    rows.insert(e, (Some(c), Some(a)));
                    ids.insert(e, next_id);
                    next_id += 1;
                }
            }
        }
        checks.push((eng.ball_now(), rows.clone()));
    }
    let all_eids: Vec<u64> = checks.iter().flat_map(|(_, rs)| rs.keys().copied()).collect::<std::collections::BTreeSet<_>>().into_iter().collect();
    for (k, (t, rs)) in checks.iter().enumerate() {
        for c in CITIES {
            let vid = eng.vocab_id(c).unwrap() as u64;
            let want: Vec<u64> = rs.iter().filter(|(_, (rc, _))| *rc == Some(c)).map(|(&e, _)| e).collect();
            assert_eq!(eng.ball_pull_at("users.city", vid, *t), want, "回 {k} t={t} city={c}");
        }
        for a in [0i64, 7, 49] {
            let want: Vec<u64> = rs.iter().filter(|(_, (_, ra))| *ra == Some(a)).map(|(&e, _)| e).collect();
            assert_eq!(eng.ball_pull_at("users.age", a as u64, *t), want, "回 {k} t={t} age={a}");
        }
        for &e in &all_eids {
            let (c, a) = rs.get(&e).copied().unwrap_or((None, None));
            assert_eq!(eng.ball_value_at(e, "users.city", *t), c.map(|c| eng.vocab_id(c).unwrap() as u64), "回 {k} e={e}");
            assert_eq!(eng.ball_value_at(e, "users.age", *t), a.map(|a| a as u64), "回 {k} e={e}");
        }
    }
    // 今の時刻の答え = 今の読み
    let now = eng.ball_now();
    for c in CITIES {
        let mut cur = users.where_eq("city", c).find().unwrap();
        cur.sort_unstable();
        assert_eq!(eng.ball_pull_at("users.city", eng.vocab_id(c).unwrap() as u64, now), cur, "今の {c}");
    }
    // 値の変わった書き込み (書き換え・外す・消す・足す) は全部、 デッキの差分の帳面に届いた
    assert_eq!(eng.ball_diff_count("users.city"), Some(city_d));
    assert_eq!(eng.ball_diff_count("users.age"), Some(age_d));
    let (faces, _bytes) = eng.ball_stats("users.city").unwrap();
    assert!(faces > 200, "書き換えの面が積まれている ({faces})");
    drop(users);
    drop(db);
    cleanup(&path);
}

#[test]
fn leaf_columns_cannot_be_balls() {
    let path = tmp_path("leaf");
    cleanup(&path);
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").column("memo", enchudb_schema::ColumnType::Leaf).primary_key("id").build().unwrap();
    assert!(db.engine().declare_ball("t.memo").is_err());
    assert!(db.engine().declare_ball("t.nope").is_err());
    drop(db);
    cleanup(&path);
}

/// 書き手 4 本が自分の row 16 個の a (値 0..6) を書いたり外したりする (engine の `tie_to_by_id` / `untie_by_id`)。 回ごとに
/// Barrier でそろえ、 そろった時刻と書き手の写しを控える。 全部の回の時刻で、 デッキのカードの答えと球の答えを写しと比べる
/// (同じ値の帳面に書き手が並んで書き足す・帳面を伸ばして写す、 を並べて通す)。 回の途中の時刻は、 デッキの答えを球の面と突き合わせる。
/// 帳面を名簿の鍵の外で書く変異 (書き換え / 外すの速い道) と、 sparse の道で新しい値の鍵を取らない変異は、 この 2 本の並行の試験で
/// release で 20 回中 20 回落ちる (変異なしは 20 回中 0 回。 6 回目の形 2026-10-26、 8 回目の形 (宣言の前の値 + 控え) 2026-10-27 も同じ)。
#[test]
fn concurrent_writers_leave_history_in_the_ledgers() {
    concurrent_run("conc", &[0, 1, 2, 3, 4, 5], false);
}

/// 上と同じを WAL 付き (concurrent + oplog) で回す: 球の時刻は書き込みの WAL の HLC (名簿の鍵より前に決まる)。 同じ値に書く書き手の
/// HLC の順と鍵の順がずれた時は球の時刻を上げ、 WAL の時計もそこまで進める (柵でそろえた時の 「今」 の答えが後から変わらない)。
#[test]
fn concurrent_writers_with_wal_hlc_times() {
    concurrent_run("walhlc", &[3, 9, 1 << 24, (1 << 24) + 64, 0, 1], true);
}

/// 上と同じを値 2 つで (外すが 3 回に 1 回、 同じ値に重なる書き手が多い = 外す書き込みの時刻を上げる回が多い)。
#[test]
fn concurrent_writers_with_wal_hlc_times_and_many_removes() {
    concurrent_run("walrm", &[5, 6], true);
}

/// 上と同じを、 値の種類が多い列の値 (2^20 以上: 円柱では sparse = write_lock の道、 帳面は値ごとに分けない列 1 本) と、
/// 配列の中の小さい値を混ぜて回す (小さい値 ↔ 大きい値の書き換えは、 write_lock と値の鍵を両方取る道)。 大きい値のうち 3 つは
/// 下位 bit が同じ (帳面の同じ列に落ちる)。
#[test]
fn concurrent_writers_on_wide_values() {
    concurrent_run("wide", &[3, 9, 1 << 24, (1 << 24) + 64, (1 << 24) + 128, (1 << 30) + 1], false);
}

/// 時刻を上げた書き込み (外す / 書き換え) は WAL の時計をそこまで進める: その後に払い出す HLC の書き込みは、 上げた時刻より後に
/// なる。 上げる場面は HLC を指定して書く口 (sync で届いた書き込みの道) で決めた順に作る: e1 が HLC 200 で値 5 に入った後、
/// e0 が HLC 150 で値 5 から出る → 値 5 の帳面の最後 (200) の後 = 201 に上がる。 指定する HLC の wall は時計より 1 時間先
/// (この後に払い出す HLC が上げた時刻を越えるのは、 時計を進めた時だけ)。 並行の試験では上げた書き込みが柵の直前に来た時しか
/// 見えない (外すの後に渡さない変異で 20 回中 10 回)。
#[test]
fn raised_times_move_the_wal_clock() {
    for remove in [false, true] {
        let path = tmp_path(if remove { "raised_rm" } else { "raised_set" });
        cleanup(&path);
        let mut db = Database::create(&path).unwrap();
        db.table("t").number("id").number("a").primary_key("id").build().unwrap();
        let mut eids = Vec::new();
        {
            let t = db.get_table("t").unwrap();
            for i in 0..4i64 {
                eids.push(t.insert().set("id", i).commit().unwrap());
            }
        }
        let db = db.finish_with_oplog(16 << 20).unwrap();
        let eng = db.arc_engine();
        assert!(eng.declare_ball("t.a").unwrap());
        let ha = eng.himo_id("t.a").unwrap() as u16;
        let wall = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64 + 3_600_000;
        let h = |logical| enchudb_oplog::Hlc { wall, logical, peer: 0 };
        let pack = |logical: u64| wall << 23 | logical;
        assert!(eng.set_cell(eids[0], ha, 5, h(100)));
        assert!(eng.set_cell(eids[1], ha, 5, h(200)));
        if remove {
            assert!(eng.clear_cell(eids[0], ha, h(150)));
        } else {
            assert!(eng.set_cell(eids[0], ha, 6, h(150)));
        }
        let faces = eng.ball_faces(eids[0], "t.a");
        assert_eq!(faces[0].to, pack(201), "remove={remove}: 値 5 の帳面の最後 (200) の後に上がる");
        // この後の書き込み (WAL の HLC) は上げた時刻より後
        eng.tie_to_by_id(eids[2], ha, 7u32);
        let later = eng.ball_faces(eids[2], "t.a")[0].from;
        assert!(later > pack(201), "remove={remove}: 上げた時刻 {} の後の書き込みが {later}", pack(201));
        drop(eng);
        drop(db);
        cleanup(&path);
    }
}

fn concurrent_run(name: &str, vals: &[u64], wal: bool) {
    const TH: usize = 4;
    const PER: usize = 16;
    let n_vals = vals.len() as u64;
    let path = tmp_path(name);
    cleanup(&path);
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").number("a").primary_key("id").build().unwrap();
    if wal {
        db.enable_sync().unwrap(); // cell の版数 (書き込みの HLC) を持たせて、 球の時刻と突き合わせる
    }
    // 3 行に 1 行は宣言の前に値を入れておく (球の 「前から」 の面と、 帳面の頭のデッキになる)
    let mut eids = Vec::new();
    let mut seeded = vec![None::<u64>; TH * PER];
    {
        let t = db.get_table("t").unwrap();
        for i in 0..TH * PER {
            let mut row = t.insert().set("id", i as i64);
            if i % 3 == 0 {
                let v = vals[i % vals.len()];
                row = row.set("a", v as i64);
                seeded[i] = Some(v);
            }
            eids.push(row.commit().unwrap());
        }
    }
    let db: std::sync::Arc<Database> = if wal { db.finish_with_oplog(64 << 20).unwrap() } else { std::sync::Arc::new(db) };
    let t = db.get_table("t").unwrap();
    let eng = db.arc_engine();
    eng.ball_set_snap_min(16); // 控えを作る書き手と、 帳面に足す書き手が重なる
    assert!(eng.declare_ball("t.a").unwrap());
    let ha = eng.himo_id("t.a").unwrap() as u16;
    let rounds = 200;
    let bar = std::sync::Barrier::new(TH + 1);
    let diffs = std::sync::atomic::AtomicU64::new(0);
    let log = std::sync::Mutex::new(vec![None::<u64>; TH * PER]);
    // 値を変えた最後の書き込みの HLC (WAL 付きの時、 書いた直後に cell の版数から読む)
    let changed_at = std::sync::Mutex::new(vec![enchudb_oplog::Hlc::ZERO; TH * PER]);
    // 回ごとの、 値を変えた書き込みの数
    let changes = std::sync::Mutex::new(vec![0u64; rounds]);
    let mut checks: Vec<(u64, Vec<Option<u64>>)> = Vec::new();
    std::thread::scope(|s| {
        for th in 0..TH {
            let (eng, bar, log, eids, diffs, changed_at, changes) = (&eng, &bar, &log, &eids, &diffs, &changed_at, &changes);
            let vals = vals;
            let mut mine = seeded[th * PER..(th + 1) * PER].to_vec();
            s.spawn(move || {
                let mut r = Rng(0x9E37_79B9_7F4A_7C15 ^ th as u64);
                for round in 0..rounds {
                    let mut changed = 0;
                    for _ in 0..30 {
                        let k = r.below(PER as u64) as usize;
                        let e = eids[th * PER + k];
                        let new = match r.below(n_vals + 1) {
                            k if k == n_vals => {
                                eng.untie_by_id(e, ha);
                                None
                            }
                            k => {
                                let v = vals[k as usize];
                                eng.tie_to_by_id(e, ha, v as u32);
                                Some(v)
                            }
                        };
                        if mine[k] != new {
                            changed += 1;
                            diffs.fetch_add(u64::from(mine[k].is_some()) + u64::from(new.is_some()), std::sync::atomic::Ordering::Relaxed);
                            if wal {
                                changed_at.lock().unwrap()[th * PER + k] = eng.cell_hlc(e, ha);
                            }
                        }
                        mine[k] = new;
                    }
                    log.lock().unwrap()[th * PER..(th + 1) * PER].copy_from_slice(&mine);
                    changes.lock().unwrap()[round] += changed;
                    bar.wait();
                    bar.wait();
                }
            });
        }
        for _ in 0..rounds {
            bar.wait();
            checks.push((eng.ball_now(), log.lock().unwrap().clone()));
            bar.wait();
        }
    });
    for (k, (tm, want)) in checks.iter().enumerate() {
        for &v in vals {
            let deck: Vec<u64> = (0..TH * PER).filter(|&i| want[i] == Some(v)).map(|i| eids[i]).collect();
            assert_eq!(eng.ball_pull_at("t.a", v, *tm), deck, "回 {k} t={tm} v={v}");
        }
        for i in 0..TH * PER {
            assert_eq!(eng.ball_value_at(eids[i], "t.a", *tm), want[i], "回 {k} e={}", eids[i]);
        }
    }
    // 控えは書き換え / 外すの両方の道で作る (登録した書き手が作るので、 書き手が返った後は登録した数 = 作り終えた数)
    let (registered, built) = eng.ball_snap_counts("t.a").unwrap();
    assert!(built > 50, "控えの道を通っていない: {built}");
    assert_eq!(registered, built, "登録した控えは全部作る");
    // 宣言の時 (時刻 0) のデッキは、 宣言の前に入れた値
    for &v in vals {
        let want: Vec<u64> = (0..TH * PER).filter(|&i| seeded[i] == Some(v)).map(|i| eids[i]).collect();
        assert_eq!(eng.ball_pull_at("t.a", v, 0), want, "時刻 0 v={v}");
    }
    // 柵でそろえた時刻 now(r) 以下の球の時刻 (値を変えた書き込み 1 回につき 1 つ: 書き換えは前の面を閉じた時刻 = 次の面の上を
    // 向いた時刻) の数は、 回 r までに値を変えた書き込みの数。 後の回の書き込みが過去の時刻に入ると (上げた時刻を WAL の時計に
    // 渡さない等)、 デッキの答えに出なくても数が合わない
    let mut times_of: Vec<Vec<u64>> = eids
        .iter()
        .map(|&e| {
            let mut ts: Vec<u64> = eng.ball_faces(e, "t.a").iter().flat_map(|f| [f.from, f.to]).filter(|&x| x != 0 && x != u64::MAX).collect();
            ts.sort_unstable();
            ts.dedup();
            ts
        })
        .collect();
    times_of.iter_mut().for_each(|t| t.shrink_to_fit());
    let changes = changes.into_inner().unwrap();
    let mut done = 0u64;
    for (r, (tm, _)) in checks.iter().enumerate() {
        done += changes[r];
        let seen: u64 = times_of.iter().map(|t| t.partition_point(|&x| x <= *tm) as u64).sum();
        assert_eq!(seen, done, "回 {r} t={tm}: 柵までの球の時刻の数と、 値を変えた書き込みの数");
    }
    // 時刻の元: WAL 付きは HLC (wall ms << 23 = 2^40 より大きい)、 無ければ数え口
    assert_eq!(eng.ball_now() > 1 << 40, wal, "時刻の元が違う: {}", eng.ball_now());
    if wal {
        // 球の最後の時刻 (書き換え / 外すとも) は、 値を変えた最後の書き込みの WAL の HLC 以上 (上げた時だけ大きい)。
        // 同じ値の書き直しは球を回さないので、 cell の今の版数でなく書き手が控えた HLC と比べる
        let changed_at = changed_at.into_inner().unwrap();
        let mut checked = 0;
        for (i, &e) in eids.iter().enumerate() {
            let h = changed_at[i];
            if h == enchudb_oplog::Hlc::ZERO {
                continue;
            }
            let last = eng.ball_faces(e, "t.a").last().map(|f| if f.to == u64::MAX { f.from } else { f.to }).unwrap();
            assert!(last >= h.wall << 23 | h.logical as u64, "e={e} 球の最後 {last} < HLC {h:?}");
            checked += 1;
        }
        assert!(checked > TH * PER / 2, "版数を持つ cell が少ない: {checked}");
    }
    // 回の途中の時刻も: デッキの帳面の答え = 球の面から集めた答え (最後の 20 回の、 球の面の時刻の境目とその 1 つ前を全部)
    let from = checks[checks.len().saturating_sub(21)].0;
    let mut times = std::collections::BTreeSet::new();
    for &e in &eids {
        for f in eng.ball_faces(e, "t.a") {
            for x in [f.from, f.to] {
                if x >= from && x != u64::MAX {
                    times.insert(x);
                    times.insert(x - 1);
                }
            }
        }
    }
    assert!(times.len() > 100, "途中の時刻が少ない: {}", times.len());
    for tm in times {
        for &v in vals {
            let faces: Vec<u64> = eids.iter().copied().filter(|&e| eng.ball_value_at(e, "t.a", tm) == Some(v)).collect();
            assert_eq!(eng.ball_pull_at("t.a", v, tm), faces, "t={tm} v={v}");
        }
    }
    // 今の読み (名簿) も合う
    let last = &checks.last().unwrap().1;
    for &v in vals {
        let mut cur = eng.pull_raw("t.a", v as u32);
        cur.sort_unstable();
        let want: Vec<u64> = (0..TH * PER).filter(|&i| last[i] == Some(v)).map(|i| eids[i]).collect();
        assert_eq!(cur, want, "今の v={v}");
    }
    assert_eq!(eng.ball_diff_count("t.a"), Some(diffs.into_inner()));
    drop(t);
    drop(db);
    cleanup(&path);
}
