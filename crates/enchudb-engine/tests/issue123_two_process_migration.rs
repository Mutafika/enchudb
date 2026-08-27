//! #123: **本物の 2 プロセス**で、 writer が v7 → v8 (VIX2 → VIX3) migration を走らせて
//! いる最中に、 別プロセスの readonly reader が値を引き続けても壊れないこと。
//!
//! 既存の「cross-process」テスト (`issue106_leaf_cross_process.rs`) は同一プロセス内の
//! 別 Engine ハンドルで、 プロセスを跨いでいない。 oboro / sinfo-studio が
//! `open_readonly` の別プロセスで読む運用をしているので、 そこを実プロセスで固定する。
//!
//! 子プロセスはこのテストバイナリ自身を `ENCHUDB_ISSUE123_CHILD` 付きで起動して兼ねる。

use enchudb_engine::{Engine, ValueType};

const CHILD_ENV: &str = "ENCHUDB_ISSUE123_CHILD";

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/tmp/issue123_2p_{}_{}_{}.enchu", tag, std::process::id(), nanos)
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    for ext in ["lock", "oplog", "schema", "tables"] {
        let _ = std::fs::remove_file(format!("{}.{}", path, ext));
    }
}

fn keys() -> Vec<String> {
    // #123 の実例と同じ形 (旧 slot 選択では下位ビットが衝突する)
    (0..300).map(|i| format!("第{}条", 10 + i)).collect()
}

/// 子: writer として open (= migration が走る) し、 しばらく churn してから抜ける。
fn child_writer(path: &str) {
    let eng = Engine::open(path).expect("child: writer open");
    let ks = keys();
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(1200);
    let mut n = 0usize;
    while std::time::Instant::now() < deadline {
        // 既存 eid の body を re-tie し続ける (Leaf slot churn)
        let eid = (n % ks.len()) as u64;
        let hid = eng.himo_id("body").unwrap() as u16;
        eng.tie_bytes_to_by_id(eid, hid, format!("本文-{}-{}", n % 7, "z".repeat(80)).as_bytes());
        n += 1;
    }
    println!("child: {} writes", n);
}

#[test]
fn readonly_process_reads_while_writer_migrates() {
    if let Ok(path) = std::env::var(CHILD_ENV) {
        child_writer(&path);
        return;
    }

    let path = tmp_path("mig");
    cleanup(&path);
    let ks = keys();

    // v7 相当の初期状態を作る (このビルドは v8 で書くので、 migration そのものではなく
    // **2 プロセス下の rebuild / churn 耐性**を見る。 実 v7 DB からの migration は
    // scratchpad の v7fixture/v7verify で end-to-end 検証済み)。
    {
        let mut eng = Engine::create_growable_tiny(&path).expect("create");
        eng.define_himo("art", ValueType::Tag, 4096);
        eng.define_himo("body", ValueType::Leaf, 0);
        for k in &ks {
            let eid = eng.entity().unwrap();
            eng.tie_text(eid, "art", k);
            eng.tie_text(eid, "body", &format!("本文-init-{}", k));
        }
        let _ = eng.flush();
    }

    // 子 (writer) を起動
    let exe = std::env::current_exe().expect("current_exe");
    let mut child = std::process::Command::new(exe)
        .args(["readonly_process_reads_while_writer_migrates", "--exact", "--nocapture"])
        .env(CHILD_ENV, &path)
        .spawn()
        .expect("spawn child writer");

    // 親: readonly で開いて Tag を引き続ける (vocab index 経路)
    let mut rounds = 0usize;
    let mut mismatch = 0usize;
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(1200);
    while std::time::Instant::now() < deadline {
        let ro = match Engine::open_readonly(&path) {
            Ok(e) => e,
            Err(_) => continue, // writer が header を書き換えている瞬間は retry
        };
        for (i, k) in ks.iter().enumerate() {
            if ro.get_text_owned(i as u64, "art").as_deref() != Some(k.as_bytes()) {
                mismatch += 1;
            }
        }
        rounds += 1;
    }
    let status = child.wait().expect("child wait");

    assert!(status.success(), "child writer が失敗した: {status:?}");
    assert!(rounds > 0, "readonly open が 1 度も成功しなかった");
    assert_eq!(
        mismatch, 0,
        "別プロセス writer と併走中に Tag lookup が {mismatch} 件ずれた (rounds={rounds})"
    );

    cleanup(&path);
}
