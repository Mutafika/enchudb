//! #435: sync で届いた Leaf の値 (`remote_tieleaf_apply`) が、 行の lock を取るまでの間に入った新しい書き込みに版数で
//! 負けた時、 cell が指している slot (勝った書き込みの値) を空きに戻さない。
//!
//! 旧: lock の中の版数の判定 (`set_cell_local`) の返り値を見ずに、 lock を取った時の cell の slot を空きに戻していた。
//! その slot が後の書き込みで使い回されると、 cell は別の値を読んだ。 返り値も `Applied` だった。
//!
//! 行の lock を握った thread の後ろで受信を待たせ、 その間に新しい値を書いてから離す (決定的)。

use enchudb_engine::{Engine, RemoteApply, ValueType};
use std::sync::Arc;

#[test]
fn remote_tieleaf_that_loses_to_a_newer_write_keeps_the_cells_slot() {
    let path = std::env::temp_dir().join(format!("issue435_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    let mut eng = Engine::create_growable_with_cell_version(path.to_str().unwrap(), 1000).unwrap();
    eng.define_himo("body", ValueType::Leaf, 0);
    let hid = eng.himo_id("body").unwrap() as u16;
    let e = eng.entity().unwrap();
    eng.flush().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 4 << 20).unwrap();
    eng.tie_text_to(e, "body", "v0");
    let h0 = eng.cell_hlc(e, hid);
    assert!(h0.wall > 0, "cell の版数が無い (前提が崩れた)");
    // v0 より新しい = 最初の判定は通る版数で届く
    let remote = enchudb_oplog::Hlc { wall: h0.wall + 1, logical: 0, peer: 2 };

    let row = eng.write_row(e);
    let receiver = {
        let eng = eng.clone();
        std::thread::spawn(move || eng.remote_tieleaf_apply(e, hid, b"remote-old", remote))
    };
    std::thread::sleep(std::time::Duration::from_millis(100)); // 受信は行の lock で待つ
    eng.tie_text_to(e, "body", "local-new"); // 受信より新しい版数 (受信が clock を進めている)
    drop(row);
    let applied = receiver.join().unwrap();
    assert!(matches!(applied, RemoteApply::Stale), "負けた受信が {applied:?} を返した");

    // 書き出して空きに戻し、 同じ長さの値で使い回させる
    eng.body_msync().unwrap();
    for _ in 0..20 {
        let x = eng.entity().unwrap();
        eng.tie_text_to(x, "body", "XXXXXXXXX");
    }
    assert_eq!(eng.get_text_owned(e, "body").as_deref(), Some(&b"local-new"[..]), "cell の slot が使い回された");
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}
