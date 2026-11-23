//! #1047 / #1046 の (2): 開く時の突き合わせが完了させた削除 (下限の版数の tombstone) は、 下限が配った版数以下なので record を載せず
//! floor を上げる。 ホストを挟む形で、 その floor がホスト経由で下の client (C) に届き、 C が A の state で取り直して X を消す。 A が開いた
//! 後に同じ名前で足し直した行 X2 は C で残る (#923 の形が起きない)。
#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_transport::host::{HttpHost, HttpHostTransport};

const HOST: PeerId = 100;
const WAL: usize = 16 << 20;

struct Dir(std::path::PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("issue1047-host-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Dir(dir)
    }
    fn db(&self, id: &str) -> String {
        self.0.join(id).to_str().unwrap().to_string()
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let dest = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &dest);
        } else {
            std::fs::copy(entry.path(), dest).unwrap();
        }
    }
}

/// 名前 `k` と `v` を持つ表 `notes`、 v15 に移るための枠 1 の表 `u` (移り終えた形で返す)。
fn db(path: &str, peer: PeerId) -> Arc<Engine> {
    {
        let mut e = Engine::create_with_cell_version(path, 65_536).unwrap();
        e.define_table("u", 1).unwrap();
        e.define_table("notes", 4096).unwrap();
        e.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
        let k = e.himo_id("notes.k").unwrap() as u16;
        e.set_table_pk("notes", k).unwrap();
        e.enable_sync_tables().unwrap();
        e.flush().unwrap();
    }
    let e = Engine::open_concurrent_with_oplog(path, WAL).unwrap();
    e.set_peer_id(peer);
    if peer != HOST {
        let u = e.entity_in("u").unwrap();
        e.delete(u);
        let u2 = e.entity_in("u").unwrap();
        assert_ne!(u2, u, "前提: v15");
    }
    e
}

struct Client {
    eng: Arc<Engine>,
    s: Syncer,
}

fn client(eng: Arc<Engine>, url: &str) -> Client {
    let t = Arc::new(HttpHostTransport::connect(url.to_string(), eng.peer_id()).unwrap());
    let s = Syncer::new(eng.clone(), t);
    s.serve_state();
    Client { eng, s }
}

impl Client {
    fn write(&self, k: u32, v: u32) -> u64 {
        let e = self.eng.entity_in("notes").unwrap();
        self.eng.tie_to(e, "notes.k", k);
        self.eng.tie_to(e, "notes.v", v);
        self.eng.oplog_commit();
        std::thread::sleep(Duration::from_millis(3));
        e
    }
    /// 名前 `k` の生きた行の `v`。
    fn live(&self, k: u32) -> Vec<Option<u64>> {
        self.eng.pull_raw("notes.k", k).into_iter().filter(|&e| self.eng.is_live(e)).map(|e| self.eng.get(e, "notes.v")).collect()
    }
    fn push(&self) {
        self.eng.oplog_sync().unwrap();
        self.s.publish_since(Hlc::ZERO);
    }
    /// 受ける。 抜けを言われたら、 言われた書き手を取り直して受け直す。 戻り値: 取り直した書き手。
    fn pull(&self) -> Vec<PeerId> {
        let mut booted = Vec::new();
        for _ in 0..4 {
            let out = self.s.pull_once(HOST);
            if !out.history_truncated {
                break;
            }
            for a in out.truncated_authors {
                assert!(self.s.bootstrap_pull_via(HOST, a).is_some(), "書き手 {a} の state を取れない");
                booted.push(a);
            }
        }
        booted
    }
    fn sync(&self) -> Vec<PeerId> {
        self.push();
        self.pull()
    }
}

/// falsify: 突き合わせの 2. の完了に tombstone を置かない (前の形) と、 A は floor を上げず、 C は X を持ち続ける (X の世界番号が C で生きた行に
/// 当たる)。
#[test]
fn a_delete_finished_at_open_reaches_a_client_through_the_host() {
    let dir = Dir::new("lb");
    let host_eng = db(&dir.db("host"), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let c = client(db(&dir.db("c"), 3), &url);
    let pa = dir.db("a");
    let (before, after, img) = (dir.db("before"), dir.db("after"), dir.db("img"));
    let x = {
        let a = client(db(&pa, 1), &url);
        let x = a.write(5, 1);
        a.write(1000, 1); // X の後ろに生きた行 (払い出し位置が開く時に X の手前へ戻らない)
        for _ in 0..2 {
            a.sync();
            c.sync();
        }
        assert_eq!(c.live(5), vec![Some(1)], "前提: C に X がある");
        // 削除の前の写しに、 削除の後の生きている印だけを置く (突き合わせの 2.)
        let _hold = a.eng.hold_consumer();
        a.eng.oplog_sync().unwrap();
        copy_dir(Path::new(&pa), Path::new(&before));
        a.eng.delete(x);
        copy_dir(Path::new(&pa), Path::new(&after));
        x
    };
    copy_dir(Path::new(&before), Path::new(&img));
    std::fs::copy(Path::new(&after).join("entities.seg"), Path::new(&img).join("entities.seg")).unwrap();
    let eng_a = Engine::open_concurrent_with_oplog(&img, WAL).unwrap();
    eng_a.set_peer_id(1);
    assert_eq!(eng_a.generation_repairs(), (0, 1), "前提: 突き合わせの 2. が X の場所を完了させた");
    let a = client(eng_a, &url);
    // A が同じ名前 5 で足し直す
    a.write(5, 9);
    let mut booted = Vec::new();
    for _ in 0..4 {
        a.sync();
        booted.extend(c.sync());
    }
    let world_x = enchudb_oplog::make_eid(1, x as u32);
    let x_on_c_live = c.eng.resolve_remote_eid_existing(world_x).is_some_and(|e| c.eng.is_live_unchecked(e));
    let named = c.live(5);
    drop((a, c, host));
    assert!(!x_on_c_live, "A が開いた時に完了させた X の削除が C に届かない (取り直した書き手 {booted:?})");
    assert_eq!(named, vec![Some(9)], "C で、 A が足し直した名前 5 の行が無い / X が残った");
    assert!(booted.contains(&1), "C は A の floor で抜けを言われ、 A の state で取り直す (取り直した書き手 {booted:?})");
}
