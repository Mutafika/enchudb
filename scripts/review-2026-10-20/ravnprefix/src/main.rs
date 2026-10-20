use enchudb_engine::engine::Engine;
use enchudb_engine::himo_store::ValueType;
use enchudb_engine::ravn::Ravn;
use enchudb_oplog::{eid_local, eid_peer, make_eid};
use std::sync::Arc;

fn main() {
    let dir = std::env::temp_dir().join(format!("ravn-prefix-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut eng = Engine::create_standalone(dir.to_str().unwrap()).unwrap();
    eng.set_peer_id(7);
    eng.define_himo("friend", ValueType::Ref, 0);
    let a = eng.entity().unwrap();
    let b = eng.entity().unwrap();
    eng.tie_ref(a, "friend", b);
    println!("peer(a)={} peer(b)={} a={a:#x} b={b:#x}", eid_peer(a), eid_peer(b));
    // engine-level: the Ref cell holds the local id
    println!("get(a, friend) = {:?} (local(b)={})", eng.get(a, "friend"), eid_local(b));
    let eng = Arc::new(eng);
    let r = Ravn::new(eng.clone());
    let fwd = r.follow(&[a], &["friend"]);
    let back = r.reverse_follow(&[b], "friend");
    println!("follow(a)         = {fwd:#x?}  == [b]? {}", fwd == vec![b]);
    println!("reverse_follow(b) = {back:#x?}  == [a]? {}", back == vec![a]);
    println!("pull_raw(friend, local(b)) = {:#x?}", eng.pull_raw("friend", eid_local(b)));
    // bfs: visited is seeded with prefixed start, follow yields unprefixed -> can a cycle revisit the start?
    let c = eng.entity().unwrap();
    // a -> b -> c -> a  (cycle). tie_ref needs &mut; we moved into Arc, so use tie_ref_to (himo defined)
    eng.tie_ref_to(b, "friend", c);
    eng.tie_ref_to(c, "friend", a);
    let bfs = r.bfs(&[a], "friend", 6);
    println!("bfs(a, depth 6) = {bfs:#x?}");
    println!("expected depths: 1:[b] 2:[c] then stop (a already visited). got {} levels", bfs.len());
    let _ = make_eid(0, 0);
    let _ = std::fs::remove_dir_all(&dir);
}
