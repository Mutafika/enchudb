//! ホスト: 全 client が経由する、 DB を持つ peer の HTTP の口 (`HttpHost`) と、 それと話す client 側の transport
//! (`HttpHostTransport`)。
//!
//! sync の既定はホストを置く形 (2026-10-23)。 ホストは自分の `Engine` に client の record を当て (`Syncer` の pull と同じ道)、
//! 中継として自分の `_sync_ops` に載せ、 client はホストの `_sync_ops` から pull する。 共有の掲示板 (`HttpRelay`) と違い、
//! record の置き場はホストの DB だけで、 再起動しても続く。
//!
//! - `POST /v2/publish?peer=c` : client c の record。 ホストが当てて中継し、 WAL を書き出してから、 ホストが当てた位置 (c への ack)
//!   を返す。 client の `_sync_ops` はこの ack で回収される (ホストは record の置き場の権威なので、 #227 の下流の消化で丸めない)
//! - `POST /v2/pull?to=c` (body = 書き手ごとの cursor) : ホストの `_sync_ops` のうち cursor より後 (c 自身の record は除く) と、
//!   ホストの floor。 floor は record を読んだ後に読む (間に回収が入っても穴を見逃さない側)。 cursor は ack も兼ねる
//! - `POST /v2/ack?peer=c` (body = cursor) : c がホストの流れを当てた位置。 ホストの回収を回す
//! - `POST /v2/floor?peer=c` (body = floor) : c の floor (c が WAL に載らずに落とした record など)。 ホストの c の cursor が届かなければ、
//!   次の publish の返事で c の state を求める → c は `POST /v2/state?author=c` で自分の state を送る
//! - `GET /v2/floors?to=c` : c に配る floor (c がまだ pull していない時の、 pull の前の判定に)
//! - `GET /v2/state?author=a` : ホストが持つ a の state (抜けた client の取り直し)
//! - 古い client (`GET /pull?from&wall..` / `POST /publish?peer=`) もそのまま受ける (ack が届かないので、 その client の分は回収されない)
//!
//! 計画は repo 直下の `HOST_PLAN.md`。

use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use enchudb::Engine;
use enchudb::sync::Syncer;
use enchudb::transport::{StateBatch, StateProvider, Transport, WireRecord, decode_batch, encode_batch};
use enchudb_oplog::{Hlc, PeerId};

use crate::http::{HttpTransport, parse_query, read_request, send_response};

// ─────────────────────────────────────────────────────────────
// 中身の形
// ─────────────────────────────────────────────────────────────

/// 書き手ごとの位置 (cursor / ack / floor) の列: `[数 u32]([peer u32][wall u64][logical u32][hlc peer u32])*`。
fn encode_cursors(list: &[(PeerId, Hlc)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + list.len() * 20);
    out.extend_from_slice(&(list.len() as u32).to_le_bytes());
    for (p, h) in list {
        out.extend_from_slice(&p.to_le_bytes());
        put_hlc(&mut out, *h);
    }
    out
}

fn decode_cursors(buf: &[u8]) -> Option<Vec<(PeerId, Hlc)>> {
    let n = u32::from_le_bytes(buf.get(..4)?.try_into().ok()?) as usize;
    let body = buf.get(4..)?;
    if body.len() != n.checked_mul(20)? {
        return None;
    }
    let (chunks, _) = body.as_chunks::<20>();
    Some(chunks.iter().map(|c| (u32::from_le_bytes(c[..4].try_into().unwrap()), get_hlc(&c[4..]))).collect())
}

fn put_hlc(out: &mut Vec<u8>, h: Hlc) {
    out.extend_from_slice(&h.wall.to_le_bytes());
    out.extend_from_slice(&h.logical.to_le_bytes());
    out.extend_from_slice(&h.peer.to_le_bytes());
}

/// 16 byte から HLC (呼び手が長さを確かめた)。
fn get_hlc(b: &[u8]) -> Hlc {
    Hlc {
        wall: u64::from_le_bytes(b[..8].try_into().unwrap()),
        logical: u32::from_le_bytes(b[8..12].try_into().unwrap()),
        peer: u32::from_le_bytes(b[12..16].try_into().unwrap()),
    }
}

/// record の列を長さ付きで: `[長さ u32][encode_batch]`。 後ろに別の中身を続けられる。
fn encode_framed(records: &[WireRecord]) -> Vec<u8> {
    let batch = encode_batch(records);
    let mut out = Vec::with_capacity(4 + batch.len());
    out.extend_from_slice(&(batch.len() as u32).to_le_bytes());
    out.extend_from_slice(&batch);
    out
}

/// [`encode_framed`] を読み、 record と残りを返す。
fn decode_framed(buf: &[u8]) -> Option<(Vec<WireRecord>, &[u8])> {
    let n = u32::from_le_bytes(buf.get(..4)?.try_into().ok()?) as usize;
    let batch = buf.get(4..4usize.checked_add(n)?)?;
    Some((decode_batch(batch).ok()?, &buf[4 + n..]))
}

/// state: `[framed records][as_of 16 byte][complete u8]`。
fn encode_state(s: &StateBatch) -> Vec<u8> {
    let mut out = encode_framed(&s.records);
    put_hlc(&mut out, s.as_of);
    out.push(s.complete as u8);
    out
}

fn decode_state(buf: &[u8]) -> Option<StateBatch> {
    let (records, rest) = decode_framed(buf)?;
    if rest.len() != 17 {
        return None;
    }
    Some(StateBatch { records, as_of: get_hlc(rest), complete: rest[16] != 0 })
}

/// publish の返事の印: ホストが c の state を求めている (c の floor にホストの cursor が届かない)。
const NEED_STATE: u8 = 1;

/// 書き手ごとの位置の列を、 書き手ごとに大きい方へ足す。
fn merge_max(into: &mut Vec<(PeerId, Hlc)>, add: &[(PeerId, Hlc)]) {
    for (p, h) in add {
        match into.iter_mut().find(|(q, _)| q == p) {
            Some((_, cur)) if *cur < *h => *cur = *h,
            Some(_) => {}
            None => into.push((*p, *h)),
        }
    }
}

// ─────────────────────────────────────────────────────────────
// ホスト
// ─────────────────────────────────────────────────────────────

/// ホストの `Syncer` の transport。 ホストは自分から pull しない: client が送った record を `pull_as_multi` で渡し、 `Syncer` が
/// 返す ack / floor / state の配布元をここに受ける。 HTTP の口がそれを読む。
#[derive(Default)]
struct Loopback {
    host: PeerId,
    /// client → 送られてきた record (その client の `pull_once` が取る)。
    inbox: Mutex<HashMap<PeerId, Vec<WireRecord>>>,
    /// ホストの `Syncer` が c の流れを当てた後の ack (`record_pull_ack_multi(c, host, ..)`)。 publish の返事で c に返す。
    acks_for_clients: Mutex<HashMap<PeerId, Vec<(PeerId, Hlc)>>>,
    /// client c がホストの流れを当てた位置 (`POST /v2/ack`)。 ホストの `absorb_pull_acks` が取る。
    acks_from_clients: Mutex<HashMap<PeerId, Vec<(PeerId, Hlc)>>>,
    /// client c の floor (`POST /v2/floor`)。 ホストの c の pull の前の判定が読む。
    client_floors: Mutex<HashMap<PeerId, Vec<(PeerId, Hlc)>>>,
    /// ホストが配れる state (書き手 → 配布元、 `serve_state` が登録する)。
    providers: Mutex<HashMap<PeerId, StateProvider>>,
    /// client が送った自分の state (ホストの `bootstrap_pull_via(c, c)` が取る)。
    pushed_state: Mutex<HashMap<PeerId, StateBatch>>,
}

impl Transport for Loopback {
    fn pull(&self, from: PeerId, _since: Hlc) -> Vec<WireRecord> {
        self.inbox.lock().unwrap().remove(&from).unwrap_or_default()
    }

    /// ホストの record は ring から直に配る (`POST /v2/pull`)。
    fn publish(&self, _peer: PeerId, _records: Vec<WireRecord>) {}

    fn pull_as_multi(&self, _to: PeerId, from: PeerId, _since: &[(PeerId, Hlc)]) -> Vec<WireRecord> {
        self.inbox.lock().unwrap().remove(&from).unwrap_or_default()
    }

    fn history_floor_multi(&self, peer: PeerId) -> Option<Vec<(PeerId, Hlc)>> {
        self.client_floors.lock().unwrap().get(&peer).cloned()
    }

    fn record_pull_ack_multi(&self, author: PeerId, by: PeerId, cursors: &[(PeerId, Hlc)]) {
        if by == self.host {
            self.acks_for_clients.lock().unwrap().insert(author, cursors.to_vec());
        }
    }

    fn take_pull_acks_multi(&self, author: PeerId) -> Vec<(PeerId, Vec<(PeerId, Hlc)>)> {
        if author != self.host {
            return Vec::new();
        }
        self.acks_from_clients.lock().unwrap().drain().collect()
    }

    fn register_state_provider(&self, peer: PeerId, provider: StateProvider) {
        self.providers.lock().unwrap().insert(peer, provider);
    }

    fn register_state_provider_for(&self, author: PeerId, by: PeerId, provider: StateProvider) {
        if by == self.host {
            self.providers.lock().unwrap().insert(author, provider);
        }
    }

    fn fetch_state(&self, author: PeerId) -> Option<StateBatch> {
        self.pushed_state.lock().unwrap().remove(&author)
    }
}

struct HostInner {
    engine: Arc<Engine>,
    syncer: Syncer,
    loopback: Arc<Loopback>,
    /// client ごとの publish を 1 本ずつに並べる (同じ client の record を当てる順を保つ)。
    client_locks: Mutex<HashMap<PeerId, Arc<Mutex<()>>>>,
}

impl HostInner {
    fn client_lock(&self, c: PeerId) -> Arc<Mutex<()>> {
        self.client_locks.lock().unwrap().entry(c).or_default().clone()
    }

    /// client `from` の record を当てる。 戻り値は (c の state が要るか, c への ack)。
    fn accept(&self, from: PeerId, records: Vec<WireRecord>) -> (bool, Vec<(PeerId, Hlc)>) {
        let lock = self.client_lock(from);
        let _serial = lock.lock().unwrap();
        self.loopback.inbox.lock().unwrap().insert(from, records);
        let out = self.syncer.pull_once(from);
        self.loopback.inbox.lock().unwrap().remove(&from);
        // 中継として WAL に載せた record を書き出してから ack を返す (ack で client は自分の ring から消すので、 その後にホストが
        // 落ちて中継の record を失うと、 他の client に届く道が無くなる)
        if out.applied > 0 {
            let _ = self.engine.oplog_sync();
        }
        let ack = self
            .loopback
            .acks_for_clients
            .lock()
            .unwrap()
            .remove(&from)
            .unwrap_or_else(|| self.syncer.cursors_of(from));
        (out.history_truncated, ack)
    }

    /// client `to` に配る record: ホストの `_sync_ops` のうち、 書き手ごとの cursor より後 (to 自身の record は除く)。
    fn records_for(&self, to: PeerId, since: &[(PeerId, Hlc)]) -> Vec<WireRecord> {
        let cursor = |a: PeerId| since.iter().find(|(p, _)| *p == a).map_or(Hlc::ZERO, |(_, h)| *h);
        // ring の順 (lsn = ホストが当てた順) のまま渡す。 書き手ごとには HLC の昇順 (client は自分の record を HLC の順に送り、
        // ホストはその順に当てる)。 当てる側は届く順に依らない作り。
        // ring を毎回全部読む (100 万行で重い。 書き手ごとの位置を覚えて途中から読むのは後で)
        self.engine
            .pending_sync_ops(0)
            .iter()
            .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
            .map(WireRecord::from)
            .filter(|r| r.author_peer != to && r.hlc > cursor(r.author_peer))
            .collect()
    }

    /// client `to` に配る floor (書き手ごと、 `(u32::MAX, h)` は書き手を問わない下限)。 `to` 自身の書き手の分は除く (`to` の
    /// record は `to` に配らないので、 `to` の cursor はそこで進まない)。
    fn floors_for(&self, to: PeerId) -> Vec<(PeerId, Hlc)> {
        let mut f = match self.engine.sync_reclaimed_floors() {
            Some(f) if !f.is_empty() => f,
            _ => self.engine.sync_reclaimed_floor().map(|f| vec![(u32::MAX, f)]).unwrap_or_default(),
        };
        f.retain(|(a, _)| *a != to);
        f
    }

    /// client `by` の ack (ホストの流れを当てた位置) を受ける。 `by` 自身の record は `by` に配らないので、 その分は 「ホストが
    /// `by` から受けた所まで `by` は持っている」 を足す (足さないと `by` の record の行がホストの ring から回収されない)。
    fn ack_from(&self, by: PeerId, mut cursors: Vec<(PeerId, Hlc)>) {
        if let Some(own) = self.syncer.cursors_of(by).into_iter().find(|(a, _)| *a == by) {
            merge_max(&mut cursors, &[own]);
        }
        merge_max(self.loopback.acks_from_clients.lock().unwrap().entry(by).or_default(), &cursors);
        self.syncer.reclaim_beat();
    }
}

/// ホストの HTTP の口。 バックグラウンドの thread で listen し、 drop で止める。
pub struct HttpHost {
    inner: Arc<HostInner>,
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    addr: SocketAddr,
}

impl HttpHost {
    /// `engine` (sync の表があり、 peer id が 0 でない、 WAL 付きの DB) をホストにして `bind_addr` で listen する。 ホストは中継の
    /// 印 (`set_gossip_remote_apply`) を立てる。 `cursor_path` は client ごとの受けた位置の置き場 (無ければ memory だけで、
    /// 開き直すと client の record を当て直す。 当て直しは冪等)。
    pub fn start(bind_addr: &str, engine: Arc<Engine>, cursor_path: Option<PathBuf>) -> io::Result<Self> {
        let host = engine.peer_id();
        if host == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "the host needs a non-zero peer id (Engine::set_peer_id)"));
        }
        engine.set_gossip_remote_apply(true);
        let loopback = Arc::new(Loopback { host, ..Loopback::default() });
        let mut syncer = Syncer::try_new(engine.clone(), loopback.clone())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        if let Some(p) = cursor_path {
            syncer = syncer.with_cursor_path(p);
        }
        syncer.set_host(true);
        syncer.serve_state();
        let inner = Arc::new(HostInner { engine, syncer, loopback, client_locks: Mutex::new(HashMap::new()) });

        let listener = TcpListener::bind(bind_addr)?;
        let addr = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let (shutdown_bg, inner_bg) = (shutdown.clone(), inner.clone());
        let handle = std::thread::spawn(move || {
            while !shutdown_bg.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let inner = inner_bg.clone();
                        std::thread::spawn(move || {
                            let _ = handle_host_connection(stream, &inner);
                        });
                    }
                    Err(_) => std::thread::sleep(Duration::from_millis(5)),
                }
            }
        });
        Ok(Self { inner, shutdown, handle: Some(handle), addr })
    }

    /// listen している addr。
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// ホストの peer id。
    pub fn peer_id(&self) -> PeerId {
        self.inner.engine.peer_id()
    }

    /// ホストの `Syncer` (観測・試験用)。
    pub fn syncer(&self) -> &Syncer {
        &self.inner.syncer
    }
}

impl Drop for HttpHost {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn handle_host_connection(mut stream: TcpStream, host: &HostInner) -> io::Result<()> {
    let Some(req) = read_request(&mut stream)? else { return Ok(()) };
    let q = parse_query(&req.query);
    let peer_param = |k: &str| q.get(k).and_then(|s| s.parse::<PeerId>().ok());
    match (req.method.as_str(), req.route.as_str()) {
        ("GET", "/v2/hello") => send_response(&mut stream, 200, &host.engine.peer_id().to_le_bytes()),
        ("POST", "/v2/publish") | ("POST", "/publish") => {
            let Some(from) = peer_param("peer") else { return send_response(&mut stream, 400, b"peer") };
            let Ok(records) = decode_batch(&req.body) else { return send_response(&mut stream, 400, b"batch") };
            let (need_state, ack) = host.accept(from, records);
            if req.route == "/publish" {
                return send_response(&mut stream, 200, b"");
            }
            let mut out = vec![if need_state { NEED_STATE } else { 0 }];
            out.extend_from_slice(&encode_cursors(&ack));
            send_response(&mut stream, 200, &out)
        }
        ("POST", "/v2/pull") => {
            let Some(to) = peer_param("to") else { return send_response(&mut stream, 400, b"to") };
            let Some(since) = decode_cursors(&req.body) else { return send_response(&mut stream, 400, b"cursors") };
            // record を読んでから floor を読む (間に回収が入ると floor が上がる側 = 穴を見逃さない)
            let records = host.records_for(to, &since);
            let mut out = encode_framed(&records);
            out.extend_from_slice(&encode_cursors(&host.floors_for(to)));
            // pull の cursor は ack も兼ねる (`Syncer::pull_once` は当てて書き出した後にしか cursor を進めない、 #149)。 自分の record
            // しか無い client は何も受けないので `record_pull_ack_multi` を呼ばない — ここで受けないと、 その client の record の行が
            // ホストの ring から回収されない
            host.ack_from(to, since);
            send_response(&mut stream, 200, &out)
        }
        ("GET", "/pull") => {
            // 古い client: scalar の cursor。 ホストの ring の全部の書き手から
            let since = Hlc {
                wall: q.get("wall").and_then(|s| s.parse().ok()).unwrap_or(0),
                logical: q.get("logical").and_then(|s| s.parse().ok()).unwrap_or(0),
                peer: q.get("peer").and_then(|s| s.parse().ok()).unwrap_or(0),
            };
            let records: Vec<WireRecord> =
                host.records_for(0, &[]).into_iter().filter(|r| r.hlc > since).collect();
            send_response(&mut stream, 200, &encode_batch(&records))
        }
        ("POST", "/v2/ack") => {
            let Some(by) = peer_param("peer") else { return send_response(&mut stream, 400, b"peer") };
            let Some(cursors) = decode_cursors(&req.body) else { return send_response(&mut stream, 400, b"cursors") };
            host.ack_from(by, cursors);
            send_response(&mut stream, 200, b"")
        }
        ("POST", "/v2/floor") => {
            let Some(by) = peer_param("peer") else { return send_response(&mut stream, 400, b"peer") };
            let Some(floors) = decode_cursors(&req.body) else { return send_response(&mut stream, 400, b"floors") };
            merge_max(host.loopback.client_floors.lock().unwrap().entry(by).or_default(), &floors);
            send_response(&mut stream, 200, b"")
        }
        ("GET", "/v2/floors") => {
            let Some(to) = peer_param("to") else { return send_response(&mut stream, 400, b"to") };
            send_response(&mut stream, 200, &encode_cursors(&host.floors_for(to)))
        }
        ("GET", "/v2/state") => {
            let Some(author) = peer_param("author") else { return send_response(&mut stream, 400, b"author") };
            let provider = host.loopback.providers.lock().unwrap().get(&author).cloned();
            match provider.and_then(|p| p()) {
                Some(state) => send_response(&mut stream, 200, &encode_state(&state)),
                None => send_response(&mut stream, 404, b"no state"),
            }
        }
        ("POST", "/v2/state") => {
            let Some(author) = peer_param("author") else { return send_response(&mut stream, 400, b"author") };
            let Some(state) = decode_state(&req.body) else { return send_response(&mut stream, 400, b"state") };
            let lock = host.client_lock(author);
            let _serial = lock.lock().unwrap();
            host.loopback.pushed_state.lock().unwrap().insert(author, state);
            let ok = host.syncer.bootstrap_pull_via(author, author).is_some();
            host.loopback.pushed_state.lock().unwrap().remove(&author);
            let _ = host.engine.oplog_sync();
            send_response(&mut stream, if ok { 200 } else { 500 }, b"")
        }
        _ => send_response(&mut stream, 404, b"not found"),
    }
}

// ─────────────────────────────────────────────────────────────
// client
// ─────────────────────────────────────────────────────────────

/// ホストと話す client の transport。 `Syncer::new(engine, Arc::new(HttpHostTransport::connect(url, me)?))` で使い、
/// `publish_since` / `pull_once(t.host_id())` を回す。 自分の state をホストに送れるよう、 client も `serve_state()` を呼ぶこと
/// (ホストが求めた時だけ送る)。
pub struct HttpHostTransport {
    http: HttpTransport,
    host: PeerId,
    /// この client の peer id。
    me: PeerId,
    /// ホストが当てた位置 (publish の返事)。 `take_pull_acks_multi` で `Syncer` に渡す。
    pending_acks: Mutex<HashMap<PeerId, Vec<(PeerId, Hlc)>>>,
    /// 書き手ごとにホストが当てた位置 (送り済みの record を送り直さない)。
    acked: Mutex<Vec<(PeerId, Hlc)>>,
    /// 最後の pull の返事にあったホストの floor。
    floors: Mutex<Option<Vec<(PeerId, Hlc)>>>,
    /// 自分の state の配布元 (`serve_state` が登録する)。 ホストが求めた時に送る。
    own_state: Mutex<HashMap<PeerId, StateProvider>>,
}

impl HttpHostTransport {
    /// `base_url` = "http://host:port"、 `me` = この client の peer id。 ホストの peer id を聞いてから返す。
    pub fn connect(base_url: String, me: PeerId) -> io::Result<Self> {
        let http = HttpTransport::new(base_url);
        let (status, body) = http.request("GET", "/v2/hello", &[])?;
        let host = match (status, body.get(..4)) {
            (200, Some(b)) => u32::from_le_bytes(b.try_into().unwrap()),
            _ => return Err(io::Error::other(format!("not an enchudb host (GET /v2/hello: {status})"))),
        };
        Ok(Self {
            http,
            host,
            me,
            pending_acks: Mutex::new(HashMap::new()),
            acked: Mutex::new(Vec::new()),
            floors: Mutex::new(None),
            own_state: Mutex::new(HashMap::new()),
        })
    }

    /// ホストの peer id (`pull_once` に渡す)。
    pub fn host_id(&self) -> PeerId {
        self.host
    }

    /// ホストが求めたので、 自分 (`author`) の state を送る。
    fn push_state(&self, author: PeerId) {
        let provider = self.own_state.lock().unwrap().get(&author).cloned();
        let Some(state) = provider.and_then(|p| p()) else {
            eprintln!(
                "[enchudb] host asked for the state of peer {author}, but no state provider is registered — call \
                 Syncer::serve_state() on the client"
            );
            return;
        };
        let _ = self.http.request("POST", &format!("/v2/state?author={author}"), &encode_state(&state));
    }
}

impl Transport for HttpHostTransport {
    fn pull(&self, from: PeerId, since: Hlc) -> Vec<WireRecord> {
        self.pull_as_multi(0, from, &[(from, since)])
    }

    fn publish(&self, peer: PeerId, records: Vec<WireRecord>) {
        self.publish_to(peer, self.host, records);
    }

    fn publish_to(&self, from: PeerId, _to: PeerId, records: Vec<WireRecord>) {
        let records: Vec<WireRecord> = {
            let acked = self.acked.lock().unwrap();
            let done = |a: PeerId| acked.iter().find(|(p, _)| *p == a).map_or(Hlc::ZERO, |(_, h)| *h);
            records.into_iter().filter(|r| r.hlc > done(r.author_peer)).collect()
        };
        if records.is_empty() {
            return;
        }
        let Ok((200, body)) = self.http.request("POST", &format!("/v2/publish?peer={from}"), &encode_batch(&records))
        else {
            return;
        };
        let (Some(&flags), Some(ack)) = (body.first(), body.get(1..).and_then(decode_cursors)) else { return };
        merge_max(&mut self.acked.lock().unwrap(), &ack);
        merge_max(self.pending_acks.lock().unwrap().entry(from).or_default(), &ack);
        if flags & NEED_STATE != 0 {
            self.push_state(from);
        }
    }

    fn known_peers(&self) -> Vec<PeerId> {
        vec![self.host]
    }

    fn pull_as_multi(&self, to: PeerId, from: PeerId, since: &[(PeerId, Hlc)]) -> Vec<WireRecord> {
        if from != self.host {
            return Vec::new();
        }
        let Ok((200, body)) = self.http.request("POST", &format!("/v2/pull?to={to}"), &encode_cursors(since)) else {
            return Vec::new();
        };
        let Some((records, rest)) = decode_framed(&body) else { return Vec::new() };
        let Some(floors) = decode_cursors(rest) else { return Vec::new() };
        // 返事の floor は record を読んだ後の物。 cursor が floor に届かない書き手は、 この返事の record も途中から始まりうる
        // (間に回収が入った) ので渡さない — cursor は進まず、 次の `pull_once` の前の判定が floor で抜けを知らせる
        let baseline = floors.iter().find(|(a, _)| *a == u32::MAX).map_or(Hlc::ZERO, |(_, h)| *h);
        let short = |a: PeerId| {
            let cur = since.iter().find(|(p, _)| *p == a).map_or(Hlc::ZERO, |(_, h)| *h);
            let f = floors.iter().find(|(p, _)| *p == a).map_or(Hlc::ZERO, |(_, h)| *h);
            cur < f.max(baseline)
        };
        let records = records.into_iter().filter(|r| !short(r.author_peer)).collect();
        *self.floors.lock().unwrap() = Some(floors);
        records
    }

    fn history_floor_multi(&self, peer: PeerId) -> Option<Vec<(PeerId, Hlc)>> {
        if peer != self.host {
            return None;
        }
        if let Some(f) = self.floors.lock().unwrap().clone() {
            return Some(f);
        }
        // まだ pull していない: 最初の pull の前の判定が抜けを知るよう、 ホストに聞く (以後は pull の返事の floor)
        match self.http.request("GET", &format!("/v2/floors?to={}", self.me), &[]) {
            Ok((200, body)) => decode_cursors(&body),
            _ => None,
        }
    }

    fn set_history_floor(&self, peer: PeerId, floor: Hlc) {
        self.set_history_floor_multi(peer, &[(u32::MAX, floor)]);
    }

    fn set_history_floor_multi(&self, peer: PeerId, floors: &[(PeerId, Hlc)]) {
        let _ = self.http.request("POST", &format!("/v2/floor?peer={peer}"), &encode_cursors(floors));
    }

    fn record_pull_ack_multi(&self, author: PeerId, by: PeerId, cursors: &[(PeerId, Hlc)]) {
        if author == self.host {
            let _ = self.http.request("POST", &format!("/v2/ack?peer={by}"), &encode_cursors(cursors));
        }
    }

    fn take_pull_acks_multi(&self, author: PeerId) -> Vec<(PeerId, Vec<(PeerId, Hlc)>)> {
        match self.pending_acks.lock().unwrap().remove(&author) {
            Some(cursors) => vec![(self.host, cursors)],
            None => Vec::new(),
        }
    }

    fn register_state_provider(&self, peer: PeerId, provider: StateProvider) {
        self.own_state.lock().unwrap().insert(peer, provider);
    }

    fn register_state_provider_for(&self, author: PeerId, by: PeerId, provider: StateProvider) {
        if author == by {
            self.own_state.lock().unwrap().insert(author, provider);
        }
    }

    fn fetch_state(&self, author: PeerId) -> Option<StateBatch> {
        match self.http.request("GET", &format!("/v2/state?author={author}"), &[]) {
            Ok((200, body)) => decode_state(&body),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use enchudb_oplog::oplog::DecodedOp;

    /// 決めた返事を返す偽のホスト (`/v2/hello` と `/v2/pull`)。
    fn fake_host(pull_answer: Vec<u8>) -> (SocketAddr, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let h = std::thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                let req = read_request(&mut stream).unwrap().unwrap();
                let body = if req.route == "/v2/hello" { 7u32.to_le_bytes().to_vec() } else { pull_answer.clone() };
                send_response(&mut stream, 200, &body).unwrap();
            }
        });
        (addr, h)
    }

    /// pull の返事の floor に cursor が届かない書き手の record は渡さない (cursor を進めると抜けを越える)。 floor は覚え、 次の
    /// `pull_once` の前の判定が抜けを知らせる。 pull の前の判定の後にホストが回収した時の形。
    #[test]
    fn a_pull_answer_below_the_floor_is_held_back() {
        let h = |w| Hlc { wall: w, logical: 0, peer: 5 };
        let rec = |author: PeerId, w| {
            WireRecord::unsigned(Hlc { peer: author, ..h(w) }, author, DecodedOp::Tie { eid: 1, himo_id: 0, value: w })
        };
        // 書き手 5 は h(9) まで回収済み (cursor h(3) から h(4)..h(8) が抜けた)、 書き手 6 は回収なし
        let mut answer = encode_framed(&[rec(5, 9), rec(5, 10), rec(6, 4)]);
        answer.extend_from_slice(&encode_cursors(&[(5, h(9))]));
        let (addr, server) = fake_host(answer);
        let t = HttpHostTransport::connect(format!("http://{addr}"), 1).unwrap();
        let got = t.pull_as_multi(1, 7, &[(5, h(3)), (6, h(3))]);
        server.join().unwrap();
        assert_eq!(got.iter().map(|r| r.author_peer).collect::<Vec<_>>(), vec![6], "floor に届かない書き手の record を渡した");
        assert_eq!(t.history_floor_multi(7), Some(vec![(5, h(9))]), "floor を覚えない");
    }

    #[test]
    fn cursors_and_state_round_trip() {
        let h = |w| Hlc { wall: w, logical: 3, peer: 9 };
        let list = vec![(1, h(5)), (u32::MAX, h(7))];
        assert_eq!(decode_cursors(&encode_cursors(&list)), Some(list.clone()));
        assert_eq!(decode_cursors(&encode_cursors(&[])), Some(vec![]));
        assert_eq!(decode_cursors(&encode_cursors(&list)[..30]), None, "切れた列");
        let s = StateBatch { records: vec![], as_of: h(11), complete: true };
        let back = decode_state(&encode_state(&s)).unwrap();
        assert_eq!((back.as_of, back.complete, back.records.len()), (h(11), true, 0));
        let mut framed = encode_framed(&[]);
        framed.extend_from_slice(&encode_cursors(&list));
        let (recs, rest) = decode_framed(&framed).unwrap();
        assert!(recs.is_empty());
        assert_eq!(decode_cursors(rest), Some(list));
    }
}
