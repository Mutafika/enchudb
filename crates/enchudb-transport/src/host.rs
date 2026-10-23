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
use std::sync::{Arc, Mutex, RwLock};
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

/// 書き手ごとの位置 (cursor / ack / floor) の列。
type Cursors = Vec<(PeerId, Hlc)>;

/// 書き手ごとの位置の列の形: `[数 u32]([peer u32][wall u64][logical u32][hlc peer u32])*`。
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

/// 位置の列を頭から読み、 列と残りを返す。
fn decode_cursors_prefix(buf: &[u8]) -> Option<(Cursors, &[u8])> {
    let n = u32::from_le_bytes(buf.get(..4)?.try_into().ok()?) as usize;
    let end = 4usize.checked_add(n.checked_mul(20)?)?;
    Some((decode_cursors(buf.get(..end)?)?, &buf[end..]))
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
/// publish の返事の印: 黙ったので外した後に戻ってきた client の record なので受けなかった。 c は新しい peer id の DB にホストから
/// 取り直してから書き直す (留守の間に消えた行を持ったままなので、 受けると忘れた削除を越えて消えた行が生き返りうる)。
const STALE: u8 = 2;

/// 黙った client を線と回収の数から外すまでの既定 (30 日)。
const DEFAULT_SILENCE: Duration = Duration::from_secs(30 * 24 * 60 * 60);

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

/// HLC を `w.l.p` に (query の値)。
fn hlc_param(h: Hlc) -> String {
    format!("{}.{}.{}", h.wall, h.logical, h.peer)
}

fn parse_hlc(s: &str) -> Option<Hlc> {
    let mut it = s.split('.');
    let h = Hlc { wall: it.next()?.parse().ok()?, logical: it.next()?.parse().ok()?, peer: it.next()?.parse().ok()? };
    it.next().is_none().then_some(h)
}

/// ホストが client ごとに覚えること (削除を忘れてよい線のため、 `HOST_PLAN.md` の段 2)。
#[derive(Clone, Copy, Debug, PartialEq)]
struct ClientState {
    /// 最後に来た時刻 (ms)。
    last_seen_ms: u64,
    /// この client の配り終えの線 (HLC がこれ以下の自分の record は全部ホストに出した)。 一度も言っていない client は F を止める
    /// (まだ出していない自分の record が、 線を越えた後に届きうる)。
    through: Option<Hlc>,
    /// この client の受け終えの線 (ホストが配った配り終えの線のうち、 全部受けたと言った物)。
    caught_up: Hlc,
    /// 黙ったので外した (線と `_sync_peers` から)。
    dropped: bool,
    /// 外した後に戻ってきた。 record のある publish を全部断る (留守の間に消えた行を持ったままなので、 線より後の書き込みでも
    /// 消えた行を生き返らせうる)。 新しい peer id の DB で作り直すこと
    returned: bool,
}

impl Default for ClientState {
    fn default() -> Self {
        Self { last_seen_ms: 0, through: None, caught_up: Hlc::ZERO, dropped: false, returned: false }
    }
}

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
    /// client ごとの状態と、 削除を忘れてよい線 (F、 下がらない)。
    lines: Mutex<Lines>,
    /// client の状態の置き場 (`cursor_path` の横)。 無ければ線を出さない (開き直した後に誰を待つか分からないので、 忘れない側)。
    lines_path: Option<PathBuf>,
    /// client の record を当てる間 (read) と、 F を進めてホストが忘れる間 (write) を分ける。 忘れる前に当てた record と、 忘れた
    /// 後に当てた record で答えが変わる (忘れた削除より前の作成) ので、 F を配る前に忘れ終え、 その間は当てない。 client は返事の
    /// F で忘れてから当てる (`Syncer::pull_once`)
    gate: RwLock<()>,
}

struct Lines {
    clients: HashMap<PeerId, ClientState>,
    forget: Hlc,
    silence: Option<Duration>,
    /// 最後に書き出した中身 (変わった時だけ書く)。
    saved: Option<(HashMap<PeerId, ClientState>, Hlc)>,
}

impl Default for Lines {
    fn default() -> Self {
        Self { clients: HashMap::new(), forget: Hlc::ZERO, silence: None, saved: None }
    }
}

impl Lines {
    fn silence(&self) -> Duration {
        self.silence.unwrap_or(DEFAULT_SILENCE)
    }

    /// 線に数える client (黙っていない、 外した後に戻ったのでもない)。
    fn counted(&self, c: &ClientState, now: u64) -> bool {
        !c.dropped && !c.returned && now.saturating_sub(c.last_seen_ms) < self.silence().as_millis() as u64
    }

    /// F の候補 (今の F より上の時だけ): 数える client ごとの min(受け終えの線, 配り終えの線) の最小。 線を一度も言っていない
    /// client は 0。 受け終えの線は、 その client が線を言う前に計算した P (その client の record を待っていない) でもありうる
    /// ので、 配り終えの線でも丸める = F より前の、 数える client の record は全部ホストに当たっている。
    fn forget_candidate(&self, now: u64) -> Option<Hlc> {
        self.clients
            .values()
            .filter(|c| self.counted(c, now))
            .map(|c| c.through.map_or(Hlc::ZERO, |t| c.caught_up.min(t)))
            .min()
            .filter(|f| *f > self.forget)
    }

    /// 書き出す中身。
    fn encode(&self) -> String {
        let mut out = format!("F {}\n", hlc_param(self.forget));
        let mut ids: Vec<_> = self.clients.keys().copied().collect();
        ids.sort_unstable();
        for id in ids {
            let c = &self.clients[&id];
            out.push_str(&format!(
                "C {id} {} {} {} {} {}\n",
                c.last_seen_ms,
                c.through.map_or("-".into(), hlc_param),
                hlc_param(c.caught_up),
                c.dropped as u8,
                c.returned as u8
            ));
        }
        out
    }

    fn decode(text: &str) -> Lines {
        let mut l = Lines::default();
        for line in text.lines() {
            let f: Vec<&str> = line.split(' ').collect();
            match f.as_slice() {
                ["F", h] => l.forget = parse_hlc(h).unwrap_or(Hlc::ZERO),
                ["C", id, seen, through, caught, dropped, returned] => {
                    let (Ok(id), Ok(seen)) = (id.parse::<PeerId>(), seen.parse::<u64>()) else { continue };
                    l.clients.insert(
                        id,
                        ClientState {
                            last_seen_ms: seen,
                            through: parse_hlc(through),
                            caught_up: parse_hlc(caught).unwrap_or(Hlc::ZERO),
                            dropped: *dropped == "1",
                            returned: *returned == "1",
                        },
                    );
                }
                _ => {}
            }
        }
        l
    }

    /// 前に書いた中身から、 書き直すほど変わったか (last_seen だけの変化は 1 時間まで書かない)。
    fn changed(&self) -> bool {
        let Some((clients, forget)) = &self.saved else { return true };
        if *forget != self.forget || clients.len() != self.clients.len() {
            return true;
        }
        self.clients.iter().any(|(id, c)| match clients.get(id) {
            None => true,
            Some(o) => {
                let quiet = ClientState { last_seen_ms: o.last_seen_ms, ..*c };
                quiet != *o || c.last_seen_ms.saturating_sub(o.last_seen_ms) > 3_600_000
            }
        })
    }
}

impl HostInner {
    fn client_lock(&self, c: PeerId) -> Arc<Mutex<()>> {
        self.client_locks.lock().unwrap().entry(c).or_default().clone()
    }

    /// client `from` の record を当てる。 `through` は from の配り終えの線 (この batch を全部当てたら進める)。 戻り値は (返事の印,
    /// from への ack)。
    fn accept(&self, from: PeerId, records: Vec<WireRecord>, through: Option<Hlc>) -> (u8, Vec<(PeerId, Hlc)>) {
        let lock = self.client_lock(from);
        let _serial = lock.lock().unwrap();
        let seen = |s: &Self| s.syncer.cursors_of(from).into_iter().find(|(a, _)| *a == from).map_or(Hlc::ZERO, |(_, h)| h);
        let returning = self.lines_path.is_some()
            && self.lines.lock().unwrap().clients.get(&from).is_some_and(|c| c.dropped || c.returned);
        let mine = records.iter().any(|r| r.author_peer == from);
        if returning && mine {
            self.touch(from, |_| {});
            return (STALE, self.syncer.cursors_of(from));
        }
        let top = records.iter().filter(|r| r.author_peer == from).map(|r| r.hlc).max();
        let (out, all_in) = {
            let _gate = self.gate.read().unwrap_or_else(|p| p.into_inner());
            self.loopback.inbox.lock().unwrap().insert(from, records);
            let out = self.syncer.pull_once(from);
            self.loopback.inbox.lock().unwrap().remove(&from);
            // 全部当てた (from の cursor が batch の最大に届いた) 時だけ、 配り終えの線を進める
            let all_in = top.is_none_or(|t| seen(self) >= t) && !out.history_truncated;
            // 中継として WAL に載せた record を書き出してから ack を返す (ack で client は自分の ring から消すので、 その後にホストが
            // 落ちて中継の record を失うと、 他の client に届く道が無くなる)
            if out.applied > 0 {
                let _ = self.engine.oplog_sync();
            }
            (out, all_in)
        };
        self.touch(from, |c| {
            if all_in && !c.returned && let Some(t) = through {
                c.through = Some(c.through.map_or(t, |cur| cur.max(t)));
            }
        });
        let ack = self
            .loopback
            .acks_for_clients
            .lock()
            .unwrap()
            .remove(&from)
            .unwrap_or_else(|| self.syncer.cursors_of(from));
        (if out.history_truncated { NEED_STATE } else { 0 }, ack)
    }

    /// client `c` が来た: last_seen を今にして `f` を当て、 線を計算し直す。 外した client が来たら、 戻ったとして断る側に回す。
    fn touch(&self, c: PeerId, f: impl FnOnce(&mut ClientState)) {
        let now = now_ms();
        {
            let mut l = self.lines.lock().unwrap();
            let st = l.clients.entry(c).or_default();
            st.last_seen_ms = now;
            if st.dropped {
                st.dropped = false;
                st.returned = true;
            }
            f(st);
        }
        self.recompute(now);
    }

    /// 配り終えの線 P: ホスト自身の書き込みと、 数える client のうち線を言ったものの最小。
    fn publish_line(&self) -> Hlc {
        let mut p = self.engine.sync_published_through().unwrap_or(Hlc::ZERO);
        let now = now_ms();
        let l = self.lines.lock().unwrap();
        for c in l.clients.values().filter(|c| l.counted(c, now)) {
            if let Some(t) = c.through {
                p = p.min(t);
            }
        }
        p
    }

    /// ホストの `_sync_ops` に残っている record の HLC の最小 (残っていなければ None)。 F はこれを越えない: 残っている record は
    /// まだ個別に配りうるので、 その削除をホストが忘れた後に誰かが当てると、 名前で束ねた行の答えが割れる。
    fn ring_min(&self) -> Option<Hlc> {
        self.engine
            .pending_sync_ops(0)
            .iter()
            .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
            .map(|r| r.hlc)
            .min()
    }

    /// 黙った client を外し、 忘れてよい線 F を進め、 進んだらホストの削除の記録を捨てる。 F = min([`Lines::forget_candidate`]、
    /// ホストの ring に残る record) で下がらない (pull しかしない client は F を止める)。 置き場が無ければ何もしない。
    fn recompute(&self, now: u64) {
        let Some(path) = &self.lines_path else { return };
        let mut drop_peers = Vec::new();
        let candidate = {
            let mut l = self.lines.lock().unwrap();
            let limit = l.silence().as_millis() as u64;
            for (id, c) in l.clients.iter_mut() {
                if !c.dropped && !c.returned && now.saturating_sub(c.last_seen_ms) >= limit {
                    c.dropped = true;
                    drop_peers.push(*id);
                }
            }
            l.forget_candidate(now)
        };
        for p in drop_peers {
            self.engine.forget_sync_peer(p);
        }
        if let Some(candidate) = candidate {
            // 当てる間を止めて (write)、 ring の最小で丸め、 忘れ終えてから F を置く
            let _gate = self.gate.write().unwrap_or_else(|p| p.into_inner());
            let f = self.ring_min().map_or(candidate, |m| candidate.min(m));
            let current = self.lines.lock().unwrap().forget;
            if f > current {
                if self.engine.forget_deletes_before(f) > 0 {
                    let _ = self.engine.persist_sync_state();
                }
                self.lines.lock().unwrap().forget = f;
            }
        }
        let text = {
            let mut l = self.lines.lock().unwrap();
            l.changed().then(|| {
                l.saved = Some((l.clients.clone(), l.forget));
                l.encode()
            })
        };
        if let Some(text) = text {
            let tmp = path.with_extension("tmp");
            if std::fs::write(&tmp, text).is_ok() {
                let _ = std::fs::rename(&tmp, path);
            }
        }
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
        // 回収は ring が半分埋まってから (`absorb_pull_acks`)。 F は ring に残る record を越えないので、 削除の記録を忘れるのも回収の後
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
    /// 開き直すと client の record を当て直す。 当て直しは冪等)。 その横 (`<cursor_path>.clients`) に client ごとの線を置き、
    /// 削除を忘れてよい線を配る (`cursor_path` が無ければ線を配らない = 誰も削除の記録を忘れない)。
    pub fn start(bind_addr: &str, engine: Arc<Engine>, cursor_path: Option<PathBuf>) -> io::Result<Self> {
        let host = engine.peer_id();
        if host == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "the host needs a non-zero peer id (Engine::set_peer_id)"));
        }
        engine.set_gossip_remote_apply(true);
        let loopback = Arc::new(Loopback { host, ..Loopback::default() });
        let mut syncer = Syncer::try_new(engine.clone(), loopback.clone())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let lines_path = cursor_path.as_ref().map(|p| {
            let mut s = p.clone().into_os_string();
            s.push(".clients");
            PathBuf::from(s)
        });
        let mut lines = lines_path
            .as_ref()
            .and_then(|p| std::fs::read_to_string(p).ok())
            .map(|t| Lines::decode(&t))
            .unwrap_or_default();
        lines.saved = Some((lines.clients.clone(), lines.forget));
        if let Some(p) = cursor_path {
            syncer = syncer.with_cursor_path(p);
        }
        syncer.set_host(true);
        syncer.serve_state();
        let inner = Arc::new(HostInner {
            engine,
            syncer,
            loopback,
            client_locks: Mutex::new(HashMap::new()),
            lines: Mutex::new(lines),
            lines_path,
            gate: RwLock::new(()),
        });

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

    /// 黙った client を線と回収の数から外すまでの長さ (既定 30 日)。 外した client が戻って自分の record を出すと、 ホストは受けずに
    /// `STALE` を返す (client は新しい peer id の DB に取り直してから書き直す)。
    pub fn set_silence_limit(&self, limit: Duration) {
        self.inner.lines.lock().unwrap().silence = Some(limit);
        self.inner.recompute(now_ms());
    }

    /// 削除を忘れてよい線 (配っている物、 観測・試験用)。 線を配らないホストは ZERO。
    pub fn forget_line(&self) -> Hlc {
        self.inner.lines.lock().unwrap().forget
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
            let through = q.get("through").and_then(|s| parse_hlc(s));
            let (flags, ack) = host.accept(from, records, through);
            if req.route == "/publish" {
                return send_response(&mut stream, 200, b"");
            }
            let mut out = vec![flags];
            out.extend_from_slice(&encode_cursors(&ack));
            send_response(&mut stream, 200, &out)
        }
        ("POST", "/v2/pull") => {
            let Some(to) = peer_param("to") else { return send_response(&mut stream, 400, b"to") };
            let Some(since) = decode_cursors(&req.body) else { return send_response(&mut stream, 400, b"cursors") };
            // 前の返事を全部当てた client は、 その返事の配り終えの線まで受け終えた
            let seen_line = q.get("seen").and_then(|s| parse_hlc(s));
            host.touch(to, |c| {
                if let Some(h) = seen_line {
                    c.caught_up = c.caught_up.max(h);
                }
            });
            // 配り終えの線を record より先に読む (線より前の record は今ホストにある = この後に読む record に入る)。 floor は record の
            // 後 (間に回収が入ると floor が上がる側 = 穴を見逃さない)
            let publish_line = host.publish_line();
            let records = host.records_for(to, &since);
            let mut out = encode_framed(&records);
            out.extend_from_slice(&encode_cursors(&host.floors_for(to)));
            put_hlc(&mut out, publish_line);
            put_hlc(&mut out, host.lines.lock().unwrap().forget);
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
            host.touch(by, |_| {});
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
    /// `Syncer` が record を集める前に伝えた自分の配り終えの線 (次の publish に付ける) と、 最後に送った線。
    through: Mutex<(Option<Hlc>, Option<Hlc>)>,
    /// 前の pull の返事: (配り終えの線, 書き手ごとの record の最大, floor で止めた書き手があったか)。 次の要求で、 全部当てていれば
    /// 線を 「受け終えた」 と返す。
    prev_answer: Mutex<Option<(Hlc, Cursors, bool)>>,
    /// ホストが配った、 削除を忘れてよい線。
    forget: Mutex<Hlc>,
    /// ホストが `STALE` を返した (取り直してから書き直す)。
    rebase: AtomicBool,
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
            through: Mutex::new((None, None)),
            prev_answer: Mutex::new(None),
            forget: Mutex::new(Hlc::ZERO),
            rebase: AtomicBool::new(false),
        })
    }

    /// ホストがこの client の record を受けなかった (黙ったので外された後に戻ってきた)。 新しい peer id の DB にホストから取り直して
    /// から書き直すこと (この peer id の record はもう受けない)。 受けると、 忘れた削除を越えて消えた行が生き返りうる。
    pub fn rebase_required(&self) -> bool {
        self.rebase.load(Ordering::Acquire)
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
        // 送る record が無くても、 配り終えの線が進んだら言う (書き込みの無い client が線を止めない)
        let (through, sent) = *self.through.lock().unwrap();
        if records.is_empty() && (from != self.me || through.is_none() || through == sent) {
            return;
        }
        let mut path = format!("/v2/publish?peer={from}");
        if from == self.me
            && let Some(t) = through
        {
            path.push_str(&format!("&through={}", hlc_param(t)));
        }
        let Ok((200, body)) = self.http.request("POST", &path, &encode_batch(&records)) else { return };
        let (Some(&flags), Some(ack)) = (body.first(), body.get(1..).and_then(decode_cursors)) else { return };
        if flags & STALE != 0 {
            self.rebase.store(true, Ordering::Release);
            return;
        }
        if from == self.me {
            self.through.lock().unwrap().1 = through;
        }
        merge_max(&mut self.acked.lock().unwrap(), &ack);
        merge_max(self.pending_acks.lock().unwrap().entry(from).or_default(), &ack);
        if flags & NEED_STATE != 0 {
            self.push_state(from);
        }
    }

    fn uses_published_through(&self) -> bool {
        true
    }

    fn note_published_through(&self, peer: PeerId, through: Hlc) {
        if peer == self.me {
            self.through.lock().unwrap().0 = Some(through);
        }
    }

    fn forget_horizon(&self, from: PeerId) -> Option<Hlc> {
        let f = *self.forget.lock().unwrap();
        (from == self.host && f > Hlc::ZERO).then_some(f)
    }

    fn known_peers(&self) -> Vec<PeerId> {
        vec![self.host]
    }

    fn pull_as_multi(&self, to: PeerId, from: PeerId, since: &[(PeerId, Hlc)]) -> Vec<WireRecord> {
        if from != self.host {
            return Vec::new();
        }
        // 前の返事を全部当てた (cursor が返事の record に届いた、 floor で止めた書き手が無い) なら、 その返事の配り終えの線まで
        // 受け終えたと言う
        let cur = |a: PeerId| since.iter().find(|(p, _)| *p == a).map_or(Hlc::ZERO, |(_, h)| *h);
        let seen = self
            .prev_answer
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(_, tops, held)| !held && tops.iter().all(|(a, t)| cur(*a) >= *t))
            .map(|(p, _, _)| *p);
        let mut path = format!("/v2/pull?to={to}");
        if let Some(p) = seen {
            path.push_str(&format!("&seen={}", hlc_param(p)));
        }
        let Ok((200, body)) = self.http.request("POST", &path, &encode_cursors(since)) else {
            return Vec::new();
        };
        let Some((records, rest)) = decode_framed(&body) else { return Vec::new() };
        let Some((floors, rest)) = decode_cursors_prefix(rest) else { return Vec::new() };
        let (publish_line, forget) = match rest.len() {
            32 => (get_hlc(&rest[..16]), get_hlc(&rest[16..])),
            _ => (Hlc::ZERO, Hlc::ZERO),
        };
        // 返事の floor は record を読んだ後の物。 cursor が floor に届かない書き手は、 この返事の record も途中から始まりうる
        // (間に回収が入った) ので渡さない — cursor は進まず、 次の `pull_once` の前の判定が floor で抜けを知らせる
        let baseline = floors.iter().find(|(a, _)| *a == u32::MAX).map_or(Hlc::ZERO, |(_, h)| *h);
        let short = |a: PeerId| {
            let cur = since.iter().find(|(p, _)| *p == a).map_or(Hlc::ZERO, |(_, h)| *h);
            let f = floors.iter().find(|(p, _)| *p == a).map_or(Hlc::ZERO, |(_, h)| *h);
            cur < f.max(baseline)
        };
        let held = records.iter().any(|r| short(r.author_peer));
        let records: Vec<WireRecord> = records.into_iter().filter(|r| !short(r.author_peer)).collect();
        let mut tops: Vec<(PeerId, Hlc)> = Vec::new();
        for r in &records {
            merge_max(&mut tops, &[(r.author_peer, r.hlc)]);
        }
        *self.prev_answer.lock().unwrap() = Some((publish_line, tops, held));
        {
            let mut f = self.forget.lock().unwrap();
            *f = (*f).max(forget);
        }
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

    /// F は数える client の配り終えの線を越えない: 線を言う前に計算された P を受け終えた client (時計が遅れていて、 配り終えの線が
    /// P より下) が居ても、 F はその client の配り終えの線まで。 線を一度も言っていない client と、 外した / 戻った client も見る。
    #[test]
    fn the_forget_line_does_not_pass_a_clients_published_line() {
        let h = |w| Hlc { wall: w, logical: 0, peer: 0 };
        let now = 1_000;
        let client = |through: Option<u64>, caught: u64| ClientState {
            last_seen_ms: now,
            through: through.map(h),
            caught_up: h(caught),
            ..ClientState::default()
        };
        let mut l = Lines::default();
        l.clients.insert(1, client(Some(90), 80));
        l.clients.insert(2, client(Some(50), 80)); // P=80 を受け終えたが、 自分の record は 50 までしか出していない
        assert_eq!(l.forget_candidate(now), Some(h(50)), "client 2 のまだ出していない record を越えた");
        l.clients.insert(3, client(None, 80));
        assert_eq!(l.forget_candidate(now), None, "線を言っていない client が居るのに進んだ");
        l.clients.get_mut(&3).unwrap().returned = true;
        l.clients.insert(4, ClientState { dropped: true, ..client(None, 0) });
        assert_eq!(l.forget_candidate(now), Some(h(50)), "外した / 戻った client が F を止めた");
        l.forget = h(50);
        assert_eq!(l.forget_candidate(now), None, "F が下がらずに同じ値を言い直した");
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
