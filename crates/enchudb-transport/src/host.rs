//! ホスト: 全 client が経由する、 DB を持つ peer の HTTP の口 (`HttpHost`) と、 それと話す client 側の transport
//! (`HttpHostTransport`)。
//!
//! sync の既定はホストを置く形 (2026-10-23)。 ホストは自分の `Engine` に client の record を当て (`Syncer` の pull と同じ道)、
//! 中継として自分の `_sync_ops` に載せ、 client はホストの `_sync_ops` から pull する。 共有の掲示板 (`HttpRelay`) と違い、
//! record の置き場はホストの DB だけで、 再起動しても続く。
//!
//! ホストが中継に載せるのは **受けた record の全部** (#698): ホストの LWW で負けた record も載せる。 client はホストからしか
//! 受けないので、 負けた record (特に負けた名前のカード = 「この世界番号はこの名前のデッキ」 を運ぶ唯一の record) を配らないと、
//! その世界番号宛ての後の書き込み・削除・改名を他の client が自分の行に結べず、 相手から直に pull する形と答えが割れる。
//! LWW は受け手がそれぞれ自分でする。 載せないのは、 検査 (ACL・署名) や容量で断った record (届き直す) と Commit だけ。
//! 容量で断った record より後 (HLC) の同じ書き手の record は、 断った record が通るまで受けない (当てず載せず、 一緒に
//! 届き直す。 #754: 先に載せると、 受け手の cursor が断った record を越えて、 後で通っても届かない)。
//!
//! - `POST /v2/publish?peer=c` : client c の record。 ホストが当てて中継し、 WAL を書き出してから、 ホストが当てた位置 (c への ack)
//!   を返す。 client の `_sync_ops` はこの ack で回収される (ホストは record の置き場の権威なので、 #227 の下流の消化で丸めない)。
//!   書き出せなければ ack を返さない (503、 #961: client は受けたと思わずに次の beat で送り直す)。 client の cursor の置き場は
//!   書き出しが返った後に書く (#964)。 query の `acked=peer:w.l.p,..` は client が 「ホストが受けた」 と思っている位置 (#961 (b)):
//!   ホストの cursor がそこまで届いていなければ (作り直した / 古い控えから戻した)、 当てずに印 `BEHIND` と今の位置を返す (client は
//!   覚えを置き換えて、 足りない所から送り直す)。 付けない client (0.37.4 以前) には確かめない
//! - `POST /v2/pull?to=c` (body = 書き手ごとの cursor) : ホストの `_sync_ops` のうち cursor より後 (c 自身の record は除く) と、
//!   ホストの floor。 floor は record を読んだ後に読む (間に回収が入っても穴を見逃さない側)。 cursor は ack も兼ねる。
//!   返事は `[framed records][floor の列][配り終えの線 16][忘れてよい線 16]`、 c が黙って外された後に戻った client の時だけ、
//!   後ろに印 1 byte (`STALE`) が付く (#712)
//! - `POST /v2/ack?peer=c` (body = cursor) : c がホストの流れを当てた位置。 ホストの回収を回す
//! - `POST /v2/floor?peer=c` (body = floor) : c の floor (c が WAL に載らずに落とした record など)。 ホストの c の cursor が届かなければ、
//!   次の publish の返事で c の state を求める → c は `POST /v2/state?author=c` で自分の state を送る
//! - `GET /v2/floors?to=c` : c に配る floor (c がまだ pull していない時の、 pull の前の判定に)
//! - `GET /v2/state?author=a` : ホストが持つ a の state (抜けた client の取り直し)
//! - 古い client (`GET /pull?from&wall..` / `POST /publish?peer=`) もそのまま受ける (ack が届かないので、 その client の分は回収されない)
//!
//! 計画は repo 直下の `HOST_PLAN.md`。

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{JoinHandle, ThreadId};
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
///
/// pull の返事の後ろにも付く (#712、 外した後に戻ってきた client の時だけ): 何も出さずに受けるだけの client は publish の返事を
/// 受けないので、 留守の間に消えた行を持ったまま何も知らされなかった (ホストの state は replica の物で、 消えた行の掃除は走らない)。
/// record は今までどおり配る。 印の無い返事は今までと同じ長さなので、 古い client には外された時だけ長さが変わる (古い client は
/// 長さの違う後ろを読まず、 線を ZERO と読む = 何も忘れない側。 外された client は線に数えていない)
const STALE: u8 = 2;
/// publish の返事の印 (#961、 要求に `acked` を付けた client にだけ返す): ホストの cursor が、 client の言う 「ホストが受けた位置」
/// に届いていない (同じ URL の裏でホストを空の DB で作り直した / 古い控えから戻した / cursor の置き場が WAL より遅れている)。
/// record は当てていない (当てると cursor が新しい record で進み、 届いていない古い record を送り直しても `hlc > cursor` で落ちる)。
/// 返事の位置はホストの今の cursor で、 client は覚え (`acked`) をそれに置き換えて、 次の beat で足りない所から送り直す。
///
/// 返事の形は今までと同じ (`[印][位置の列]`)。 `acked` を付けない client (0.37.4 以前) には返さない (古い client は知らない印を
/// 読まずに位置を ack として足すので)。 0.37.4 以前のホストは `acked` を読まない (今までどおり当てる = 直らないが壊れない)
const BEHIND: u8 = 4;
/// `acked` に載せる書き手の数の上限 (request line の長さ。 ホストは header を 16 KiB までしか読まない)。 載せない書き手は確かめない
/// だけ (誤って `BEHIND` にはならない)。
const ACKED_PARAM_MAX: usize = 64;

/// 黙った client を線と回収の数から外すまでの既定 (30 日)。
const DEFAULT_SILENCE: Duration = Duration::from_secs(30 * 24 * 60 * 60);

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

/// ホストの時計 (#713): **ホストが動いていた時間** (ms)。 client の沈黙 (`last_seen_ms`) はこれで数える。
///
/// 前は壁時計だった。 ホストが止まっていた間も client の沈黙に数えるので、 ホストが沈黙の上限より長く止まった後は、 最初の
/// client が来た時点で他の全員が 「黙った」 として外され、 その後に来て record を出すと `STALE` で断られた (client は黙って
/// いたのではなく、 ホストに届かなかっただけ)。 壁時計が大きく進んだ時 (起動時に時計が古い機械) も同じだった。
///
/// 動いている間は単調な時計 (`Instant`) で進め、 止まる時 / client の状態を書く時に今の値を置き場に残し (`T` の行)、 開き直すと
/// そこから続ける = 止まっていた間は数えない。 最初の値は壁時計 (ms) なので、 止まったことの無いホストでは壁時計と同じ。
struct HostClock {
    base: u64,
    started: std::time::Instant,
}

impl HostClock {
    /// 置き場から読んだ状態の続きから。 `T` の行があればその値。 無い置き場 (この版より前のホストが書いた、 `last_seen` は
    /// 壁時計) は、 いちばん最近来た client の時刻 (それより後は、 動いていたか止まっていたか分からない = 数えない)。 client の
    /// 居ない / 置き場の無いホストは今の壁時計 `wall`。
    fn resume(lines: &Lines, wall: u64) -> Self {
        let base = lines.clock_ms.or_else(|| lines.clients.values().map(|c| c.last_seen_ms).max()).unwrap_or(wall);
        Self { base, started: std::time::Instant::now() }
    }

    fn now(&self) -> u64 {
        self.base + self.started.elapsed().as_millis() as u64
    }
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

/// 書き手ごとの位置の列を query の値に: `peer:w.l.p,peer:w.l.p`。 `first` の書き手を先に、 [`ACKED_PARAM_MAX`] 個まで。
fn cursors_param(list: &[(PeerId, Hlc)], first: PeerId) -> String {
    let head = list.iter().filter(|(p, _)| *p == first);
    let rest = list.iter().filter(|(p, _)| *p != first);
    head.chain(rest)
        .take(ACKED_PARAM_MAX)
        .map(|(p, h)| format!("{p}:{}", hlc_param(*h)))
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_cursors_param(s: &str) -> Option<Cursors> {
    s.split(',')
        .map(|e| {
            let (p, h) = e.split_once(':')?;
            Some((p.parse().ok()?, parse_hlc(h)?))
        })
        .collect()
}

/// ホストが client ごとに覚えること (削除を忘れてよい線のため、 `HOST_PLAN.md` の段 2)。
#[derive(Clone, Copy, Debug, PartialEq)]
struct ClientState {
    /// 最後に来た時刻 (ms、 ホストの時計 [`HostClock`] = ホストが動いていた時間)。
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

/// `floor` (書き手ごとの位置) より後の record か。 位置の無い書き手の record は全部 (`Syncer` が `Transport::publish_floor` の位置で
/// 集める record を絞る式と同じ)。
fn above_floor(floor: &[(PeerId, Hlc)], r: &WireRecord) -> bool {
    floor.iter().find(|(a, _)| *a == r.author_peer).is_none_or(|(_, h)| r.hlc > *h)
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
    /// ホストが state を求めた client (publish の返事に `NEED_STATE` を立てた)。 `POST /v2/state` はここに居る書き手の分だけ受け、
    /// 受けたら外す (#699: 求めていない state を受けると、 空で complete の state 1 つでその書き手の行を全部掃除していた)。
    asked_state: Mutex<HashSet<PeerId>>,
    /// client ごとの状態と、 削除を忘れてよい線 (F、 下がらない)。
    lines: Mutex<Lines>,
    /// client の状態の置き場 (`cursor_path` の横)。 無ければ線を出さない (開き直した後に誰を待つか分からないので、 忘れない側)。
    lines_path: Option<PathBuf>,
    /// client の沈黙を数える時計 (ホストが動いていた時間、 #713)。
    clock: HostClock,
    /// 当てている受信 (`accept` と、 client が送った state を当てる `POST /v2/state`) と、 F を進めてホストが忘れる間の登録
    /// ([`Applying`])。 守る物は 2 つ (3820cd9):
    ///
    /// 1. F は当てている record を越えない: 当てている途中の record は、 当て終えて ring に写すまで `ring_min` に見えない。 受信は
    ///    当てる前に record の版数の最小を登録し、 ring に写した後 (`sync_for_ack` の後) に外す。 F = min(候補, `ring_min`, 登録の
    ///    最小)
    /// 2. F より古い record を当てるのと、 F より前の削除の記録を忘れるのを重ねない (忘れる前に当てた record と、 忘れた後に当てた
    ///    record で答えが変わる = 忘れた削除より前の作成)。 忘れている途中に来た、 その F より古い record を持つ受信は、 忘れ終える
    ///    まで待つ (前の gate と同じく、 忘れた後に当てる)。 F 以上の record だけを持つ受信は、 忘れるのと並んで当てる: F 以上の
    ///    record の答えは F より前の削除の記録に依らない (依るなら、 F を受けて忘れてから当てる client (`Syncer::pull_once`) と、
    ///    忘れる前に当てたホストで答えが割れる = 前の作りでも同じ前提)
    ///
    /// 数える client の受信では、 その client の線 (候補 ≤ 配り終えの線 < 新しい record) が先に効く。 登録が F を抑えるのは、 数えない
    /// client の受信 (新しい client の最初の publish、 `BEHIND` / 503 の後の送り直し、 他の書き手の record を運ぶ client) と state の
    /// 道。 今の F より下の record を当てている受信が居る間 (state の古い cell など)、 F は進まない (上限が今の F 以下)。
    ///
    /// #980: 前は `RwLock` (当てる間 read、 忘れる間 write) で締め出した。 当てる間は行の lock を待ちうる (アプリが握る行・WAL の
    /// 空きを待つ書き手の行・同じ stripe の行) ので、 write で待つと、 std の `RwLock` は書き手が待つ間に新しい読み手を通さず、
    /// 後から来た全 client の受信が、 その 1 つの受信が行を待つ間ずっと止まった。 待たない `try_write` にすると、 受信が絶えず
    /// 重なるホストで F が上限なく遅れた。 登録の形は誰も締め出さない (待つのは 2 の古い record の受信が、 忘れる間だけ)。
    ///
    /// 残り: `oplog_sync` の bridge が ring に全部を写す前提。 ring が満杯で止まった時 (#846)・落ちた書き込みの後の止め (#783)・
    /// `oplog_sync` の失敗 (fsync の失敗は bridge の前に返る。 `sync_for_ack` は false のまま登録を外す) は、 外した後も record が
    /// WAL にだけある = F が越えうる (前の gate も同じ所で外れた)。 外すのを bridge の位置で決めたいが、 bridge の cursor は畳む時に
    /// 頭へ戻る (単調でない)。 engine から 「bridge がまだ書いていない record の版数の最小」 を出して F を抑えるのが直す道。
    applying: Mutex<Applying>,
    /// [`Applying::forgetting`] が外れた (待っている受信を起こす)。
    forgot: std::sync::Condvar,
    /// #753: client が送った state を当てて載せ、 ring に写し終えるまで (write) と、 ring を読んで配る間 (read、 `records_for` /
    /// `records_since`) を分ける。 state の record は HLC の順に並ばないので、 途中まで載った所を配ると、 受け手の cursor が、
    /// まだ載っていない (版数の小さい) record を越える。 差分 (`accept`) の record は書き手ごとに HLC の昇順で載るので止めない。
    /// #980 の前は F を進める間と同じ lock (`gate`) で、 F を進める書き手が待つ間 pull も止まった。
    state_gate: RwLock<()>,
    /// #961: 失敗した WAL の書き出し (`oplog_sync`) の数と、 その後に始めて成功した書き出しが覆った数。 `wal_cleared < wal_failed` の間は、
    /// 新しく載せた record が無くても ack の前に書き出し直す ([`HostInner::sync_for_ack`])。
    wal_failed: AtomicU64,
    wal_cleared: AtomicU64,
    /// 試験: ホストの `oplog_sync` を失敗させる。
    #[cfg(test)]
    fail_wal: AtomicBool,
    /// 試験: ホストの `oplog_sync` の割り込み点 (#964)。 書き出しの直前に `false`、 書き出しが返った直後に `true` で呼ぶ。
    #[cfg(test)]
    wal_sync_hook: Mutex<Option<WalSyncHook>>,
    /// 試験: F を忘れる段に入った回数 (`recompute` が登録を見て、 今の F より上を決めに行った)。
    #[cfg(test)]
    forget_rounds: AtomicU64,
    /// 試験: F を忘れている途中 (`forgetting` を立てた後、 `ring_min` の前) の割り込み点。
    #[cfg(test)]
    forget_hook: Mutex<Option<Box<dyn FnMut() + Send>>>,
    /// 試験: 忘れている間に待った受信の数。
    #[cfg(test)]
    waited: AtomicU64,
    /// 置き場 (`.clients`) を書くのを 1 本ずつにする ([`HostInner::save_lines`])。
    save_lock: Mutex<()>,
    /// 置き場を書けなかった (書けるまで、 失敗を 1 回だけ言う)。
    save_failing: AtomicBool,
    /// 試験: `recompute` が削除の記録を忘れて disk に書いた (`persist_sync_state`) 直後の割り込み点 (0.38 前 review の B4)。
    #[cfg(test)]
    forgot_hook: Mutex<Option<Box<dyn FnMut() + Send>>>,
    /// 試験: `recompute` が登録を読む (`applying` の lock を取る) 直前の割り込み点 (1 回だけ呼ぶ)。
    #[cfg(test)]
    lock_hook: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

/// 当てている受信の登録 ([`HostInner`] の `applying`)。
#[derive(Default)]
struct Applying {
    next: u64,
    /// 当てている受信ごとの、 当てる record の版数の最小。
    live: std::collections::BTreeMap<u64, Hlc>,
    /// 忘れている途中に来て待っている受信の、 当てる record の版数の最小 (次の回の上限に入れる)。
    waiting: std::collections::BTreeMap<u64, Hlc>,
    /// F を忘れている途中なら、 その上限 (候補と登録の最小の小さい方)。 この上限より古い record を持つ受信は、 外れるまで待つ。
    forgetting: Option<Hlc>,
}

/// 受信の登録 (drop で外す)。
struct ApplyGuard<'a> {
    host: &'a HostInner,
    id: Option<u64>,
}

impl Drop for ApplyGuard<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            self.host.applying.lock().unwrap_or_else(|p| p.into_inner()).live.remove(&id);
        }
    }
}

/// 忘れている途中の印 (drop で外して、 待っている受信を起こす。 忘れる途中で panic しても受信を待たせ続けない)。
struct ForgetGuard<'a>(&'a HostInner);

impl Drop for ForgetGuard<'_> {
    fn drop(&mut self) {
        self.0.applying.lock().unwrap_or_else(|p| p.into_inner()).forgetting = None;
        self.0.forgot.notify_all();
    }
}

/// 試験の割り込み点 ([`HostInner`] の `wal_sync_hook`)。 引数は 「書き出しが返った後か」。
#[cfg(test)]
type WalSyncHook = Box<dyn FnMut(bool) + Send>;

struct Lines {
    clients: HashMap<PeerId, ClientState>,
    forget: Hlc,
    silence: Option<Duration>,
    /// 最後に書き出した中身 (変わった時だけ書く)。
    saved: Option<(HashMap<PeerId, ClientState>, Hlc)>,
    /// 置き場にあったホストの時計 (`T` の行、 [`HostClock`])。 この版より前のホストが書いた置き場には無い。
    clock_ms: Option<u64>,
}

impl Default for Lines {
    fn default() -> Self {
        Self { clients: HashMap::new(), forget: Hlc::ZERO, silence: None, saved: None, clock_ms: None }
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

    /// 書き出す中身。 `now` = 今のホストの時計 (`T` の行。 知らない版は読み飛ばす)。
    fn encode(&self, now: u64) -> String {
        let mut out = format!("F {}\nT {now}\n", hlc_param(self.forget));
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
                ["T", t] => l.clock_ms = t.parse().ok(),
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

    /// 前に書いた中身から、 書き直すほど変わったか: client の出入りと外した / 戻った印 (F を進める前に disk にあるべき物)、 last_seen の
    /// 間引きの幅 ([`Lines::last_seen_slack_ms`]) を越える変化。
    ///
    /// #966 (0.38 前 review B4): 線 (through / caught_up) と F の変化だけでは書かない。 書くたびに中身と directory を fsync するので、
    /// 要求のたびに線が動くと要求ごとに fsync が乗った (前は fsync せずに書き、 線と F の変化でも書いた)。 落ちた後に開くと、 線と F は
    /// 最後に書いた時点に戻る = 低い側 (F を低く決める = 削除を早く忘れない側)。 last_seen は、 最後に書いた後の訪れを失うので、 沈黙を
    /// 最大で間引きの幅だけ長く数えうる (ホストの時計も最後に書いた時点に戻り、 その後に動いていた時間は数えない)。 きれいに止めた時は
    /// 全部書く (`Drop`)。
    fn changed(&self) -> bool {
        let Some((clients, _)) = &self.saved else { return true };
        if clients.len() != self.clients.len() {
            return true;
        }
        let slack = self.last_seen_slack_ms();
        self.clients.iter().any(|(id, c)| match clients.get(id) {
            None => true,
            Some(o) => {
                c.dropped != o.dropped || c.returned != o.returned || c.last_seen_ms.saturating_sub(o.last_seen_ms) > slack
            }
        })
    }

    /// last_seen だけの変化を書かない幅 (ms): min(1 時間, 沈黙の上限 / 4)。 落ちた後に開くと、 client の沈黙を最大でこの幅だけ長く
    /// 数えうる (#966 の読み直し [F5]: 幅を 1 時間に決めていると、 沈黙の上限が 1 時間前後以下の時、 落ちる前に来ていた生きている
    /// client を開いた後に外しうる = 戻った client は publish を断られて作り直しになる)。 上限の 4 分の 1 なら、 上限の 4 分の 3 より
    /// 短い間隔で来る client は外さない。
    fn last_seen_slack_ms(&self) -> u64 {
        (self.silence().as_millis() as u64 / 4).min(3_600_000)
    }
}

impl HostInner {
    fn client_lock(&self, c: PeerId) -> Arc<Mutex<()>> {
        self.client_locks.lock().unwrap().entry(c).or_default().clone()
    }

    /// 当てる前に登録する (`applying` の 1)。 `min` = 当てる record の版数の最小 (record が無ければ登録しない)。 F を忘れている
    /// 途中で、 その上限より古い record を持つなら、 忘れ終えるまで待つ (`applying` の 2)。 登録は ring に写した後に外す
    /// (guard を `sync_for_ack` の後まで持つ)。
    fn begin_apply(&self, min: Option<Hlc>) -> ApplyGuard<'_> {
        let Some(min) = min else { return ApplyGuard { host: self, id: None } };
        let mut a = self.applying.lock().unwrap_or_else(|p| p.into_inner());
        a.next += 1;
        let id = a.next;
        if a.forgetting.is_some_and(|f| min < f) {
            #[cfg(test)]
            self.waited.fetch_add(1, Ordering::AcqRel);
            // 待つ間も次の回の上限に入れる (待っている間に始まった次の回が、 この受信の record を越えて上限を上げない = 待つのは
            // 高々 1 回)
            a.waiting.insert(id, min);
            while a.forgetting.is_some_and(|f| min < f) {
                a = self.forgot.wait(a).unwrap_or_else(|p| p.into_inner());
            }
            a.waiting.remove(&id);
        }
        a.live.insert(id, min);
        ApplyGuard { host: self, id: Some(id) }
    }

    /// client `from` の record を当てる。 `through` は from の配り終えの線 (この batch を全部当てたら進める)。 `acked` は from が
    /// 「ホストが受けた」 と思っている位置 (#961、 0.37.4 以前の client は付けない)。 戻り値は (返事の印, from への ack)。 WAL を
    /// 書き出せなかった時は `None` (ack を返さない、 #961)。
    fn accept(
        &self,
        from: PeerId,
        records: Vec<WireRecord>,
        through: Option<Hlc>,
        acked: Option<Cursors>,
    ) -> Option<(u8, Vec<(PeerId, Hlc)>)> {
        let lock = self.client_lock(from);
        let _serial = lock.lock().unwrap();
        let seen = |s: &Self| s.syncer.cursors_of(from).into_iter().find(|(a, _)| *a == from).map_or(Hlc::ZERO, |(_, h)| h);
        let returning = self.is_stale(from);
        let mine = records.iter().any(|r| r.author_peer == from);
        if returning && mine {
            self.touch(from, |_| {});
            return Some((STALE, self.syncer.cursors_of(from)));
        }
        // #961: client が 「ホストが受けた」 と思っている位置に、 ホストの cursor が届いていない (同じ URL の裏でホストを作り直した /
        // 古い控えから戻した / cursor の置き場が WAL より遅れている = #964)。 この batch を当てると cursor が新しい record で進み、
        // その後に client が届いていない古い record を送り直しても `hlc > cursor` で落ちる。 当てずに今の位置を返す (`BEHIND`、
        // client は覚えを置き換えて、 次の beat で足りない所から送り直す。 client の ring が回収済みなら、 その時 floor で state を求める)
        if let Some(acked) = acked {
            let have = self.syncer.cursors_of(from);
            let pos = |a: PeerId| have.iter().find(|(p, _)| *p == a).map_or(Hlc::ZERO, |(_, h)| *h);
            if acked.iter().any(|(a, h)| pos(*a) < *h) {
                self.touch(from, |_| {});
                return Some((BEHIND, have));
            }
        }
        let top = records.iter().filter(|r| r.author_peer == from).map(|r| r.hlc).max();
        let (out, all_in, durable) = {
            // 当てて ring に写し終えるまで、 F はこの batch の record を越えない (`applying`)
            let _applying = self.begin_apply(records.iter().map(|r| r.hlc).min());
            self.loopback.inbox.lock().unwrap().insert(from, records);
            let out = self.syncer.pull_once(from);
            self.loopback.inbox.lock().unwrap().remove(&from);
            // 全部当てた (from の cursor が batch の最大に届いた) 時だけ、 配り終えの線を進める
            let all_in = top.is_none_or(|t| seen(self) >= t) && !out.history_truncated;
            // 中継として WAL に載せた record を書き出してから ack を返す (ack で client は自分の ring から消すので、 その後にホストが
            // 落ちて中継の record を失うと、 他の client に届く道が無くなる)。 #698: LWW で負けた record / 宛先を解決できなかった
            // record も中継に載っている
            let durable = self.sync_for_ack(out.applied + out.skipped + out.dropped_unresolved + out.dropped_vocab > 0);
            (out, all_in, durable)
        };
        if !durable {
            // #961: ack も配り終えの線も進めない (client は受けたと思わずに次の beat で送り直す。 届き直した record はホストの cursor が
            // 越えているので当たらないが、 次の ack の前に書き出し直す = `sync_for_ack`)
            self.touch(from, |_| {});
            return None;
        }
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
        if out.history_truncated {
            self.asked_state.lock().unwrap().insert(from);
        }
        Some((if out.history_truncated { NEED_STATE } else { 0 }, ack))
    }

    /// #961: ack を返す前の WAL の書き出し。 `needed` = この呼びで WAL に載せた record がある。 書き出せたら (書き出す物が無かったら)
    /// true。
    ///
    /// 前に失敗した書き出しがあれば、 `needed` でなくても書き出し直す: 失敗した回に載せた record は、 client が送り直しても当たらず
    /// 載らない (ホストの cursor は当てた時に越えている。 越えていなくても、 当て直して負けた record は `relay_held` で 2 度載せない)
    /// ので、 書き出さずに ack を返すと、 書き出していない record を受けたと言う。 失敗の数は書き出しを始める**前**に読む: 読んだ数の
    /// 失敗は、 その回の record を載せた後に書き出しが返っている = この書き出しが覆う。 並んで失敗した回 (読んだ後に足された数) は
    /// 覆ったと数えない。
    ///
    /// #964: client ごとの cursor の置き場は、 この書き出しが返った後に書く (`Syncer::hold_cursor_file`)。 書くのは書き出しを始める
    /// **前**の cursor の写し (写しの位置までの record は当てた時に WAL に載っている = この書き出しが覆う)。 前は `pull_once` の中で
    /// 当てた直後に書いていて、 書き出しまでの間に電源が落ちた像では置き場だけが 「受けた」 と言い、 開き直したホストは client の
    /// 送り直しを cursor で落とした。
    fn sync_for_ack(&self, needed: bool) -> bool {
        let failed = self.wal_failed.load(Ordering::Acquire);
        if !needed && self.wal_cleared.load(Ordering::Acquire) >= failed {
            return true;
        }
        let mark = self.syncer.cursor_mark();
        match self.oplog_sync() {
            Ok(()) => {
                self.wal_cleared.fetch_max(failed, Ordering::AcqRel);
                self.syncer.save_cursors_through(mark);
                true
            }
            Err(e) => {
                self.wal_failed.fetch_add(1, Ordering::AcqRel);
                eprintln!("[enchudb] host: oplog_sync failed ({e}) — not acking, the client will send the records again");
                false
            }
        }
    }

    /// ホストの WAL の書き出し (試験は失敗させられる)。
    fn oplog_sync(&self) -> io::Result<()> {
        #[cfg(test)]
        if let Some(hook) = self.wal_sync_hook.lock().unwrap().as_mut() {
            hook(false);
        }
        #[cfg(test)]
        if self.fail_wal.load(Ordering::Acquire) {
            return Err(io::Error::other("test: oplog_sync fails"));
        }
        let r = self.engine.oplog_sync();
        #[cfg(test)]
        if let Some(hook) = self.wal_sync_hook.lock().unwrap().as_mut() {
            hook(true);
        }
        r
    }

    /// client `c` が来た: last_seen を今にして `f` を当て、 線を計算し直す。 外した client が来たら、 戻ったとして断る側に回す。
    fn touch(&self, c: PeerId, f: impl FnOnce(&mut ClientState)) {
        let now = self.clock.now();
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
        let now = self.clock.now();
        let l = self.lines.lock().unwrap();
        for c in l.clients.values().filter(|c| l.counted(c, now)) {
            if let Some(t) = c.through {
                p = p.min(t);
            }
        }
        p
    }

    /// 黙ったので外した client か、 外した後に戻ってきた client か (線を持つホストだけ)。
    fn is_stale(&self, c: PeerId) -> bool {
        self.lines_path.is_some() && self.lines.lock().unwrap().clients.get(&c).is_some_and(|c| c.dropped || c.returned)
    }

    /// 黙った client を外し、 忘れてよい線 F を進め、 進んだらホストの削除の記録を捨てる。 F = min([`Lines::forget_candidate`]、
    /// ホストの ring に残る record) で下がらない (pull しかしない client は F を止める)。 置き場が無ければ何もしない。
    ///
    /// #980: 当てている受信を待たない。 F は登録された受信の record の版数の最小で抑える (`applying`)。 別の要求が忘れている途中
    /// なら、 この回は進めない (次の要求が試す。 その回の候補は、 こちらの `touch` が上げた線をまだ見ていないことがある)。
    fn recompute(&self, now: u64) {
        if self.lines_path.is_none() {
            return;
        }
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
        // 当てている受信の record の最小と ring の最小で丸め、 忘れ終えてから F を置く。 忘れている間に来た、 上限より古い record の
        // 受信は待つ (`applying` の 2)。 登録の最小を読むのと印を立てるのは同じ lock の下: その後に登録する受信は印を見る
        let upper = candidate.and_then(|candidate| {
            #[cfg(test)]
            {
                let hook = self.lock_hook.lock().unwrap().take();
                if let Some(hook) = hook {
                    hook();
                }
            }
            let mut a = self.applying.lock().unwrap_or_else(|p| p.into_inner());
            let upper = a.live.values().chain(a.waiting.values()).fold(candidate, |m, h| m.min(*h));
            let current = self.lines.lock().unwrap().forget;
            (a.forgetting.is_none() && upper > current).then(|| {
                a.forgetting = Some(upper);
                upper
            })
        });
        if let Some(upper) = upper {
            let _forgetting = ForgetGuard(self);
            #[cfg(test)]
            {
                self.forget_rounds.fetch_add(1, Ordering::AcqRel);
                if let Some(hook) = self.forget_hook.lock().unwrap().as_mut() {
                    hook();
                }
            }
            // 登録を外した受信の record は ring にある (外したのは `sync_for_ack` の後) = 登録を読んだ後に ring を読む。 ホストの
            // `_sync_ops` に残っている record は、 まだ個別に配りうるので F はそれを越えない (その削除をホストが忘れた後に誰かが当てると、
            // 名前で束ねた行の答えが割れる)。
            // #985: min(上限, ring の最小)。 前は ring の全部の行を写して decode した (100 万行で 1 回 60〜110 ms、 F が ring の最古の
            // record に留められている間は要求のたびに)。 engine の覚え (`Engine::sync_ring_min_below`) は、 bridge が足した行と回収で
            // 消えた行で直し、 下の境が上限以上なら読まない
            let f = self.engine.sync_ring_min_below(upper).unwrap_or(upper);
            let current = self.lines.lock().unwrap().forget;
            // #966 (0.38 前 review B4): F を進める (削除の記録を忘れる・F を配る) 前に、 外した印と client の一覧を置き場に書いて disk に
            // 届ける (変わっていなければ前に書いた物が disk にある)。 前は忘れた後に fsync なしで書き、 忘れた削除だけが disk に届いて
            // 置き場が空 / 古い像で開き直すと、 外した client が外した印の無い client に戻って publish が当たり、 消えた行が生き返った。
            // 書けなければこの回は進めない
            if f > current && self.save_lines(false) {
                if self.engine.forget_deletes_before(f) > 0 {
                    let _ = self.engine.persist_sync_state();
                    #[cfg(test)]
                    if let Some(hook) = self.forgot_hook.lock().unwrap().as_mut() {
                        hook();
                    }
                }
                self.lines.lock().unwrap().forget = f;
            }
        }
        self.save_lines(false);
    }

    /// client の状態とホストの時計を置き場に書く。 `force` でなければ、 前に書いた中身から変わった時だけ ([`Lines::changed`])。
    /// 書けたか (置き場が無い・書く物が無ければ true)。
    ///
    /// #966 (0.38 前 review B4): 中身を fsync してから rename し (`db_files::write_atomic_if_changed`、 電源断で中身の無い / 途中の
    /// 置き場に置き換わらない)、 directory も fsync して rename を disk に届ける (返った時に disk にある = F を進めてよい)。 書くのは
    /// 1 本ずつ (`save_lock`)、 中身はその下で作る: 先に作った古い中身 (外した印の無い物) が後から rename されて、 後に作った中身を
    /// 上書きしない。 書けた中身だけを 「書いた中身」 にする (前は書く前に置き、 書けなかった中身を次の回も書き直さなかった)。
    fn save_lines(&self, force: bool) -> bool {
        let Some(path) = &self.lines_path else { return true };
        // #966 の読み直し [F6]: 書く物が無ければ書き手の lock を待たない (変わっていない = 前に書けた中身が disk にある。 `saved` は
        // 書けた後にしか変わらない)。 前は他の要求が置き場を fsync している間、 全部の要求の `recompute` の最後がここで待った
        if !force && !self.lines.lock().unwrap().changed() {
            return true;
        }
        let _one = self.save_lock.lock().unwrap_or_else(|p| p.into_inner());
        let snapshot = {
            let l = self.lines.lock().unwrap();
            (force || l.changed()).then(|| (l.clients.clone(), l.forget, l.encode(self.clock.now())))
        };
        let Some((clients, forget, text)) = snapshot else { return true };
        let dir = match path.parent() {
            Some(d) if !d.as_os_str().is_empty() => d,
            _ => std::path::Path::new("."),
        };
        let written = enchudb::db_files::write_atomic_if_changed(path, text.as_bytes())
            .and_then(|_| enchudb::db_files::sync_dir(dir));
        match written {
            Ok(()) => {
                self.lines.lock().unwrap().saved = Some((clients, forget));
                self.save_failing.store(false, Ordering::Release);
                true
            }
            Err(e) => {
                // #966 の読み直し [F6]: 書けない間は F を進めない (呼び手)。 続けて書けない間は 1 回だけ言う
                if !self.save_failing.swap(true, Ordering::AcqRel) {
                    eprintln!(
                        "[enchudb] host: could not write the client state file {} ({e}); not advancing the forget line until it can be written (#966)",
                        path.display()
                    );
                }
                false
            }
        }
    }

    /// client `to` に配る record: ホストの `_sync_ops` のうち、 書き手ごとの cursor より後 (to 自身の record は除く)。
    fn records_for(&self, to: PeerId, since: &[(PeerId, Hlc)]) -> Vec<WireRecord> {
        let cursor = |a: PeerId| since.iter().find(|(p, _)| *p == a).map_or(Hlc::ZERO, |(_, h)| *h);
        // ring の順 (lsn = ホストが当てた順) のまま渡す。 書き手ごとには HLC の昇順 (client は自分の record を HLC の順に送り、
        // ホストはその順に当てる)。 当てる側は届く順に依らない作り。
        // ring を毎回全部読む (100 万行で重い。 書き手ごとの位置を覚えて途中から読むのは後で)
        //
        // #753: state を載せている間は読まない (`POST /v2/state` が `state_gate` を write で持つ)。 state の record は HLC の順に
        // 並ばないので、 途中まで載った所を配ると、 受け手の cursor が、 まだ載っていない (版数の小さい) record を越える。 差分を
        // 当てている受信と F を進める間は待たない (#980)
        //
        // #828: state_gate は ring に写し終えるまで止めるが、 ring が満杯だと bridge は入る所まで写して止まる (#787)。 止まった所が
        // 同じ (書き手, 版数) の組 (state の Tag の cell の Vocab と Tie、 版数の無い cell の `as_of`) の途中だと、 前半だけ受けた
        // client の cursor がその版数に進み、 後から載る後半が `hlc > cursor` を通らない。 組がそろうまでその組は配らない
        // (`Engine::settled_sync_ops`)
        //
        // #949: (書き手, 版数) で絞ってから写す (前は ring の全部の行を写して decode してから絞った)。 絞りは (書き手, 版数) だけで
        // 決まるので、 上の組は割らない
        let _state = self.state_gate.read().unwrap_or_else(|p| p.into_inner());
        self.engine.settled_sync_records(|author, hlc| author != to && hlc > cursor(author))
    }

    /// 古い client (`GET /pull`、 scalar の cursor) に配る record: `records_for(0, &[])` のうち `hlc > since` の物 (書き手 0 は除く)。
    /// #955: 版数で絞ってから写す (前は全部を写して decode してから絞った)。 絞りは (書き手, 版数) だけで決まるので組は割らない。
    fn records_since(&self, since: Hlc) -> Vec<WireRecord> {
        let _state = self.state_gate.read().unwrap_or_else(|p| p.into_inner());
        self.engine.settled_sync_records(|author, hlc| author != 0 && hlc > since)
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
    ///
    /// #798: 外した / 外した後に戻ってきた client の ack は数えない。 数えると消化の位置が置き直って回収の数に戻り
    /// (`forget_sync_peer` の逆)、 その client は取り直して別の peer id に移る = もう来ないのに、 「戻ってきた」 client は二度と
    /// 外れないので、 回収がその位置で止まる。 その client が受け続けるなら、 回収された分は floor で抜けを知って取り直す。
    fn ack_from(&self, by: PeerId, mut cursors: Vec<(PeerId, Hlc)>) {
        if self.is_stale(by) {
            return;
        }
        if let Some(own) = self.syncer.cursors_of(by).into_iter().find(|(a, _)| *a == by) {
            merge_max(&mut cursors, &[own]);
        }
        merge_max(self.loopback.acks_from_clients.lock().unwrap().entry(by).or_default(), &cursors);
        // 回収は ring が半分埋まってから (`absorb_pull_acks`)。 F は ring に残る record を越えないので、 削除の記録を忘れるのも回収の後
        self.syncer.reclaim_beat();
        // 上の確かめの後に `recompute` が by を外した時: 外す側は印を置いてから数から外すので、 ここで印が見えたら外し直す
        // (見えなければ、 外す側が数から外すのはこの後)
        if self.is_stale(by) {
            self.loopback.acks_from_clients.lock().unwrap().remove(&by);
            self.engine.forget_sync_peer(by);
        }
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
    ///
    /// 置き場が無いと、 開き直すたびに、 繋いでいる client は ring の自分の record を全部送り直し (前の ack を覚えている同じ process の
    /// client も、 #961 の `BEHIND` で)、 ホストはそれを全部もう一度中継に載せる (#698: 当たらなかった record も載せる)。 1 client に
    /// つき開き直し 1 回で、 その client の ring の行の数まで。 受け手は cursor で落とすが、 ホストの ring を埋める。 置き場を付けること。
    ///
    /// 控えから戻す時 (#961): 置き場 (`cursor_path`) は DB と同じ時に控えた物か、 それより古い物を使う。 戻す前のホストの置き場 (DB より
    /// 新しい) を残すと、 ホストの cursor が client の言う 「ホストが受けた位置」 以上になって `BEHIND` を返さず、 client の送り直しを
    /// cursor で落とす (控えの後に ack した record が、 戻したホストにも他の client にも届かない)。 置き場を消して開くのは安全な側
    /// (全部の client が送り直す = 上の載せ直し)。
    ///
    /// ホストは publish の要求の query (`acked=peer:w.l.p,..`) を percent-decode せずに読む。 query を作り直す proxy (`:` / `,` を
    /// `%3A` / `%2C` にする等) を挟むと、 読めない `acked` は無いものとして確かめない: 誤って `BEHIND` にはならないが、 作り直した /
    /// 戻したホストに ack 済みの record を送り直さない (#961 (b) が直らない)。
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
        // #966 の読み直し [F6]: 開いた後の最初の回に置き場を書き直す (書いた中身を 「disk にある」 とは扱わない)。 前の binary は置き場を
        // fsync せずに書いたので、 読んだ中身がまだ disk に無いことがある (その中身のまま F を進めて電源が落ちると、 外した印を失う)
        lines.saved = None;
        let clock = HostClock::resume(&lines, now_ms());
        if let Some(p) = cursor_path {
            syncer = syncer.with_cursor_path(p);
        }
        // #964: cursor の置き場は WAL を書き出してから書く (`HostInner::sync_for_ack`)
        syncer.hold_cursor_file(true);
        syncer.set_host(true);
        syncer.serve_state();
        // #798: 前の版が、 外した後に戻ってきた client を回収の数に戻したまま保存している時
        if lines_path.is_some() {
            for (id, _) in lines.clients.iter().filter(|(_, c)| c.dropped || c.returned) {
                engine.forget_sync_peer(*id);
            }
        }
        let inner = Arc::new(HostInner {
            engine,
            syncer,
            loopback,
            client_locks: Mutex::new(HashMap::new()),
            asked_state: Mutex::new(HashSet::new()),
            lines: Mutex::new(lines),
            lines_path,
            clock,
            applying: Mutex::new(Applying::default()),
            forgot: std::sync::Condvar::new(),
            state_gate: RwLock::new(()),
            wal_failed: AtomicU64::new(0),
            wal_cleared: AtomicU64::new(0),
            #[cfg(test)]
            fail_wal: AtomicBool::new(false),
            #[cfg(test)]
            wal_sync_hook: Mutex::new(None),
            #[cfg(test)]
            forget_rounds: AtomicU64::new(0),
            #[cfg(test)]
            forget_hook: Mutex::new(None),
            #[cfg(test)]
            waited: AtomicU64::new(0),
            save_lock: Mutex::new(()),
            save_failing: AtomicBool::new(false),
            #[cfg(test)]
            forgot_hook: Mutex::new(None),
            #[cfg(test)]
            lock_hook: Mutex::new(None),
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
    /// `STALE` を返す (client は新しい peer id の DB に取り直してから書き直す)。 受けるだけの時も、 pull の返事で `STALE` を返す
    /// (record は配る、 #712)。
    ///
    /// 長さは **ホストが動いていた時間**で数える (#713): ホストが止まっていた間 (と、 壁時計が飛んだ分) は client の沈黙に
    /// 数えない。 `cursor_path` の横の置き場に時計を残し、 開き直すと続きから数える。 きれいに止めた (drop した) 時はその時点
    /// まで、 落ちた時は最後に置き場を書いた時点 (client の出入り・外した / 戻った印の変化、 遅くとも 1 時間ごと) までを数える。
    pub fn set_silence_limit(&self, limit: Duration) {
        self.inner.lines.lock().unwrap().silence = Some(limit);
        self.inner.recompute(self.inner.clock.now());
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
        // #713: 止まる時点のホストの時計を残す (開き直すと続きから数える)
        self.inner.save_lines(true);
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
            // #961: client が 「ホストが受けた」 と思っている位置 (0.37.4 以前の client は付けない = 確かめない)
            let acked = q.get("acked").and_then(|s| parse_cursors_param(s));
            // #961: WAL を書き出せなかった = ack を返さない (古い client の道も同じ。 古い client は返事を読まない)
            let Some((flags, ack)) = host.accept(from, records, through, acked) else {
                return send_response(&mut stream, 503, b"oplog_sync");
            };
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
            // 外した client かは、 来たことを記す前に見る (記すと 「戻ってきた」 に変わるが、 どちらも同じ扱い)
            let stale = host.is_stale(to);
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
            if stale {
                out.push(STALE);
            }
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
            send_response(&mut stream, 200, &encode_batch(&host.records_since(since)))
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
            // #699: ホストが求めた state だけ受ける (求めるのは、 その client の publish を当てて履歴が足りなかった時だけ)
            if !host.asked_state.lock().unwrap().remove(&author) {
                return send_response(&mut stream, 409, b"state not requested");
            }
            // 当てて ring に写し終えるまで、 F は state の record を越えない (`applying`、 `accept` と同じ。 state の record の版数は
            // cell の版数 = 古い物も、 当たらない Vocab を載せる版数の `as_of` も見る)
            let lo = state.records.iter().map(|r| r.hlc).min().map_or(state.as_of, |m| m.min(state.as_of));
            let applying = host.begin_apply(Some(lo));
            // #753: 当てて、 載せて、 ring に写し終える (`oplog_sync`) まで pull を止める (`records_for` が `state_gate` の read で待つ)。
            // 差分 (`accept`) の record は書き手ごとに HLC の昇順で載るので止めなくてよいが、 state の record はそう並ばない。
            // #980: 差分の受信は止めない (前は F を進める間と同じ lock の write で止めていた。 止まっている受信の後ろで write を待つと、
            // 後から来た全 client の受信も止まった)
            let state_gate = host.state_gate.write().unwrap_or_else(|p| p.into_inner());
            host.loopback.pushed_state.lock().unwrap().insert(author, state);
            let ok = host.syncer.bootstrap_pull_via(author, author).is_some();
            host.loopback.pushed_state.lock().unwrap().remove(&author);
            // #961: 書き出せなかったら、 次の publish の ack の前に書き出し直す (state で当てた record も cursor を進めている)
            let durable = host.sync_for_ack(true);
            // ring に写し終えた: 返事を書く前に pull を通し、 登録を外す
            drop(state_gate);
            drop(applying);
            send_response(&mut stream, if ok && durable { 200 } else { 500 }, b"")
        }
        _ => send_response(&mut stream, 404, b"not found"),
    }
}

// ─────────────────────────────────────────────────────────────
// client
// ─────────────────────────────────────────────────────────────

/// [`HttpHostTransport`] が覚えている 「ホストが受けた位置」。
#[derive(Default)]
struct Acked {
    /// 書き手ごとの位置。
    at: Cursors,
    /// `BEHIND` の返事で `at` を下げた回数 (review965 F1: 下げる前の位置で集めた batch を送らない)。
    lowered: u64,
}

/// ホストと話す client の transport。 `Syncer::new(engine, Arc::new(HttpHostTransport::connect(url, me)?))` で使い、
/// `publish_since` / `pull_once(t.host_id())` を回す。 自分の state をホストに送れるよう、 client も `serve_state()` を呼ぶこと
/// (ホストが求めた時だけ送る)。
///
/// `Syncer` には cursor の置き場 (`Syncer::with_cursor_path`) を付けること。 無いと受けた位置は memory だけで、 開き直すたびに
/// 「初めて来た client」 になる: ホストが一度でも回収していれば、 最初の pull は何も当てずに `history_truncated` を返し、 アプリが
/// 取り直し (`bootstrap_pull_via`) を呼ぶまで受け取りが止まる (#710)。
///
/// publish は `publish_since(Hlc::ZERO)` で回すこと (ホストが受けた位置は transport が覚えていて、 それ以下の record は集めない)。
/// ZERO より後の `since` を渡すと、 ホストを作り直した / 古い控えから戻した時 (#961、 [`BEHIND`]) に、 ホストに足りない record のうち
/// `since` 以下の物を集めない。 その publish は下がった位置を名乗ってホストに当たるので、 ホストの cursor がその穴を越え、 その
/// record は新しいホストにも他の client にも届かない。
pub struct HttpHostTransport {
    http: HttpTransport,
    host: PeerId,
    /// この client の peer id。
    me: PeerId,
    /// ホストが当てた位置 (publish の返事)。 `take_pull_acks_multi` で `Syncer` に渡す。
    pending_acks: Mutex<HashMap<PeerId, Vec<(PeerId, Hlc)>>>,
    /// 書き手ごとにホストが当てた位置 (送り済みの record を送り直さない)。 ack で上がる。 publish のたびにホストに言い (`acked`)、
    /// ホストの cursor がそこまで届いていなければ (同じ URL の裏でホストを空の DB で作り直した / 古い控えから戻した、 #961)、 ホストの
    /// 返事 (`BEHIND`) の位置に置き換える (下がる)。 0.37.4 以前のホストは `acked` を読まないので、 その相手では上がるだけ (作り直した
    /// ホストに ack 済みの record を、 この process の間は送り直さない)。
    acked: Mutex<Acked>,
    /// thread ごとに、 `publish_floor` を答えた時の [`Acked::lowered`]。 同じ thread の次の `publish_to` が取り出して比べる (review965
    /// F1)。 `Syncer` の外から `publish_floor` だけを呼んだ thread も控えが残り、 その間に下げていれば次の `publish_to` を 1 回送らない
    /// (次の beat が送る)。
    floor_reads: Mutex<HashMap<ThreadId, u64>>,
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
            acked: Mutex::new(Acked::default()),
            floor_reads: Mutex::new(HashMap::new()),
            floors: Mutex::new(None),
            own_state: Mutex::new(HashMap::new()),
            through: Mutex::new((None, None)),
            prev_answer: Mutex::new(None),
            forget: Mutex::new(Hlc::ZERO),
            rebase: AtomicBool::new(false),
        })
    }

    /// ホストがこの client を外した後に戻ってきた (黙っていたので、 線と回収の数から外された)。 新しい peer id の DB にホストから
    /// 取り直してから書き直すこと (この peer id の record はもう受けない。 受けると、 忘れた削除を越えて消えた行が生き返りうる)。
    /// 自分の record を出した時 (publish の返事) だけでなく、 受けた時 (pull の返事) にも立つ (#712): 受けるだけの client も、
    /// 留守の間に消えた行を持ったままなので、 この DB の中身はホストと揃わない。
    pub fn rebase_required(&self) -> bool {
        self.rebase.load(Ordering::Acquire)
    }

    /// ホストの peer id (`pull_once` に渡す)。
    pub fn host_id(&self) -> PeerId {
        self.host
    }

    /// 送らずに捨てる位置: ホストが受けた位置 (`acked`) の写しと、 その時の下げた回数 ([`Acked::lowered`])。 `publish_to` の捨て
    /// ([`above_floor`]) と `publish_floor` はどちらもこれから作る (#962: `Transport::publish_floor` の約束 = 言う位置以下の record は
    /// 送らない、 を形で揃える)。
    fn sent_floor(&self) -> (Cursors, u64) {
        let acked = self.acked.lock().unwrap();
        (acked.at.clone(), acked.lowered)
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
        // review965 F1: この thread が record を集める前に `publish_floor` を聞いた後、 並んだ publish が `BEHIND` を受けて覚えを
        // 下げていたら、 この batch は送らない。 batch は下げる前の位置より後の record だけで、 送ると下がった位置を名乗ってホストに
        // 当たり、 ホストの cursor が間の record (下がった位置と下げる前の位置の間) を越える (ack で覚えも上がり、 その record を二度と
        // 集めない)。 次の beat が下がった位置から集め直す (配り終えの線もその beat が言う)。 `publish_floor` を聞いていない publish
        // (`Syncer` が絞らずに `since` から集めた batch) は確かめない
        let read_at = self.floor_reads.lock().unwrap().remove(&std::thread::current().id());
        let (floor, lowered) = self.sent_floor();
        if read_at.is_some_and(|l| l != lowered) {
            return;
        }
        let records: Vec<WireRecord> = records.into_iter().filter(|r| above_floor(&floor, r)).collect();
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
        // #961: ホストが受けたと思っている位置を言う (ホストの cursor が届いていなければ、 ホストは当てずに `BEHIND` と今の位置を返す)
        if from == self.me && !floor.is_empty() {
            path.push_str(&format!("&acked={}", cursors_param(&floor, from)));
        }
        let Ok((200, body)) = self.http.request("POST", &path, &encode_batch(&records)) else { return };
        let (Some(&flags), Some(ack)) = (body.first(), body.get(1..).and_then(decode_cursors)) else { return };
        if flags & STALE != 0 {
            self.rebase.store(true, Ordering::Release);
            return;
        }
        if flags & BEHIND != 0 {
            // #961: ホストは言った位置まで受けていない (作り直した / 古い控えから戻した)。 この batch は当たっていない。 覚えをホストの
            // 今の位置に置き換え (`publish_floor` / 捨ての位置が下がる)、 次の beat で足りない所から送り直す。 下げた回数を上げて、
            // 下げる前の位置で集めた並んだ publish の batch を送らせない (review965 F1)。 配り終えの線は送っていないことにする (次の
            // publish でまた言う)。
            // まだ `Syncer` に渡していない ack は捨てる。 これで止まるのは、 並べて回した publish がこの beat の頭より後に受けた ack
            // だけ (新しいホストの ack なら、 次の ack が同じ位置以上を運ぶ)。 前のホストの最後の ack は beat の頭 (`absorb_pull_acks`)
            // で取り込み済みで、 client の ring はその ack で回収されうる (review965 F2: 止めない)。 新しいホストが持たない record を
            // 回収しても失わない: 回収した beat の頭で floor を言ってから送るので、 ホストは送り直しの代わりに state を求める (差分より
            // 重いだけ)
            eprintln!(
                "[enchudb] host {} has not received records it acknowledged before (rebuilt or restored?) — sending them again",
                self.host
            );
            {
                let mut acked = self.acked.lock().unwrap();
                acked.at = ack;
                acked.lowered += 1;
            }
            self.pending_acks.lock().unwrap().remove(&from);
            return;
        }
        if from == self.me {
            self.through.lock().unwrap().1 = through;
        }
        merge_max(&mut self.acked.lock().unwrap().at, &ack);
        merge_max(self.pending_acks.lock().unwrap().entry(from).or_default(), &ack);
        if flags & NEED_STATE != 0 {
            self.push_state(from);
        }
    }

    /// #955: `publish_to` が捨てる位置 = ホストが受けた位置 (`acked`、 ack の `merge_max` で上がるので、 この後の `publish_to` が捨てる
    /// 位置以下)。 `Syncer` は先に聞いて、 ホストが受けた record を集めない (since = ZERO の publish が ack 済みの行を写して decode しない)。
    /// #962: `publish_to` の捨てと同じ [`HttpHostTransport::sent_floor`] から作る。
    ///
    /// #961: 下がるのは `BEHIND` の返事を読んだ時だけ (ホストを作り直した)。 その返事は publish の最後に読むので、 下がった位置は次の
    /// beat の `publish_floor` が言う。 同じ transport の publish を並べて回すと、 間の beat が下がる前の位置で集めうる: その batch は
    /// `publish_to` が送らない (review965 F1。 送ると下がった位置を名乗って当たり、 ホストの cursor が間の record を越える)。 答えた時の
    /// 下げた回数を thread ごとに控え、 同じ thread の次の `publish_to` が比べる (`Syncer` は `publish_floor` → 集める → `publish_to` を
    /// 1 本の thread で呼ぶ)。 送らなかった batch の record は次の beat が下がった位置から集める。
    fn publish_floor(&self, _from: PeerId, _to: PeerId) -> Option<Vec<(PeerId, Hlc)>> {
        let (at, lowered) = self.sent_floor();
        self.floor_reads.lock().unwrap().insert(std::thread::current().id(), lowered);
        Some(at)
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
        // 後ろの 1 byte は印 (#712、 外された後に戻った時だけ付く)
        let (publish_line, forget, flags) = match rest.len() {
            32 => (get_hlc(&rest[..16]), get_hlc(&rest[16..]), 0),
            33 => (get_hlc(&rest[..16]), get_hlc(&rest[16..32]), rest[32]),
            _ => (Hlc::ZERO, Hlc::ZERO, 0),
        };
        if flags & STALE != 0 {
            self.rebase.store(true, Ordering::Release);
        }
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

    /// #712: pull の返事の後ろの印。 `STALE` が付いていれば `rebase_required` が立ち、 線は今までどおり読む。 印の無い返事
    /// (今までの長さ) では立たない。
    #[test]
    fn a_stale_mark_on_a_pull_answer_sets_rebase_required() {
        let h = |w| Hlc { wall: w, logical: 0, peer: 0 };
        for (mark, want) in [(Some(STALE), true), (Some(0), false), (None, false)] {
            let mut answer = encode_framed(&[]);
            answer.extend_from_slice(&encode_cursors(&[]));
            put_hlc(&mut answer, h(40));
            put_hlc(&mut answer, h(30));
            answer.extend(mark);
            let (addr, server) = fake_host(answer);
            let t = HttpHostTransport::connect(format!("http://{addr}"), 1).unwrap();
            assert!(!t.rebase_required());
            t.pull_as_multi(1, 7, &[]);
            server.join().unwrap();
            assert_eq!(t.rebase_required(), want, "印 {mark:?}");
            assert_eq!(t.forget_horizon(7), Some(h(30)), "印 {mark:?}: 忘れてよい線を読めていない");
        }
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

    /// #713: ホストの時計は置き場の値の続きから進む (止まっていた間を数えない)。 壁時計が 60 日進んでいても、 止まる直前に来た
    /// client は黙っていない。
    #[test]
    fn the_host_clock_resumes_from_the_saved_value() {
        let day = 24 * 60 * 60 * 1000u64;
        let stopped_at = 1_790_000_000_000u64;
        let wall = stopped_at + 60 * day;
        let mut l = Lines::default();
        l.clients.insert(1, ClientState { last_seen_ms: stopped_at - 1_000, ..ClientState::default() });
        l.clients.insert(2, ClientState { last_seen_ms: stopped_at - 31 * day, ..ClientState::default() });
        // 置き場を通す: 時計の行が残り、 知らない行として読み飛ばされない
        let back = Lines::decode(&l.encode(stopped_at));
        assert_eq!(back.clock_ms, Some(stopped_at));
        assert_eq!(back.clients, l.clients);
        let now = HostClock::resume(&back, wall).now();
        assert!((stopped_at..stopped_at + 60_000).contains(&now), "時計が置き場の続きでない: {now}");
        assert!(back.counted(&back.clients[&1], now), "止まっていた間を沈黙に数えた");
        assert!(!back.counted(&back.clients[&2], now), "止まる前から上限より長く黙っていた client を数えた");
        // 前提: 壁時計で数えると、 止まる直前に来た client も黙ったことになる (直す前の形)
        assert!(!back.counted(&back.clients[&1], wall));

        // この版より前のホストが書いた置き場 (`T` の行なし、 last_seen は壁時計): いちばん最近来た client の時刻から
        let old = Lines::decode(&format!("F 0.0.0\nC 1 {} - 0.0.0 0 0\nC 2 {} - 0.0.0 0 0\n", stopped_at - 1_000, stopped_at - 5_000));
        assert_eq!(old.clock_ms, None);
        let now = HostClock::resume(&old, wall).now();
        assert!((stopped_at - 1_000..stopped_at + 59_000).contains(&now), "{now}");
        // client の居ない / 置き場の無いホスト: 今の壁時計から
        let now = HostClock::resume(&Lines::default(), wall).now();
        assert!((wall..wall + 60_000).contains(&now), "{now}");
    }

    /// #949: client に配る record (`records_for`) が、 直す前の読み (配ってよい行の payload を全部写して decode し、 宛先の client
    /// 自身の record と書き手ごとの cursor 以下を外す) と同じ record を同じ並びで出す。 ring の書き手は 3 人 (ホスト自身の Tie と、
    /// 中継した 2 人の Vocab。 中継した版数はホストの版数の前と後に散る)。 宛先と cursor の組をいくつも。
    #[test]
    fn records_for_matches_the_full_copy() {
        let path = std::env::temp_dir().join(format!("enchudb-949-host-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        {
            let mut eng = Engine::create_standalone(path.to_str().unwrap()).unwrap();
            eng.define_table("rows", 100).unwrap();
            eng.define_himo_in("rows", "val", enchudb::ValueType::Number, 0).unwrap();
            eng.enable_sync_tables().unwrap();
            eng.flush().unwrap();
        }
        let eng = Engine::open_concurrent_with_oplog(path.to_str().unwrap(), 8 << 20).unwrap();
        eng.set_peer_id(1);
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
        let rows: Vec<_> = (0..10).map(|_| eng.entity_in("rows").unwrap()).collect();
        for i in 0..240u32 {
            if i % 3 == 0 {
                let author = if i % 2 == 0 { 7 } else { 9 };
                let wall = if i % 5 < 2 { 100 + u64::from(i) } else { now + 1_000_000 + u64::from(i % 11) };
                let rec = WireRecord::unsigned(
                    Hlc { wall, logical: i % 4, peer: author },
                    author,
                    DecodedOp::Vocab { vid: i, bytes: format!("r{i}").into_bytes() },
                );
                assert!(eng.relay_record(&rec), "前提: 中継した record を WAL に載せた");
            } else {
                eng.tie_async(rows[(i % 10) as usize], "rows.val", i);
            }
        }
        // #955: 書き手 0 の record (`records_for(0, ..)` と `GET /pull` は外す)
        let zero = WireRecord::unsigned(Hlc { wall: now + 2_000_000, logical: 0, peer: 0 }, 0, DecodedOp::Vocab { vid: 9999, bytes: b"z".to_vec() });
        assert!(eng.relay_record(&zero), "前提: 書き手 0 の record を WAL に載せた");
        eng.oplog_commit();
        eng.oplog_sync().unwrap();
        while eng.transfer_oplog_to_sync_ops() > 0 {}
        let host = HttpHost::start("127.0.0.1:0", eng.clone(), None).unwrap();
        type Key = (Hlc, PeerId, [u8; 64], [u8; 8], Vec<u8>);
        let key = |r: &WireRecord| -> Key { (r.hlc, r.author_peer, r.signature, r.pubkey_fp, r.signed_bytes.clone()) };
        // 直す前の読み
        let old = |to: PeerId, since: &[(PeerId, Hlc)]| -> Vec<Key> {
            let cursor = |a: PeerId| since.iter().find(|(p, _)| *p == a).map_or(Hlc::ZERO, |(_, h)| *h);
            eng.settled_sync_ops()
                .iter()
                .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
                .map(WireRecord::from)
                .filter(|r| r.author_peer != to && r.hlc > cursor(r.author_peer))
                .map(|r| key(&r))
                .collect()
        };
        let all = old(0, &[]);
        let mut hlcs: Vec<Hlc> = all.iter().map(|k| k.0).collect();
        assert!(hlcs.windows(2).any(|w| w[0] > w[1]), "前提: ring の並び (lsn) と版数の並びがずれている");
        hlcs.sort();
        let mut cuts = vec![Hlc::ZERO];
        cuts.extend(hlcs.iter().step_by(hlcs.len() / 6).copied());
        cuts.push(hlcs[hlcs.len() - 1]);
        let mut partial = 0;
        for to in [0, 1, 7, 9, 42] {
            for (i, &c) in cuts.iter().enumerate() {
                let c2 = cuts[(i * 3 + 1) % cuts.len()];
                for since in [vec![], vec![(1, c)], vec![(1, c), (7, c2), (9, c)], vec![(9, c2), (7, c)]] {
                    let want = old(to, &since);
                    let got: Vec<Key> = host.inner.records_for(to, &since).iter().map(key).collect();
                    assert!(got == want, "to {to} since {since:?}: 直す前の読みと違う ({} / {} 件)", got.len(), want.len());
                    if !want.is_empty() && want.len() < all.len() {
                        partial += 1;
                    }
                }
            }
        }
        assert!(partial > 20, "前提: 一部だけ配る組がある ({partial})");
        // #955: 古い client の `GET /pull` (scalar の cursor) も、 直す前の読み (`records_for(0, &[])` を全部写して絞る) と同じ
        let mut part = 0;
        for &c in &cuts {
            let want: Vec<Key> = old(0, &[]).into_iter().filter(|k| k.0 > c).collect();
            let got: Vec<Key> = host.inner.records_since(c).iter().map(key).collect();
            assert!(got == want, "GET /pull since {c:?}: 直す前の読みと違う ({} / {} 件)", got.len(), want.len());
            part += usize::from(!want.is_empty() && want.len() < all.len());
        }
        assert!(part >= 3, "前提: GET /pull で一部だけ配る since がある ({part})");
        assert!(old(1, &[]).iter().any(|k| k.1 == 0), "前提: ring に書き手 0 の record がある");
        drop(host);
        drop(eng);
        let _ = std::fs::remove_dir_all(&path);
    }

    /// #961: ホストが ack を返す前の WAL の書き出し (`oplog_sync`) に失敗したら、 ack を返さない (client は受けたと思わずに次の beat で
    /// 送り直す)。 送り直した record はホストの cursor が越えているので当たらないが、 書き出せるまでは、 その送り直しにも ack を返さない
    /// (失敗した回に載せた record は書き出していない)。 書き出せた後は ack を返す。
    #[test]
    fn a_failed_oplog_sync_does_not_ack() {
        let dir = std::env::temp_dir().join(format!("enchudb-961-host-ack-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mk = |name: &str, peer: PeerId| {
            let path = dir.join(name).to_str().unwrap().to_string();
            {
                let mut eng = Engine::create_standalone(&path).unwrap();
                eng.define_table("rows", 100).unwrap();
                eng.define_himo_in("rows", "val", enchudb::ValueType::Number, 0).unwrap();
                eng.enable_sync_tables().unwrap();
                eng.flush().unwrap();
            }
            let eng = Engine::open_concurrent_with_oplog(&path, 8 << 20).unwrap();
            eng.set_peer_id(peer);
            eng
        };
        let host_eng = mk("host", 100);
        let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
        let c = mk("c1", 1);
        let t = Arc::new(HttpHostTransport::connect(format!("http://{}", host.addr()), 1).unwrap());
        let s = Syncer::new(c.clone(), t.clone());
        let row = c.entity_in("rows").unwrap();
        c.tie_to(row, "rows.val", 42);
        c.oplog_commit();
        c.oplog_sync().unwrap();
        while c.transfer_oplog_to_sync_ops() > 0 {}
        let top = c.settled_sync_records(|a, _| a == 1).iter().map(|r| r.hlc).max();
        assert!(top.is_some(), "前提: client の ring に自分の record がある");
        let acked = |t: &HttpHostTransport| t.sent_floor().0.iter().find(|(a, _)| *a == 1).map(|(_, h)| *h);
        let pending = |t: &HttpHostTransport| t.pending_acks.lock().unwrap().values().any(|v| !v.is_empty());

        host.inner.fail_wal.store(true, Ordering::Release);
        s.publish_since(Hlc::ZERO);
        assert_eq!(host_eng.pull_raw("rows.val", 42).len(), 1, "前提: ホストに届いて当たった");
        assert!(
            host.syncer().cursors_of(1).iter().any(|&(a, h)| a == 1 && Some(h) == top),
            "前提: ホストの cursor は当てた record を越えた (送り直しは当たらない)"
        );
        assert_eq!(acked(&t), None, "WAL を書き出せなかったのに ack を返した");
        assert!(!pending(&t), "WAL を書き出せなかったのに、 client の ring を回収する ack を渡した");

        // 送り直す (ack 済みと思っていない)。 書き出せないままなら、 当たらない送り直しにも ack を返さない
        s.publish_since(Hlc::ZERO);
        assert_eq!(acked(&t), None, "送り直しに、 書き出していない record の ack を返した");
        assert!(!pending(&t));

        host.inner.fail_wal.store(false, Ordering::Release);
        s.publish_since(Hlc::ZERO);
        assert_eq!(acked(&t), top, "書き出せた後も ack を返さない");
        drop(s);
        drop(t);
        drop(host);
        drop(host_eng);
        drop(c);

        // client が送った state (ホストが求めた、 #699) を当てた後の書き出しに失敗した時も同じ: state はホストの cursor を as_of に
        // 進めるので、 次の publish の ack は state の record を覆う。 書き出せるまでは返さない
        let host_eng = mk("host2", 100);
        let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
        let c = mk("c2", 1);
        let t = Arc::new(HttpHostTransport::connect(format!("http://{}", host.addr()), 1).unwrap());
        let s = Syncer::new(c.clone(), t.clone());
        s.serve_state();
        let settle = |c: &Engine| {
            c.oplog_commit();
            c.oplog_sync().unwrap();
            while c.transfer_oplog_to_sync_ops() > 0 {}
        };
        let row = c.entity_in("rows").unwrap();
        c.tie_to(row, "rows.val", 1);
        settle(&c);
        // ホストに出す前に回収した (ホストの cursor は client の floor に届かない = ホストは state を求める)
        c.ack_sync(100, c.current_sync_lsn() + 1).unwrap();
        assert!(c.reclaim_sync_ops() > 0, "前提: client の ring を回収した");
        let row = c.entity_in("rows").unwrap();
        c.tie_to(row, "rows.val", 2);
        settle(&c);
        host.inner.fail_wal.store(true, Ordering::Release);
        s.publish_since(Hlc::ZERO);
        assert_eq!(
            [host_eng.pull_raw("rows.val", 1).len(), host_eng.pull_raw("rows.val", 2).len()],
            [1, 1],
            "前提: ホストが state を求めて当てた"
        );
        let as_of = host.syncer().cursors_of(1).iter().find(|(a, _)| *a == 1).map(|(_, h)| *h);
        let top = c.settled_sync_records(|a, _| a == 1).iter().map(|r| r.hlc).max();
        assert!(as_of.is_some() && as_of >= top, "前提: state でホストの cursor が client の record を越えた ({as_of:?} / {top:?})");
        assert_eq!(acked(&t), None, "前提: state の返事は ack を運ばない");
        s.publish_since(Hlc::ZERO);
        assert_eq!(acked(&t), None, "state を書き出せなかったのに、 次の publish で ack を返した");
        assert!(!pending(&t));
        host.inner.fail_wal.store(false, Ordering::Release);
        s.publish_since(Hlc::ZERO);
        assert_eq!(acked(&t), as_of, "書き出せた後も ack を返さない");
        drop(s);
        drop(t);
        drop(host);
        drop(host_eng);
        drop(c);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 試験の DB: 表 `rows` (Number の `val`)、 sync の表、 WAL 付き。
    fn rows_db(path: &str, peer: PeerId) -> Arc<Engine> {
        {
            let mut eng = Engine::create_standalone(path).unwrap();
            eng.define_table("rows", 100).unwrap();
            eng.define_himo_in("rows", "val", enchudb::ValueType::Number, 0).unwrap();
            eng.enable_sync_tables().unwrap();
            eng.flush().unwrap();
        }
        let eng = Engine::open_concurrent_with_oplog(path, 8 << 20).unwrap();
        eng.set_peer_id(peer);
        eng
    }

    /// 行を 1 つ書いて ring に載せ、 自分の record の版数の最大を返す。
    fn write_row(c: &Engine, v: u32) -> Hlc {
        let row = c.entity_in("rows").unwrap();
        c.tie_to(row, "rows.val", v);
        c.oplog_commit();
        c.oplog_sync().unwrap();
        while c.transfer_oplog_to_sync_ops() > 0 {}
        let me = c.peer_id();
        c.settled_sync_records(|x, _| x == me).iter().map(|r| r.hlc).max().unwrap()
    }

    /// ホストの ring に残る record の版数の最小を、 ring の全部の行を写して decode して読む (#985 の覚え
    /// (`Engine::sync_ring_min_hlc`) と独立の読み。 試験の前提はこれで見る)。
    fn full_ring_min(e: &Engine) -> Option<Hlc> {
        e.pending_sync_ops(0).iter().filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p)).map(|r| r.hlc).min()
    }

    /// cursor の置き場の中身 `text` にある (link, 書き手) の位置 (無ければ ZERO)。
    fn cursor_in(text: &str, link: PeerId, author: PeerId) -> Hlc {
        text.lines()
            .find_map(|l| match l.split_whitespace().collect::<Vec<_>>().as_slice() {
                [l, a, w, lo, p] if l.parse() == Ok(link) && a.parse() == Ok(author) => {
                    Some(Hlc { wall: w.parse().ok()?, logical: lo.parse().ok()?, peer: p.parse().ok()? })
                }
                _ => None,
            })
            .unwrap_or(Hlc::ZERO)
    }

    fn cursor_file(path: &str, link: PeerId, author: PeerId) -> Hlc {
        cursor_in(&std::fs::read_to_string(path).unwrap_or_default(), link, author)
    }

    /// #964: ホストは client ごとの cursor の置き場を、 当てた record を WAL に書き出した**後**に書く。
    ///
    /// 電源断の像: client a の 2 つ目の行 (k=2) を当てた後、 WAL の書き出しが返る前に電源が落ちた。 DB は k=1 まで (k=2 の前に
    /// 取った控え = k=2 の record は WAL ごと disk に届かなかった)、 cursor の置き場は落ちた時点で disk にある中身 (OS が先に書いた
    /// 最悪の形)。 書き出しは返らないので a に ack は無い。 その像でホストを同じ addr に立て直すと、 a の送り直した k=2 がホストと
    /// client b に届く。 前は置き場を当てた時 (`pull_once` の中) に書いたので、 像の置き場は k=2 を受けたと言い、 立て直したホストは
    /// 送り直しを cursor で落とした。
    #[test]
    fn the_cursor_file_does_not_run_ahead_of_the_wal() {
        let dir = std::env::temp_dir().join(format!("enchudb-964-host-cursor-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = |name: &str| dir.join(name).to_str().unwrap().to_string();
        let cursors = path("host.cursors");
        let host_eng = rows_db(&path("host"), 100);
        let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(cursors.clone().into())).unwrap();
        let addr = host.addr().to_string();
        let a = rows_db(&path("a"), 1);
        let ta = Arc::new(HttpHostTransport::connect(format!("http://{addr}"), 1).unwrap());
        let sa = Syncer::new(a.clone(), ta.clone());
        let b = rows_db(&path("b"), 2);
        let sb = Syncer::new(b.clone(), Arc::new(HttpHostTransport::connect(format!("http://{addr}"), 2).unwrap()));
        let top1 = write_row(&a, 1);
        sa.publish_since(Hlc::ZERO);
        assert_eq!(host_eng.pull_raw("rows.val", 1).len(), 1, "前提: k=1 がホストに届いた");
        assert_eq!(cursor_file(&cursors, 1, 1), top1, "前提: k=1 を受けた位置が置き場にある");
        // k=2 の前の DB の像
        host_eng.oplog_sync().unwrap();
        host_eng.snapshot_export(&path("img")).unwrap();

        // 割り込み点: k=2 を当てた後、 ack の前の WAL の書き出しの直前。 その時点の置き場の中身を控え、 書き出しは返らない (失敗)
        let at_point = Arc::new(Mutex::new(None));
        {
            let (at_point, cursors) = (at_point.clone(), cursors.clone());
            *host.inner.wal_sync_hook.lock().unwrap() = Some(Box::new(move |after| {
                let mut g = at_point.lock().unwrap();
                if !after && g.is_none() {
                    *g = Some(std::fs::read_to_string(&cursors).unwrap_or_default());
                }
            }));
        }
        host.inner.fail_wal.store(true, Ordering::Release);
        let top2 = write_row(&a, 2);
        sa.publish_since(Hlc::ZERO);
        assert_eq!(host_eng.pull_raw("rows.val", 2).len(), 1, "前提: k=2 はホストに当たった (書き出しの前)");
        assert!(
            host.syncer().cursors_of(1).iter().any(|&(x, h)| x == 1 && h == top2),
            "前提: ホストの memory の cursor は k=2 を越えた"
        );
        let seen = cursor_in(&at_point.lock().unwrap().clone().expect("前提: 割り込み点を通った"), 1, 1);
        assert!(ta.sent_floor().0.iter().all(|(x, h)| *x != 1 || *h < top2), "前提: a は k=2 の ack を受けていない");
        // 電源が落ちた: 置き場は今 disk にある中身
        std::fs::copy(&cursors, path("img.cursors")).unwrap();
        drop(host);
        drop(host_eng);

        // 像で同じ addr に立て直す
        let h2 = Engine::open_concurrent_with_oplog(&path("img"), 8 << 20).unwrap();
        h2.set_peer_id(100);
        assert_eq!(h2.pull_raw("rows.val", 2).len(), 0, "前提: 像の DB に k=2 は無い");
        let host = HttpHost::start(&addr, h2.clone(), Some(path("img.cursors").into())).unwrap();
        for _ in 0..3 {
            sa.publish_since(Hlc::ZERO);
        }
        for _ in 0..2 {
            sb.pull_once(100);
        }
        let got = [h2.pull_raw("rows.val", 2).len(), b.pull_raw("rows.val", 1).len(), b.pull_raw("rows.val", 2).len()];
        let saved = cursor_file(&path("img.cursors"), 1, 1);
        drop(host);
        drop(h2);
        drop((sa, ta, sb));
        drop((a, b));
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(got, [1, 1, 1], "[立て直したホストの k=2, b の k=1, k=2] (書き出しの前に置き場が受けたと言った record が届かない)");
        assert!(seen < top2, "WAL の書き出しの前に、 置き場が k=2 を受けたと言った ({seen:?} >= {top2:?})");
        assert_eq!(saved, top2, "書き出せた後も置き場に位置を書かない");
    }

    /// #964: 置き場に書くのは、 書き出しを**始める前**の cursor の写し。 書き出しの間に別の client (c) の record を当てて cursor が
    /// 進んでも、 その位置はこの書き出しの後に書かない (c の record はこの書き出しが覆っていない。 c の分は c の ack の前の書き出しが
    /// 書く)。
    #[test]
    fn the_cursor_file_takes_the_cursors_from_before_the_wal_sync() {
        let dir = std::env::temp_dir().join(format!("enchudb-964-host-mark-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = |name: &str| dir.join(name).to_str().unwrap().to_string();
        let cursors = path("host.cursors");
        let host_eng = rows_db(&path("host"), 100);
        let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(cursors.clone().into())).unwrap();
        let a = rows_db(&path("a"), 1);
        let sa = Syncer::new(a.clone(), Arc::new(HttpHostTransport::connect(format!("http://{}", host.addr()), 1).unwrap()));
        let c = rows_db(&path("c"), 3);
        let top_c = write_row(&c, 30);
        let recs_c = c.settled_sync_records(|x, _| x == 3);
        // a の ack の前の書き出しが返った直後 (置き場を書く前) に、 c の record を当てる (c の publish が並んで当たった形)
        {
            let inner = Arc::downgrade(&host.inner);
            let mut recs_c = Some(recs_c);
            *host.inner.wal_sync_hook.lock().unwrap() = Some(Box::new(move |after| {
                if let (true, Some(inner)) = (after, inner.upgrade())
                    && let Some(recs) = recs_c.take()
                {
                    inner.loopback.inbox.lock().unwrap().insert(3, recs);
                    inner.syncer.pull_once(3);
                    inner.loopback.inbox.lock().unwrap().remove(&3);
                }
            }));
        }
        let top_a = write_row(&a, 1);
        sa.publish_since(Hlc::ZERO);
        let applied = [host_eng.pull_raw("rows.val", 1).len(), host_eng.pull_raw("rows.val", 30).len()];
        let mem_c = host.syncer().cursors_of(3).iter().find(|(x, _)| *x == 3).map_or(Hlc::ZERO, |(_, h)| *h);
        let file = [cursor_file(&cursors, 1, 1), cursor_file(&cursors, 3, 3)];
        *host.inner.wal_sync_hook.lock().unwrap() = None;
        drop(host);
        drop(host_eng);
        drop((sa, a, c));
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(applied, [1, 1], "前提: a と c の行がホストに当たった");
        assert_eq!(mem_c, top_c, "前提: ホストの memory の cursor は c の record を越えた");
        assert_eq!(file[0], top_a, "書き出しの前に当てた a の位置を置き場に書かない");
        assert!(file[1] < top_c, "書き出しの後に当てた c の位置を、 その書き出しの後に書いた ({:?})", file[1]);
    }

    /// `cond` が真になるまで待つ (上限 `limit`)。 真になったか。
    fn wait_until(limit: Duration, mut cond: impl FnMut() -> bool) -> bool {
        let end = std::time::Instant::now() + limit;
        while !cond() {
            if std::time::Instant::now() > end {
                return false;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        true
    }

    /// #980: client a の受信 (`accept`) がホストの行の lock を待つ間 (アプリが同じ行を `write_row` で握っている)、 client b の要求が
    /// 忘れてよい線を進めようとしても、 client c の publish と pull は返る。 d の state も当たる。 a が当て続けている間も、 他の client が
    /// 線を上げれば F は進む (a は数える client なので、 a の record を越えないのは a 自身の線が候補を抑えるから。 登録が F を抑える
    /// 形は `the_forget_line_does_not_pass_a_record_being_applied` / `the_forget_line_reads_the_ring_after_the_registrations`)。
    ///
    /// 直す前: a は `gate` の read を持ったまま行を待ち、 b の要求の `touch` → `recompute` が `gate.write()` で待つ。 std の `RwLock`
    /// は書き手が待つ間に新しい読み手を通さないので、 c の `accept` (read) も pull の `records_for` (read) も、 アプリが行を放すまで
    /// 返らなかった。 置き場 (`cursor_path`) のあるホストでは、 ring に残る record が F を止めている間 (`ring_min`) は要求のたびに
    /// F を進めようとする = 普通の運用の形。
    #[test]
    fn a_receive_waiting_for_a_row_does_not_stall_other_clients() {
        use std::sync::mpsc::channel;
        let dir = std::env::temp_dir().join(format!("enchudb-980-host-gate-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = |name: &str| dir.join(name).to_str().unwrap().to_string();
        let host_eng = rows_db(&path("host"), 100);
        let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(path("host.cursors").into())).unwrap();
        let url = format!("http://{}", host.addr());
        let client = |name: &str, peer: PeerId| {
            let e = rows_db(&path(name), peer);
            let s = Arc::new(Syncer::new(e.clone(), Arc::new(HttpHostTransport::connect(url.clone(), peer).unwrap())));
            (e, s)
        };
        let ((a, sa), (b, sb), (c, sc), (d, sd)) = (client("a", 1), client("b", 2), client("c", 3), client("d", 4));
        sd.serve_state();
        let r = a.entity_in("rows").unwrap();
        a.tie_to(r, "rows.val", 1);
        let settle = |e: &Engine| {
            e.oplog_commit();
            e.oplog_sync().unwrap();
            while e.transfer_oplog_to_sync_ops() > 0 {}
        };
        settle(&a);
        write_row(&b, 20);
        write_row(&c, 30);
        // d はホストに出す前に自分の ring を回収した (ホストの cursor が d の floor に届かない = 最初の publish でホストが state を求める)
        write_row(&d, 41);
        d.ack_sync(100, d.current_sync_lsn() + 1).unwrap();
        assert!(d.reclaim_sync_ops() > 0, "前提: d の ring を回収した");
        write_row(&d, 42);
        // 全員が配り終えの線と受け終えの線を言う
        for _ in 0..3 {
            for s in [&sa, &sb, &sc] {
                s.publish_since(Hlc::ZERO);
                s.pull_once(100);
            }
        }
        let wire_r = a
            .settled_sync_records(|x, _| x == 1)
            .iter()
            .find_map(|rec| match rec.op {
                DecodedOp::Tie { eid, .. } => Some(eid),
                _ => None,
            })
            .expect("前提: a の ring に r の Tie がある");
        let local_r = host_eng.resolve_remote_eid_existing(wire_r).expect("前提: ホストに r が当たった");
        let forget_before = host.forget_line();
        assert!(
            host.inner.lines.lock().unwrap().forget_candidate(host.inner.clock.now()).is_some(),
            "前提: 次の要求は忘れてよい線を進めようとする (F {forget_before:?})"
        );

        // アプリがホストの r の行を握る。 a が r を書き換えて出すと、 ホストの受信は r の行を待つ
        a.tie_to(r, "rows.val", 2);
        settle(&a);
        let top_a = a.settled_sync_records(|x, _| x == 1).iter().map(|rec| rec.hlc).max().unwrap();
        let held = host_eng.write_row(local_r);
        let (a_tx, a_rx) = channel::<()>();
        {
            let sa = sa.clone();
            std::thread::spawn(move || {
                sa.publish_since(Hlc::ZERO);
                let _ = a_tx.send(());
            });
        }
        // a の受信が当てる間に入った (登録した。 r の行を握れないので、 放すまでそこに居る)
        let applying = || host.inner.applying.lock().unwrap().live.len();
        let a_inside = wait_until(Duration::from_secs(10), || applying() == 1);
        // b の要求 (pull = `touch` → `recompute`)。 返り、 その `recompute` が F を忘れる段に入ったか
        let rounds_before = host.inner.forget_rounds.load(Ordering::Acquire);
        let (b_tx, b_rx) = channel::<()>();
        {
            let sb = sb.clone();
            std::thread::spawn(move || {
                sb.pull_once(100);
                let _ = b_tx.send(());
            });
        }
        let b_done = b_rx.recv_timeout(Duration::from_secs(10)).is_ok();
        let b_reached = b_done && host.inner.forget_rounds.load(Ordering::Acquire) > rounds_before;
        // c が 1 行書いて出し、 pull する
        write_row(&c, 31);
        let (c_tx, c_rx) = channel::<()>();
        {
            let sc = sc.clone();
            std::thread::spawn(move || {
                sc.publish_since(Hlc::ZERO);
                sc.pull_once(100);
                let _ = c_tx.send(());
            });
        }
        let c_returned = c_rx.recv_timeout(Duration::from_secs(10)).is_ok();
        let c_applied = host_eng.pull_raw("rows.val", 31).len();
        // 同じ間に d が出す (ホストが state を求め、 d が `POST /v2/state` で送る)。 その後で c がもう 1 行出して pull する。
        // 直す前は state を当てる間 `gate` の write を持ったので、 a の受信の後ろで待ち、 c の受信もその後ろで止まった
        let (d_tx, d_rx) = channel::<()>();
        {
            let sd = sd.clone();
            std::thread::spawn(move || {
                sd.publish_since(Hlc::ZERO);
                let _ = d_tx.send(());
            });
        }
        let d_returned = d_rx.recv_timeout(Duration::from_secs(10)).is_ok();
        let d_applied = [host_eng.pull_raw("rows.val", 41).len(), host_eng.pull_raw("rows.val", 42).len()];
        write_row(&c, 32);
        let (c2_tx, c2_rx) = channel::<()>();
        {
            let sc = sc.clone();
            std::thread::spawn(move || {
                sc.publish_since(Hlc::ZERO);
                sc.pull_once(100);
                let _ = c2_tx.send(());
            });
        }
        let c2_returned = c2_rx.recv_timeout(Duration::from_secs(10)).is_ok();
        let c2_applied = host_eng.pull_raw("rows.val", 32).len();
        // 受信が当て続けている間も (a は登録したまま)、 他の client が線を上げれば F は進む (前の `try_write` は a が居る間ずっと
        // 進めなかった)。 線を上げるのは b / c / d の往復 (d は state の後の publish で配り終えの線を言う) と ring の回収
        let forget_stuck = host.forget_line();
        for i in 0..3 {
            for (e, s, v) in [(&b, &sb, 200), (&c, &sc, 300), (&d, &sd, 400)] {
                write_row(e, v + i);
                s.publish_since(Hlc::ZERO);
                s.pull_once(100);
            }
            host_eng.reclaim_sync_ops();
        }
        let forget_while_a_applies = host.forget_line();
        let a_still_inside = applying() == 1 && host_eng.pull_raw("rows.val", 2).is_empty();
        drop(held);
        let a_back = a_rx.recv_timeout(Duration::from_secs(30)).is_ok();
        let b_back = b_done || b_rx.recv_timeout(Duration::from_secs(30)).is_ok();
        let c_back = c_returned || c_rx.recv_timeout(Duration::from_secs(30)).is_ok();
        let d_back = d_returned || d_rx.recv_timeout(Duration::from_secs(30)).is_ok();
        let c2_back = c2_returned || c2_rx.recv_timeout(Duration::from_secs(30)).is_ok();
        // 止まっていた受信が出た後、 線は次の要求で進む (取れなかった分を次に回して、 進まないままにならない)
        for _ in 0..3 {
            for s in [&sa, &sb, &sc, &sd] {
                s.publish_since(Hlc::ZERO);
                s.pull_once(100);
            }
            host_eng.reclaim_sync_ops();
        }
        let forget_after = host.forget_line();
        let applied = [2, 31, 32, 41, 42, 202, 302, 402].map(|v| host_eng.pull_raw("rows.val", v).len());
        drop(host);
        drop((sa, sb, sc, sd));
        drop((a, b, c, d, host_eng));
        let _ = std::fs::remove_dir_all(&dir);
        assert!(a_inside, "前提: a の受信が当てる間に入らない");
        assert!(b_reached, "b の要求が返らないか、 F を忘れる段に入らない (b {b_done})");
        assert!(a_still_inside, "前提: c / d が出す間、 a の受信は行を待っていた");
        assert!(
            a_back && b_back && c_back && d_back && c2_back,
            "行を放した後も返らない (a {a_back}, b {b_back}, c {c_back}, d {d_back}, c2 {c2_back})"
        );
        assert_eq!(applied, [1; 8], "前提: a / b / c / d の書き込みがホストに当たった");
        assert!(
            c_returned,
            "a の受信が行を待つ間、 c の publish / pull が返らない (b の線の再計算が a を待ち、 後ろの受信が止まった)"
        );
        assert_eq!(c_applied, 1, "a の受信が行を待つ間に、 c の record が当たらない");
        assert!(d_returned && d_applied == [1, 1], "a の受信が行を待つ間、 d の state が当たらない (当たった {d_applied:?})");
        assert!(c2_returned && c2_applied == 1, "a の受信が行を待つ間、 d の state の後の c の publish が返らないか当たらない");
        assert!(
            forget_while_a_applies > forget_stuck,
            "a の受信が当て続けている間、 他の client が線を上げても F が進まない ({forget_stuck:?} → {forget_while_a_applies:?})"
        );
        assert!(forget_while_a_applies < top_a, "前提: 候補は a の配り終えの線で抑えられている (数える client はその線が先に効く)");
        assert!(forget_after > forget_before, "止まった受信の後も線が進まない ({forget_before:?} → {forget_after:?})");
    }

    /// 置き場ありのホスト、 線を言い終えた client a / b、 書いたまま出していない client n (#980 の読み直しの試験の仕度)。 n の行は
    /// a / b の往復より前に書いた = n の record の版数 (戻り値の最後) は F の候補より古い (線を一度も言っていない新しい client が、
    /// 前に書いた record を初めて出す形)。 ホストの ring は回収済み (`ring_min` は n の record を抑えない)。
    #[allow(clippy::type_complexity)]
    fn host_and_an_old_record(
        tag: &str,
    ) -> (std::path::PathBuf, HttpHost, Arc<Engine>, Vec<(Arc<Engine>, Arc<Syncer>)>, (Arc<Engine>, Arc<Syncer>), Hlc) {
        let dir = std::env::temp_dir().join(format!("enchudb-980-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = |name: &str| dir.join(name).to_str().unwrap().to_string();
        let host_eng = rows_db(&path("host"), 100);
        let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(path("host.cursors").into())).unwrap();
        let url = format!("http://{}", host.addr());
        let client = |name: &str, peer: PeerId| {
            let e = rows_db(&path(name), peer);
            let s = Arc::new(Syncer::new(e.clone(), Arc::new(HttpHostTransport::connect(url.clone(), peer).unwrap())));
            (e, s)
        };
        let ab = vec![client("a", 1), client("b", 2)];
        let rounds = |v: u32| {
            for (i, (e, _)) in ab.iter().enumerate() {
                write_row(e, v + i as u32);
            }
            for _ in 0..3 {
                for (_, s) in &ab {
                    s.publish_since(Hlc::ZERO);
                    s.pull_once(100);
                }
            }
        };
        // a / b の最初の record は ring に残り、 n の record が来るまで F を n の record より下に抑える
        rounds(10);
        let n = client("n", 9);
        let old = write_row(&n.0, 900);
        rounds(20);
        host_eng.reclaim_sync_ops();
        let candidate = host.inner.lines.lock().unwrap().forget_candidate(host.inner.clock.now());
        let forget = host.forget_line();
        assert!(candidate.is_some_and(|c| c > old), "前提: F の候補が n の record より上 ({candidate:?} / {old:?})");
        assert!(forget < old, "前提: F はまだ n の record より下 ({forget:?} / {old:?})");
        assert!(full_ring_min(&host_eng).is_none_or(|m| m > old), "前提: ホストの ring は n の record より古い物を持たない");
        (dir, host, host_eng, ab, n, old)
    }

    /// #980 の読み直し: 当てている途中の受信の record (ring に写る前) を F は越えない。 新しい client n が前に書いた record を出し、
    /// ホストが当てて WAL に載せた後・ring に写す書き出しの前 (`wal_sync_hook`) に、 別の要求の `recompute` が F を決める。 F の候補
    /// (a / b の線) は n の record より上、 ring (consumer を止めてある) には n の record がまだ無い。 F は n の登録で抑えられる。
    /// 前の作りは締め出し (当てている間は F を決めない) で同じ事を守っていた
    #[test]
    fn the_forget_line_does_not_pass_a_record_being_applied() {
        let (dir, host, host_eng, ab, (n, sn), old) = host_and_an_old_record("inflight");
        let seen = Arc::new(Mutex::new(None));
        {
            let (seen, inner) = (seen.clone(), Arc::downgrade(&host.inner));
            *host.inner.wal_sync_hook.lock().unwrap() = Some(Box::new(move |after| {
                let mut g = seen.lock().unwrap();
                if let (false, None, Some(inner)) = (after, g.as_ref(), inner.upgrade()) {
                    let ring = full_ring_min(&inner.engine);
                    inner.recompute(inner.clock.now());
                    *g = Some((ring, inner.lines.lock().unwrap().forget));
                }
            }));
        }
        let hold = host_eng.hold_consumer();
        sn.publish_since(Hlc::ZERO);
        drop(hold);
        *host.inner.wal_sync_hook.lock().unwrap() = None;
        let applied = host_eng.pull_raw("rows.val", 900).len();
        let seen = *seen.lock().unwrap();
        drop(host);
        drop((ab, sn, n, host_eng));
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(applied, 1, "前提: n の record がホストに当たった");
        let (ring, forget) = seen.expect("前提: 書き出しの前の割り込み点を通った");
        assert!(ring.is_none_or(|m| m > old), "前提: 割り込み点で n の record はまだ ring に無い ({ring:?})");
        assert!(forget <= old, "F が当てている途中の n の record を越えた ({forget:?} > {old:?})");
    }

    /// 0.38 前 review の B4 (#966 の電源断の入口): 黙った client P を外して F を進め、 ホストが削除の記録を忘れて disk に書いた時
    /// (`persist_sync_state` の直後) に電源が落ちても、 置き場 (`.clients`) には P を外した印がもう disk にある (開き直したホストは戻った P
    /// の publish を `STALE` で断る)。 忘れる前に置き場を書き出す (中身の fsync → rename → directory の fsync)。
    ///
    /// 前は置き場を書くのが忘れた後で、 書き方も fsync しない (`fs::write` + `rename`) ので、 忘れた削除だけが disk に届いて置き場が空 /
    /// 古い像がありえた (開き直したホストで P は外した印の無い client になり、 戻った P の publish が当たって、 忘れた削除より前の作成で
    /// 消えた行が生き返る)。 この試験が見るのは順 (忘れた時に disk の置き場に印がある) だけ。 fsync は電源断の模擬 (crashsim) が transport
    /// に無いので試験にかけていない。
    ///
    /// falsify: `recompute` の、 F を進める前の置き場の書き出しを外すと、 割り込み点で読む置き場に P の印が無い。
    #[test]
    fn a_dropped_client_is_on_disk_before_the_host_forgets_deletes() {
        let (dir, host, host_eng, clients_file, deleted) = host_with_a_silent_client("order");
        let now = host.inner.clock.now();
        let on_disk: Arc<Mutex<Option<Lines>>> = Arc::default();
        {
            let (on_disk, path) = (on_disk.clone(), clients_file.clone());
            *host.inner.forgot_hook.lock().unwrap() = Some(Box::new(move || {
                let text = std::fs::read_to_string(&path).unwrap_or_default();
                *on_disk.lock().unwrap() = Some(Lines::decode(&text));
            }));
        }
        host.inner.recompute(now);
        *host.inner.forgot_hook.lock().unwrap() = None;
        let forget = host.forget_line();
        let on_disk = on_disk.lock().unwrap().take();
        drop(host);
        drop(host_eng);
        let _ = std::fs::remove_dir_all(&dir);
        assert!(forget > deleted, "前提: F が削除の先へ進んだ (F {forget:?})");
        // 割り込み点は忘れた数が 0 でない時だけ呼ぶ
        let on_disk = on_disk.expect("前提: 削除を忘れて書いた所の割り込み点を通った");
        assert!(
            on_disk.clients.get(&3).is_some_and(|c| c.dropped),
            "削除を忘れて disk に書いた時、 置き場に P を外した印が無い (この時に落ちると、 開き直したホストが戻った P を断らない): {:?}",
            on_disk.clients.get(&3)
        );
    }

    /// B4 の試験の仕度: 置き場ありのホスト (沈黙の上限 60 s) に、 黙った client P (3、 次の `recompute` で外す) と数える client Q (4、
    /// 線は削除より上)、 忘れる削除 (写像の無い相手の行の削除 = 退避表に載る、 ring に record を作らない) を置く。
    /// (directory, ホスト, ホストの engine, 置き場の path, 削除の版数) を返す。
    #[allow(clippy::type_complexity)]
    fn host_with_a_silent_client(tag: &str) -> (std::path::PathBuf, HttpHost, Arc<Engine>, std::path::PathBuf, Hlc) {
        let dir = std::env::temp_dir().join(format!("enchudb-b4-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let host_eng = rows_db(dir.join("host").to_str().unwrap(), 100);
        let deleted = Hlc { wall: 1_000, logical: 0, peer: 7 };
        assert!(host_eng.remote_delete_apply_foreign(enchudb_oplog::make_eid(7, 5), deleted), "前提: 削除を退避表に載せた");
        let cursors = dir.join("host.cursors");
        let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(cursors.clone())).unwrap();
        let clients_file = {
            let mut s = cursors.into_os_string();
            s.push(".clients");
            std::path::PathBuf::from(s)
        };
        let now = host.inner.clock.now();
        let line = Hlc { wall: 5_000, logical: 0, peer: 4 };
        {
            let mut l = host.inner.lines.lock().unwrap();
            l.silence = Some(Duration::from_secs(60));
            l.clients.insert(3, ClientState { last_seen_ms: now.saturating_sub(120_000), through: Some(line), caught_up: line, ..ClientState::default() });
            l.clients.insert(4, ClientState { last_seen_ms: now, through: Some(line), caught_up: line, ..ClientState::default() });
        }
        (dir, host, host_eng, clients_file, deleted)
    }

    /// #966 の読み直し [F6] / [F2]: 置き場を書けない間は F を進めない (削除の記録を忘れない)。 書けるようになったら進む。 書けない形は、
    /// 置き場の一時 file の名前に directory を置いて作る。
    ///
    /// falsify: `recompute` の 「書けなければ進めない」 (`f > current && self.save_lines(false)` の後ろ半分) を外すと、 書けないまま F が
    /// 削除の先へ進み、 削除の記録を忘れる。
    #[test]
    fn the_forget_line_does_not_move_while_the_client_file_cannot_be_written() {
        let (dir, host, host_eng, clients_file, deleted) = host_with_a_silent_client("unwritable");
        let blocker = enchudb::db_files::tmp_path_for(&clients_file);
        std::fs::create_dir_all(&blocker).unwrap();
        let forgot = Arc::new(AtomicU64::new(0));
        {
            let forgot = forgot.clone();
            *host.inner.forgot_hook.lock().unwrap() = Some(Box::new(move || {
                forgot.fetch_add(1, Ordering::AcqRel);
            }));
        }
        host.inner.recompute(host.inner.clock.now());
        let (forget_blocked, forgot_blocked) = (host.forget_line(), forgot.load(Ordering::Acquire));
        let dropped_in_memory = host.inner.lines.lock().unwrap().clients.get(&3).is_some_and(|c| c.dropped);
        std::fs::remove_dir_all(&blocker).unwrap();
        host.inner.recompute(host.inner.clock.now());
        let (forget_after, forgot_after) = (host.forget_line(), forgot.load(Ordering::Acquire));
        *host.inner.forgot_hook.lock().unwrap() = None;
        let on_disk = Lines::decode(&std::fs::read_to_string(&clients_file).unwrap_or_default());
        drop(host);
        drop(host_eng);
        let _ = std::fs::remove_dir_all(&dir);
        assert!(dropped_in_memory, "前提: P を外した (memory)");
        assert!(
            forget_blocked < deleted && forgot_blocked == 0,
            "置き場を書けないのに F を進めた / 削除の記録を忘れた (F {forget_blocked:?}, 忘れた回 {forgot_blocked})"
        );
        assert!(forget_after > deleted && forgot_after == 1, "書けるようになった後に F が進まない (F {forget_after:?}, 忘れた回 {forgot_after})");
        assert!(on_disk.clients.get(&3).is_some_and(|c| c.dropped), "書けるようになった後の置き場に P を外した印が無い");
    }

    /// #966 の読み直し [F5]: 置き場は last_seen だけの変化を、 沈黙の上限の 4 分の 1 (と 1 時間の小さい方) を越えたら書く。 落ちた後に
    /// 開くと置き場の last_seen から数えるので、 上限 60 s で 20 s 後に来た client の last_seen は disk に届いている。
    ///
    /// 前は幅が 1 時間に決まっていて、 上限が短いと、 落ちる前に来ていた生きている client を開いた後に外しえた (戻った client は publish を
    /// 断られて作り直しになる)。
    ///
    /// falsify: `Lines::last_seen_slack_ms` を 1 時間に戻すと、 disk の last_seen が 20 s 前のまま。
    #[test]
    fn a_client_visit_beyond_a_quarter_of_the_silence_limit_reaches_the_client_file() {
        let dir = std::env::temp_dir().join(format!("enchudb-f5-host-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let host_eng = rows_db(dir.join("host").to_str().unwrap(), 100);
        let cursors = dir.join("host.cursors");
        let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(cursors.clone())).unwrap();
        let now = host.inner.clock.now();
        {
            let mut l = host.inner.lines.lock().unwrap();
            l.silence = Some(Duration::from_secs(60));
            l.clients.insert(3, ClientState { last_seen_ms: now, ..ClientState::default() });
        }
        assert!(host.inner.save_lines(false), "前提: 置き場を書いた");
        let read = || {
            let mut s = cursors.clone().into_os_string();
            s.push(".clients");
            Lines::decode(&std::fs::read_to_string(std::path::PathBuf::from(s)).unwrap_or_default())
        };
        // 5 s 後に来た (幅 15 s の内 = 書かない)、 20 s 後に来た (幅を越える = 書く)
        host.inner.lines.lock().unwrap().clients.get_mut(&3).unwrap().last_seen_ms = now + 5_000;
        assert!(host.inner.save_lines(false));
        let within = read().clients.get(&3).map(|c| c.last_seen_ms);
        host.inner.lines.lock().unwrap().clients.get_mut(&3).unwrap().last_seen_ms = now + 20_000;
        assert!(host.inner.save_lines(false));
        let beyond = read().clients.get(&3).map(|c| c.last_seen_ms);
        drop(host);
        drop(host_eng);
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(within, Some(now), "前提: 幅の内の last_seen の変化は書かない (書くたびに fsync)");
        assert_eq!(
            beyond,
            Some(now + 20_000),
            "上限 60 s の 4 分の 1 を越えて来た client の last_seen が disk に無い (落ちた後に開くと 20 s 長く黙っていたと数える)"
        );
    }

    /// #980 の 2 回目の読み直し: `recompute` は登録を読んでから `ring_min` を読む。 逆だと、 `ring_min` を読んだ後に n の受信が当て終えて
    /// ring に写して登録を外すと、 登録の写しにも `ring_min` にも n の record が無く、 F がそれを越える。 登録を読む直前
    /// (`lock_hook`) に n の受信を最後まで (ring に写して外すまで) 進める。
    #[test]
    fn the_forget_line_reads_the_ring_after_the_registrations() {
        let (dir, host, host_eng, ab, (n, sn), old) = host_and_an_old_record("order");
        let ran = Arc::new(AtomicBool::new(false));
        {
            let (ran, sn) = (ran.clone(), sn.clone());
            *host.inner.lock_hook.lock().unwrap() = Some(Box::new(move || {
                sn.publish_since(Hlc::ZERO);
                ran.store(true, Ordering::Release);
            }));
        }
        host.inner.recompute(host.inner.clock.now());
        let forget = host.forget_line();
        let applied = host_eng.pull_raw("rows.val", 900).len();
        let in_ring = full_ring_min(&host_eng).is_some_and(|m| m <= old);
        drop(host);
        drop((ab, sn, n, host_eng));
        let _ = std::fs::remove_dir_all(&dir);
        assert!(ran.load(Ordering::Acquire), "前提: 登録を読む直前の割り込み点を通った");
        assert!(applied == 1 && in_ring, "前提: n の record は割り込み点の中で当たり、 ring に写った (当たった {applied}, ring {in_ring})");
        assert!(forget <= old, "F が、 登録を外して ring に写った n の record を越えた ({forget:?} > {old:?})");
    }

    /// #980 の読み直し: F を忘れている途中に来た、 その F より古い record を持つ受信は、 忘れ終えるまで当てない (忘れる前に当てた
    /// record と忘れた後に当てた record で答えが変わるので、 前の作りと同じく忘れた後に当てる)。
    #[test]
    fn a_receive_older_than_the_line_being_forgotten_waits() {
        let (dir, host, host_eng, ab, (n, sn), _old) = host_and_an_old_record("wait");
        let seen = Arc::new(Mutex::new(None));
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        {
            let (seen, inner, he, sn) = (seen.clone(), Arc::downgrade(&host.inner), host_eng.clone(), sn.clone());
            let mut done_tx = Some(done_tx);
            *host.inner.forget_hook.lock().unwrap() = Some(Box::new(move || {
                let (Some(inner), Some(done_tx)) = (inner.upgrade(), done_tx.take()) else { return };
                let sn = sn.clone();
                std::thread::spawn(move || {
                    sn.publish_since(Hlc::ZERO);
                    let _ = done_tx.send(());
                });
                let waited = wait_until(Duration::from_secs(5), || inner.waited.load(Ordering::Acquire) > 0);
                *seen.lock().unwrap() = Some((waited, he.pull_raw("rows.val", 900).len()));
            }));
        }
        host.inner.recompute(host.inner.clock.now());
        let returned = done_rx.recv_timeout(Duration::from_secs(30)).is_ok();
        *host.inner.forget_hook.lock().unwrap() = None;
        let applied = host_eng.pull_raw("rows.val", 900).len();
        let seen = *seen.lock().unwrap();
        drop(host);
        drop((ab, sn, n, host_eng));
        let _ = std::fs::remove_dir_all(&dir);
        let (waited, during) = seen.expect("前提: F を忘れる段に入らない");
        assert!(returned && applied == 1, "前提: 忘れ終えた後に n の record が当たらない (返った {returned}, 当たった {applied})");
        assert!(waited && during == 0, "忘れている途中に、 F より古い n の record を当てた (待った {waited}, 当たった {during})");
    }

    /// #985: F が ring の最古の record に留められている間 (ring を回収しない)、 client の要求のたびに F を進めようとしても ring を
    /// 読み直さない。 F は直す前の読み (ring の全部の行を写して decode した最小) の所に留まる。 回収で最古の行が消えた後は 1 回だけ
    /// 読み直し、 F は進むが ring に残る行を越えない。
    ///
    /// 直す前: F を進めようとする回 (`forget_rounds`) ごとに ring の全部の行を写して decode した (100 万行で 1 回 60〜110 ms)。
    #[test]
    fn a_pinned_forget_line_does_not_read_the_ring_per_request() {
        let dir = std::env::temp_dir().join(format!("enchudb-985-pinned-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = |name: &str| dir.join(name).to_str().unwrap().to_string();
        let host_eng = rows_db(&path("host"), 100);
        let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(path("host.cursors").into())).unwrap();
        let url = format!("http://{}", host.addr());
        let clients: Vec<(Arc<Engine>, Syncer)> = [1, 2]
            .into_iter()
            .map(|peer| {
                let e = rows_db(&path(&format!("c{peer}")), peer);
                let s = Syncer::new(e.clone(), Arc::new(HttpHostTransport::connect(url.clone(), peer).unwrap()));
                (e, s)
            })
            .collect();
        let round = |v: u32| {
            for (i, (e, s)) in clients.iter().enumerate() {
                write_row(e, v + i as u32);
                s.publish_since(Hlc::ZERO);
                s.pull_once(100);
            }
        };
        let full = full_ring_min;
        for v in [10, 20, 30] {
            round(v);
        }
        let pinned = full(&host_eng);
        let scans = || host_eng.sync_ring_min_scans();
        let rounds = || host.inner.forget_rounds.load(Ordering::Acquire);
        let (s0, r0) = (scans(), rounds());
        for i in 0..10 {
            round(100 + 10 * i);
        }
        let (s1, r1) = (scans(), rounds());
        let forget = host.forget_line();
        let still = full(&host_eng);
        // 回収で最古の行が消える: 次に F を進めようとする回が 1 回だけ読み直す
        let purged = host_eng.reclaim_sync_ops();
        let after_reclaim = full(&host_eng);
        for i in 0..5 {
            round(300 + 10 * i);
        }
        let (s2, r2) = (scans(), rounds());
        let forget_after = host.forget_line();
        let ring_after = full(&host_eng);
        drop(host);
        drop(clients);
        drop(host_eng);
        let _ = std::fs::remove_dir_all(&dir);
        assert!(pinned.is_some() && still == pinned, "前提: ring の最古の record は回収まで残る ({pinned:?} / {still:?})");
        assert_eq!(Some(forget), pinned, "前提: F が ring の最古の record に留められている");
        assert!(r1 - r0 >= 10, "前提: 要求のたびに F を進めようとした ({})", r1 - r0);
        assert_eq!(s1 - s0, 0, "F が ring の最古の record に留められている間に、 要求で ring を読み直した ({} 回)", s1 - s0);
        assert!(purged > 0 && after_reclaim > pinned, "前提: 回収で最古の行が消えた ({purged} 行、 {after_reclaim:?})");
        assert!(r2 > r1, "前提: 回収の後も F を進めようとした");
        assert_eq!(s2 - s1, 1, "回収の後に ring を読み直した回数");
        assert!(forget_after > forget, "回収の後に F が進まない ({forget:?} → {forget_after:?})");
        assert!(ring_after.is_none_or(|m| forget_after <= m), "F が ring に残る record を越えた ({forget_after:?} > {ring_after:?})");
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
