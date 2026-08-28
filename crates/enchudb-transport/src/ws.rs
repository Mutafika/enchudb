//! WebSocket push transport — origin が publish 時に subscriber に即時 broadcast。
//!
//! HTTP pull は polling のため propagation が pull interval + RTT で遅れる。
//! WebSocket では origin が能動的に push、propagation = RTT のみ (LAN なら sub-ms)。
//!
//! # 構成
//!
//! - [`WsPushHub`] = origin 側サーバー、TCP listen + WS handshake、`broadcast()` で全 subscriber に流す
//! - [`WsPushClient`] = replica 側クライアント、connect → 受信 callback ループ
//!
//! # 使い方
//!
//! ```
//! use enchudb_transport::ws::WsPushHub;
//!
//! // origin: ephemeral port で hub 起動、subscriber が居なくても broadcast OK
//! let hub = WsPushHub::start("127.0.0.1:0").unwrap();
//! assert!(hub.addr().port() != 0);
//! // hub.broadcast(peer_id, &records) で connected subscriber に push
//! // hub は drop で shutdown
//! ```
//!
//! # フレーム形式
//!
//! WebSocket binary frame = `encode_batch(&[WireRecord])` (既存の transport.rs と同じ)。
//! 1 frame = 1 batch。順序は HLC 昇順を hub 側で保証。

use enchudb::changefeed::ChangeListener;
use std::io::{self};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tungstenite::protocol::{Message, WebSocket};

use enchudb::transport::{decode_batch, encode_batch, WireRecord};
use enchudb_oplog::{Hlc, PeerId};

// ─────────────────────────────────────────────────────────────
// Server side: WsPushHub
// ─────────────────────────────────────────────────────────────

/// 接続中の subscriber。broadcast 時に書き込みする。
struct Subscriber {
    /// この subscriber が張っている **link** (= pull における pull 先)。
    /// relay に張れば relay の stream 全部 (複数 author) が来る。
    from_peer: PeerId,
    /// #228: author 別の消化位置。 **relay の stream は HLC が単調でない**
    /// (自 row = 自 clock、 中継 row = 原 author の HLC 素通し #209) ので、
    /// scalar cursor で filter すると中継された古い record が永久に落ちる
    /// (#216 の push 版)。 **entry の無い author は `Hlc::ZERO` 起点** —
    /// 「既知 author の min」 や baseline への短絡は同じ穴を一段下で再現する。
    since_by: std::collections::HashMap<PeerId, Hlc>,
    /// legacy (scalar `since`) 用の**無帰属 baseline**。 author 別を運べない
    /// 旧 client は「単一 author の stream に張っている」という宣言なので、
    /// 全 author への下限として従来どおり効かせる。 `since_by` を送ってくる
    /// client では使わない (ZERO)。
    baseline: Hlc,
    ws: Mutex<WebSocket<TcpStream>>,
}

impl Subscriber {
    /// この record を送るべきか。
    fn wants(&self, r: &WireRecord) -> bool {
        let floor = self
            .since_by
            .get(&r.author_peer)
            .copied()
            .unwrap_or(Hlc::ZERO)
            .max(self.baseline);
        r.hlc > floor
    }
}

/// WebSocket push サーバー。listen + handshake + broadcast。
pub struct WsPushHub {
    addr: SocketAddr,
    subscribers: Arc<Mutex<Vec<Arc<Subscriber>>>>,
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl WsPushHub {
    pub fn start(bind_addr: &str) -> io::Result<Self> {
        let listener = TcpListener::bind(bind_addr)?;
        let addr = listener.local_addr()?;
        listener.set_nonblocking(true)?;

        let subscribers: Arc<Mutex<Vec<Arc<Subscriber>>>> = Arc::new(Mutex::new(Vec::new()));
        let shutdown = Arc::new(AtomicBool::new(false));

        let subs_bg = subscribers.clone();
        let sh_bg = shutdown.clone();
        let handle = std::thread::spawn(move || {
            loop {
                if sh_bg.load(Ordering::Acquire) { break; }
                match listener.accept() {
                    Ok((stream, _)) => {
                        let subs = subs_bg.clone();
                        std::thread::spawn(move || {
                            let _ = handle_ws_connection(stream, subs);
                        });
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
            }
        });

        Ok(Self { addr, subscribers, shutdown, handle: Some(handle) })
    }

    pub fn addr(&self) -> SocketAddr { self.addr }

    /// 接続中 subscriber 数。
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.lock().unwrap().len()
    }

    /// `peer` (= **link**) の records を、そこに張っている subscriber 全員に push する。
    ///
    /// `peer` は「この stream を誰から受け取ったことにするか」= pull における pull 先。
    /// relay がこれを呼ぶ場合、 records には **原 author の record** が混ざる
    /// (#209 素通し) が、 それでも `peer` は relay 自身の id にする — subscriber は
    /// link に張るので (pull の意味論と同じ)。 author 別の絞り込みは
    /// [`Subscriber::wants`] が `author_peer` を見て行う (#228)。
    ///
    /// 死んでる subscriber は次回 broadcast で除去 (write エラー検出時)。
    pub fn broadcast(&self, peer: PeerId, records: &[WireRecord]) {
        if records.is_empty() { return; }
        let subs = self.subscribers.lock().unwrap().clone();
        let mut dead_indices: Vec<usize> = Vec::new();

        for (idx, sub) in subs.iter().enumerate() {
            if sub.from_peer != peer { continue; }
            let filtered: Vec<WireRecord> = records.iter()
                .filter(|r| sub.wants(r))
                .cloned()
                .collect();
            if filtered.is_empty() { continue; }
            let bytes = encode_batch(&filtered);
            let mut ws = sub.ws.lock().unwrap();
            if ws.send(Message::Binary(bytes.into())).is_err() {
                dead_indices.push(idx);
            }
        }

        if !dead_indices.is_empty() {
            let mut subs_w = self.subscribers.lock().unwrap();
            // ptr_eq で除去 (順序が変わってる可能性あり)
            for dead in dead_indices.iter().rev() {
                if let Some(d) = subs.get(*dead) {
                    subs_w.retain(|s| !Arc::ptr_eq(s, d));
                }
            }
        }
    }
}

impl Drop for WsPushHub {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() { let _ = h.join(); }
    }
}

/// #228: `since_by` query の parse。 `author:wall.logical.peer` を `,` 区切り。
///
/// **entry が 1 つでも壊れていたら丸ごと捨てる** — 半端に読むと「消化済み」を偽る
/// floor になり、 record を黙って落とす。 空 map に落ちれば全部 ZERO 起点で
/// 送り直されるだけ (冪等な apply が受け止める)。
fn parse_since_by(v: &str) -> std::collections::HashMap<PeerId, Hlc> {
    let mut out = std::collections::HashMap::new();
    for entry in v.split(',').filter(|e| !e.is_empty()) {
        let Some((author, hlc)) = entry.split_once(':') else { return Default::default() };
        let mut parts = hlc.split('.');
        let (Some(wall), Some(logical), Some(peer), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Default::default();
        };
        let (Ok(author), Ok(wall), Ok(logical), Ok(peer)) = (
            author.parse::<PeerId>(),
            wall.parse::<u64>(),
            logical.parse::<u32>(),
            peer.parse::<u32>(),
        ) else {
            return Default::default();
        };
        out.insert(author, Hlc { wall, logical, peer });
    }
    out
}

/// `since_by` query の組み立て (client 側)。
fn encode_since_by(since: &[(PeerId, Hlc)]) -> String {
    since
        .iter()
        .map(|(a, h)| format!("{}:{}.{}.{}", a, h.wall, h.logical, h.peer))
        .collect::<Vec<_>>()
        .join(",")
}

/// 1 接続の handshake → subscriber 登録 → 接続生存維持。
fn handle_ws_connection(
    stream: TcpStream,
    subscribers: Arc<Mutex<Vec<Arc<Subscriber>>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // listener が non-blocking だと accept した stream も non-blocking を継承する (Linux)
    // handshake 中は blocking じゃないと accept_hdr の内部 read が WouldBlock で失敗
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;

    // handshake と同時に path から query を抜く (Arc<Mutex> で共有)
    type Captured = (PeerId, Hlc, std::collections::HashMap<PeerId, Hlc>);
    let captured: Arc<Mutex<Captured>> =
        Arc::new(Mutex::new((0, Hlc::ZERO, std::collections::HashMap::new())));
    let captured_cb = captured.clone();
    let callback = move |req: &Request, response: Response| -> Result<Response, ErrorResponse> {
        let uri = req.uri().to_string();
        if let Some(q) = uri.split_once('?').map(|(_, q)| q) {
            let mut g = captured_cb.lock().unwrap();
            for kv in q.split('&') {
                if let Some((k, v)) = kv.split_once('=') {
                    match k {
                        "from" => g.0 = v.parse().unwrap_or(0),
                        "wall" => g.1.wall = v.parse().unwrap_or(0),
                        "logical" => g.1.logical = v.parse().unwrap_or(0),
                        "peer" => g.1.peer = v.parse().unwrap_or(0),
                        // #228: author 別 cursor。 `author:wall.logical.peer` を `,` 区切り。
                        // 壊れた entry は黙って落とさず**丸ごと無視**する
                        // (半端に読むと「消化済み」を偽る floor になる)。
                        "since_by" => g.2 = parse_since_by(v),
                        _ => {}
                    }
                }
            }
        }
        Ok(response)
    };
    let ws = tungstenite::accept_hdr(stream, callback)?;
    // handshake 完了後、read で長時間 block しないよう短い timeout に
    {
        let s = ws.get_ref();
        s.set_read_timeout(Some(Duration::from_millis(100)))?;
    }
    let (from_peer, scalar_since, since_by) = captured.lock().unwrap().clone();
    // #228: author 別を送ってきた client には baseline を適用しない — 未知 author は
    // ZERO 起点でなければならない (baseline へ短絡すると同じ穴の再発)。 scalar しか
    // 送らない legacy client では従来どおり全 author への下限として効かせる。
    let baseline = if since_by.is_empty() { scalar_since } else { Hlc::ZERO };
    let sub = Arc::new(Subscriber { from_peer, since_by, baseline, ws: Mutex::new(ws) });
    subscribers.lock().unwrap().push(sub.clone());

    // 短い read timeout で poll、WouldBlock のたびに lock 解放して broadcast に譲る
    loop {
        let read_result = {
            let mut ws = sub.ws.lock().unwrap();
            ws.read()
        };
        match read_result {
            Ok(Message::Close(_)) => break,
            Ok(_) => {}
            Err(tungstenite::Error::Io(e))
                if e.kind() == io::ErrorKind::WouldBlock
                || e.kind() == io::ErrorKind::TimedOut =>
            {
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(_) => break,
        }
    }

    // 接続終了 → subscribers から外す
    let mut subs = subscribers.lock().unwrap();
    subs.retain(|s| !Arc::ptr_eq(s, &sub));
    Ok(())
}

// ─────────────────────────────────────────────────────────────
// Client side: WsPushClient
// ─────────────────────────────────────────────────────────────

/// WS subscribe を別スレッドで張り、record 着信ごとに callback を呼ぶ。
/// Drop で接続切る。
pub struct WsPushClient {
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl WsPushClient {
    /// `url` 例: "ws://127.0.0.1:8080/push"
    /// `from` = 張り先の **link** (pull における pull 先)、
    /// `since` = この HLC より後だけ受け取る。
    ///
    /// **`since` は全 author への下限として効く**ので、 relay に張る場合は
    /// [`WsPushClient::connect_and_run_multi`] を使うこと — relay の stream は
    /// HLC が単調でないので、 scalar cursor は中継された古い record を落とす
    /// (#228)。 単一 author の stream に張る場合はこちらで正しい。
    pub fn connect_and_run<F>(
        url: &str,
        from: PeerId,
        since: Hlc,
        on_record: F,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        F: FnMut(WireRecord) + Send + 'static,
    {
        Self::connect_inner(url, from, since, &[], on_record)
    }

    /// #228: author 別 cursor で subscribe する (relay link 用)。
    ///
    /// relay の stream は「自 row (自 clock)」と「中継 row (原 author の HLC 素通し
    /// #209)」の merge なので **HLC が単調でない**。 scalar cursor で filter すると、
    /// relay 自身の新しい row を跨いだ後に中継された古い record が永久に落ちる
    /// (#216 の push 版)。 author ごとの substream は relay を何 hop 挟んでも単調、
    /// がこの粒度を健全にする不変式。
    ///
    /// **`since` に無い author は `Hlc::ZERO` 起点**で全部届く。 push は changefeed
    /// (= これから commit される分) しか流さないので、 過去の再送にはならない。
    ///
    /// 受け取った record を apply しても、 それ自体は author への消化証明にならない
    /// (**delivery ≠ apply**)。 cursor 前進と ack は従来どおり `Syncer::pull_once`
    /// の仕事で、 **push は pull を置き換えず pull の頻度を下げる**もの。
    pub fn connect_and_run_multi<F>(
        url: &str,
        from: PeerId,
        since: &[(PeerId, Hlc)],
        on_record: F,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        F: FnMut(WireRecord) + Send + 'static,
    {
        Self::connect_inner(url, from, Hlc::ZERO, since, on_record)
    }

    fn connect_inner<F>(
        url: &str,
        from: PeerId,
        since: Hlc,
        since_by: &[(PeerId, Hlc)],
        mut on_record: F,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        F: FnMut(WireRecord) + Send + 'static,
    {
        let mut full_url = format!(
            "{}?from={}&wall={}&logical={}&peer={}",
            url, from, since.wall, since.logical, since.peer
        );
        if !since_by.is_empty() {
            full_url.push_str("&since_by=");
            full_url.push_str(&encode_since_by(since_by));
        }
        let (mut ws, _) = tungstenite::connect(&full_url)?;
        // Drop で shutdown → join したい、長い read を中断するため短い timeout を入れる
        {
            let s = ws.get_ref();
            if let tungstenite::stream::MaybeTlsStream::Plain(tcp) = s {
                tcp.set_read_timeout(Some(Duration::from_millis(200)))?;
            }
        }
        let shutdown = Arc::new(AtomicBool::new(false));
        let sh_bg = shutdown.clone();
        let handle = std::thread::spawn(move || {
            loop {
                if sh_bg.load(Ordering::Acquire) { break; }
                match ws.read() {
                    Ok(Message::Binary(bytes)) => {
                        if let Ok(records) = decode_batch(&bytes) {
                            for r in records { on_record(r); }
                        }
                    }
                    Ok(Message::Close(_)) => break,
                    Ok(_) => continue,
                    Err(tungstenite::Error::Io(e))
                        if e.kind() == io::ErrorKind::WouldBlock
                        || e.kind() == io::ErrorKind::TimedOut => continue,
                    Err(_) => break,
                }
            }
        });
        Ok(Self { shutdown, handle: Some(handle) })
    }
}

impl Drop for WsPushClient {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() { let _ = h.join(); }
    }
}

// ─────────────────────────────────────────────────────────────
// ChangeListener adapter — Engine の changefeed を WsPushHub に流す
// ─────────────────────────────────────────────────────────────

/// `enchudb::changefeed::ChangeListener` を実装して [`WsPushHub`] に流すアダプタ。
///
/// engine が WAL に commit するたびに、自動で connected subscriber に push される。
///
/// # push は pull を置き換えない (#228)
///
/// **delivery ≠ apply**。 push で届いた record を subscriber が apply しても、
/// それは author への消化証明にはならない (WS の送信完了は「相手が書けた」ことを
/// 何も保証しない)。 cursor の前進と pull-as-ack (#149) は従来どおり
/// `Syncer::pull_once` の仕事で、 **push は pull の頻度を下げるもの**。
/// reclaim が回るのは pull が回っている時だけ。
///
/// # relay が使う場合 (#209 / #228)
///
/// `peer_id` は broadcast の **link 名**として使われる (subscriber の `from` と照合)。
/// relay の changefeed には**原 author の record** が流れるが、 `peer_id` には
/// **relay 自身の id** を渡すこと — subscriber は pull と同じく link に張るため。
/// author 別の絞り込みは subscriber 側の `since_by` が `author_peer` を見て行う
/// ([`WsPushClient::connect_and_run_multi`])。 relay に張る subscriber が scalar
/// `since` を使うと、 中継された古い HLC の record が落ちる。
///
/// # 使い方
///
/// ```
/// use std::sync::Arc;
/// use enchudb::{Engine, ValueType};
/// use enchudb_transport::ws::{WsPushHub, WsPushHubAdapter};
///
/// let path = format!("/tmp/enchudb-ws-adapter-doc-{}.db", std::process::id());
/// # let _ = std::fs::remove_file(&path);
/// # let _ = std::fs::remove_file(format!("{}.oplog", path));
/// {
///     let mut eng = Engine::create_standalone(&path).unwrap();
///     eng.define_himo("v", ValueType::Number, 100);
///     eng.flush().unwrap();
/// }
/// let eng = Engine::open_concurrent_with_oplog(&path, 4 * 1024 * 1024).unwrap();
/// eng.set_peer_id(1);
///
/// // hub 起動 + adapter attach
/// let hub = Arc::new(WsPushHub::start("127.0.0.1:0").unwrap());
/// eng.add_change_listener(Arc::new(WsPushHubAdapter::new(hub.clone(), 1)));
///
/// // 以降の commit は subscriber に自動 broadcast
/// // (このテストでは subscriber 居ないので broadcast は no-op)
/// let e = eng.entity().unwrap();
/// eng.tie_async(e, "v", 42);
/// eng.oplog_commit();
/// eng.flush_writes();
/// eng.oplog_sync().unwrap();
///
/// drop(eng);
/// # let _ = std::fs::remove_file(&path);
/// # let _ = std::fs::remove_file(format!("{}.oplog", path));
/// ```
pub struct WsPushHubAdapter {
    hub: std::sync::Arc<WsPushHub>,
    peer_id: enchudb_oplog::PeerId,
}

impl WsPushHubAdapter {
    /// `peer_id` は publish 時に subscriber の `from_peer` フィルタで照合されるので、
    /// engine が attach されている自 peer の id を渡すこと。
    pub fn new(hub: std::sync::Arc<WsPushHub>, peer_id: enchudb_oplog::PeerId) -> Self {
        Self { hub, peer_id }
    }
}

impl ChangeListener for WsPushHubAdapter {
    fn on_changes(&self, records: &[enchudb::transport::WireRecord]) {
        self.hub.broadcast(self.peer_id, records);
    }
}

// ─────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use enchudb_oplog::oplog::DecodedOp;
    use std::sync::mpsc;

    fn rec(wall: u64, peer: PeerId, eid: u64, value: u32) -> WireRecord {
        WireRecord::unsigned(
            Hlc { wall, logical: 0, peer },
            peer,
            DecodedOp::Tie { eid, himo_id: 0, value },
        )
    }

    #[test]
    fn hub_starts_and_stops() {
        let hub = WsPushHub::start("127.0.0.1:0").unwrap();
        assert!(hub.addr().port() != 0);
        assert_eq!(hub.subscriber_count(), 0);
        drop(hub);
    }

    #[test]
    fn client_subscribes_and_receives_broadcast() {
        let hub = WsPushHub::start("127.0.0.1:0").unwrap();
        let url = format!("ws://{}/push", hub.addr());

        let (tx, rx) = mpsc::channel();
        let _client = WsPushClient::connect_and_run(
            &url,
            1, // from peer 1
            Hlc::ZERO,
            move |r| { let _ = tx.send(r); },
        ).unwrap();

        // hub に subscriber が現れるまで待つ
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while hub.subscriber_count() == 0 {
            if std::time::Instant::now() > deadline { panic!("subscriber not registered"); }
            std::thread::sleep(Duration::from_millis(20));
        }

        // 3 record broadcast
        hub.broadcast(1, &[
            rec(100, 1, 10, 100),
            rec(200, 1, 20, 200),
            rec(300, 1, 30, 300),
        ]);

        // 受信確認
        let mut got = Vec::new();
        for _ in 0..3 {
            let r = rx.recv_timeout(Duration::from_secs(10)).expect("timeout");
            got.push(r);
        }
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].hlc.wall, 100);
        assert_eq!(got[2].hlc.wall, 300);
    }

    #[test]
    fn since_filter_excludes_old_records() {
        let hub = WsPushHub::start("127.0.0.1:0").unwrap();
        let url = format!("ws://{}/push", hub.addr());

        let (tx, rx) = mpsc::channel();
        let _client = WsPushClient::connect_and_run(
            &url, 1,
            Hlc { wall: 150, logical: 0, peer: 1 }, // wall=150 より新しいのだけ
            move |r| { let _ = tx.send(r); },
        ).unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while hub.subscriber_count() == 0 {
            if std::time::Instant::now() > deadline { panic!("subscriber not registered"); }
            std::thread::sleep(Duration::from_millis(20));
        }

        hub.broadcast(1, &[
            rec(100, 1, 10, 100), // < since、来ないはず
            rec(200, 1, 20, 200),
            rec(300, 1, 30, 300),
        ]);

        // 200, 300 のみ
        let r1 = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let r2 = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(r1.hlc.wall, 200);
        assert_eq!(r2.hlc.wall, 300);

        let extra = rx.recv_timeout(Duration::from_millis(200));
        assert!(extra.is_err(), "should not receive third (filtered)");
    }

    /// #228: relay link に張った subscriber が、 **中継された古い HLC の record** を
    /// 落とさないこと。
    ///
    /// relay の stream は「自 row (自 clock)」 と 「中継 row (原 author の HLC 素通し
    /// #209)」 の merge なので **HLC が単調でない**。 scalar な `since` は
    /// 「単調な単一 stream」 を前提にしているので、 relay 自身の新しい row を跨いだ
    /// cursor で subscribe すると、 後から中継された author の古い record が
    /// filter で永久に落ちる (#216 の push 版)。
    #[test]
    fn relayed_old_hlc_record_is_not_filtered_out() {
        let hub = WsPushHub::start("127.0.0.1:0").unwrap();
        let url = format!("ws://{}/push", hub.addr());

        // link = relay(2)。 relay 自身の row は wall 300 まで消化済み、
        // author 1 は未知 (= ZERO 起点)。
        let (tx, rx) = mpsc::channel();
        let _client = WsPushClient::connect_and_run_multi(
            &url,
            2,
            &[(2, Hlc { wall: 300, logical: 0, peer: 2 })],
            move |r| {
                let _ = tx.send(r);
            },
        )
        .unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while hub.subscriber_count() == 0 {
            if std::time::Instant::now() > deadline {
                panic!("subscriber not registered");
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        // relay が 「自 row (wall 400)」 と 「中継 row (author 1、 wall 100)」 を
        // まとめて push する。
        hub.broadcast(
            2,
            &[rec(100, 1, 10, 100), rec(400, 2, 20, 400)],
        );

        let mut got: Vec<WireRecord> = Vec::new();
        for _ in 0..2 {
            match rx.recv_timeout(Duration::from_secs(5)) {
                Ok(r) => got.push(r),
                Err(_) => break,
            }
        }
        let authors: Vec<PeerId> = got.iter().map(|r| r.author_peer).collect();
        assert!(
            authors.contains(&1),
            "中継された author 1 の古い record (wall 100) が scalar since で落ちている \
             — 届いたのは {authors:?}"
        );
        assert!(authors.contains(&2), "relay 自身の row も届くこと: {authors:?}");
    }

    /// legacy (scalar `since`) の subscriber は従来どおり全 author への下限として
    /// 効くこと — 「単一 author の stream に張っている」 という宣言なので、 その
    /// 契約は変えない。
    #[test]
    fn scalar_since_still_applies_as_baseline() {
        let hub = WsPushHub::start("127.0.0.1:0").unwrap();
        let url = format!("ws://{}/push", hub.addr());

        let (tx, rx) = mpsc::channel();
        let _client = WsPushClient::connect_and_run(
            &url,
            2,
            Hlc { wall: 300, logical: 0, peer: 2 },
            move |r| {
                let _ = tx.send(r);
            },
        )
        .unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while hub.subscriber_count() == 0 {
            if std::time::Instant::now() > deadline {
                panic!("subscriber not registered");
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        hub.broadcast(2, &[rec(100, 1, 10, 100), rec(400, 2, 20, 400)]);

        let first = rx.recv_timeout(Duration::from_secs(5)).expect("wall 400 は届く");
        assert_eq!(first.hlc.wall, 400);
        assert!(
            rx.recv_timeout(Duration::from_millis(300)).is_err(),
            "scalar since の baseline が効いていない"
        );
    }

    #[test]
    fn broadcast_to_other_peer_does_not_reach() {
        let hub = WsPushHub::start("127.0.0.1:0").unwrap();
        let url = format!("ws://{}/push", hub.addr());

        let (tx, rx) = mpsc::channel();
        let _client = WsPushClient::connect_and_run(
            &url, 1, Hlc::ZERO, // from=1
            move |r| { let _ = tx.send(r); },
        ).unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while hub.subscriber_count() == 0 {
            if std::time::Instant::now() > deadline { panic!("not registered"); }
            std::thread::sleep(Duration::from_millis(20));
        }

        // peer=2 の broadcast → from=1 の subscriber には来ない
        hub.broadcast(2, &[rec(100, 2, 1, 100)]);
        let r = rx.recv_timeout(Duration::from_millis(300));
        assert!(r.is_err(), "subscriber should not receive other peer's records");

        // peer=1 の broadcast → 来る
        hub.broadcast(1, &[rec(100, 1, 1, 100)]);
        let r = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(r.hlc.peer, 1);
    }

    // ───── ChangeListener adapter integration ─────

    #[test]
    fn changelistener_adapter_pushes_engine_commits_to_subscriber() {
        // engine の WAL commit が adapter 経由で subscriber に届くか
        use enchudb::{Engine, ValueType};
        use std::sync::Arc;

        let path = format!(
            "/tmp/enchudb-ws-adapter-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        for suffix in ["", ".oplog", ".crc"] {
            let _ = std::fs::remove_file(format!("{}{}", path, suffix));
        }
        {
            let mut eng = Engine::create_standalone(&path).unwrap();
            eng.define_himo("v", ValueType::Number, 100);
            eng.flush().unwrap();
        }
        let eng = Engine::open_concurrent_with_oplog(&path, 4 * 1024 * 1024).unwrap();
        eng.set_peer_id(1);

        // hub + subscriber
        let hub = Arc::new(WsPushHub::start("127.0.0.1:0").unwrap());
        let url = format!("ws://{}/push", hub.addr());
        let (tx, rx) = mpsc::channel();
        let _client = WsPushClient::connect_and_run(
            &url,
            1,
            Hlc::ZERO,
            move |r| { let _ = tx.send(r); },
        )
        .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while hub.subscriber_count() == 0 {
            if std::time::Instant::now() > deadline {
                panic!("subscriber not registered");
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        // engine に adapter attach
        eng.add_change_listener(Arc::new(WsPushHubAdapter::new(hub.clone(), 1)));

        // commit
        let e = eng.entity().unwrap();
        eng.tie_async(e, "v", 777);
        eng.oplog_commit();
        eng.flush_writes();
        eng.oplog_sync().unwrap();

        // subscriber に届く
        let mut got_tie = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(r) => {
                    if matches!(r.op, DecodedOp::Tie { value: 777, .. }) {
                        got_tie = true;
                        break;
                    }
                }
                Err(_) => continue,
            }
        }
        assert!(got_tie, "subscriber should receive Tie value=777 via adapter");

        drop(eng);
        for suffix in ["", ".oplog", ".crc"] {
            let _ = std::fs::remove_file(format!("{}{}", path, suffix));
        }
    }
}
