//! dist_dashboard — 分散 enchudb を生で見るライブ GUI ダッシュボード。
//!
//! 1 プロセスに origin + replica × 3 を同居させ、sabitori で 4 分割ビューに
//! 各 peer の records / HLC / lag を live 表示する。
//!
//! 起動:
//! ```bash
//! cargo run --features v32 --example dist_dashboard --release
//! ```

#![cfg(feature = "v32")]

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use parking_lot_like::RwLock;

use sabitori::*;
use sabitori_core::tui::{block, hsep};
use sabitori_style::Theme;

use enchudb::{Engine, HimoType, Hlc};
use enchudb::sync::Syncer;
use enchudb::transport::{Transport, WireRecord};
use enchudb::http_transport::{HttpRelay, HttpTransport};
use enchudb::wal::DecodedOp;

// std::sync::RwLock でも十分だが、UI thread で短い read lock を多数取るので alias に。
mod parking_lot_like {
    pub use std::sync::RwLock;
}

// ────────────────────────────────────────────────────────────
// rqlite client module (std::net のみ、3-node subprocess cluster)
// ────────────────────────────────────────────────────────────
mod rqlite {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};

    pub struct Cluster {
        pub http_ports: Vec<u16>,
        children: Vec<Child>,
        data_dirs: Vec<String>,
    }

    impl Cluster {
        /// 3 node rqlited を spawn して voter 3 人揃うまで待つ。
        /// rqlited が PATH に無ければ Err。
        pub fn start(base_port: u16) -> std::io::Result<Self> {
            Command::new("rqlited").arg("-version").output()
                .map_err(|_| std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "rqlited not in PATH",
                ))?;

            let pid = std::process::id();
            let mut http_ports = Vec::new();
            let mut children = Vec::new();
            let mut data_dirs = Vec::new();
            for i in 0u32..3 {
                let http = base_port + (i as u16) * 2;
                let raft = base_port + (i as u16) * 2 + 1;
                let nid = i + 1;
                let data = format!("/tmp/enchu_rqlite_{}_node{}", pid, nid);
                let _ = std::fs::remove_dir_all(&data);
                let mut cmd = Command::new("rqlited");
                cmd.arg("-node-id").arg(nid.to_string())
                    .arg("-http-addr").arg(format!("127.0.0.1:{}", http))
                    .arg("-raft-addr").arg(format!("127.0.0.1:{}", raft));
                if i > 0 {
                    cmd.arg("-join").arg(format!("127.0.0.1:{}", base_port + 1));
                }
                cmd.arg(&data);
                cmd.stdout(Stdio::null()).stderr(Stdio::null());
                children.push(cmd.spawn()?);
                http_ports.push(http);
                data_dirs.push(data);
                std::thread::sleep(Duration::from_millis(if i == 0 { 1500 } else { 800 }));
            }
            // 全 node 揃うまで待つ
            let deadline = Instant::now() + Duration::from_secs(15);
            while Instant::now() < deadline {
                if let Ok(body) = http_get(&format!("127.0.0.1:{}", base_port), "/nodes") {
                    if body.contains("\"3\":") { break; }
                }
                std::thread::sleep(Duration::from_millis(300));
            }
            Ok(Self { http_ports, children, data_dirs })
        }

        pub fn execute(&self, sql: &str) -> std::io::Result<String> {
            let host = format!("127.0.0.1:{}", self.http_ports[0]);
            let body = format!("[{}]", json_string(sql));
            http_post(&host, "/db/execute", &body)
        }

        /// 各 node の count を並行じゃなく順番に取る (UI thread から叩かれる想定なので単純で OK)
        pub fn count(&self, node_idx: usize, table: &str) -> std::io::Result<u64> {
            let host = format!("127.0.0.1:{}", self.http_ports[node_idx]);
            let sql = format!("SELECT COUNT(*) FROM {}", table);
            let body = format!("[{}]", json_string(&sql));
            let resp = http_post(&host, "/db/query", &body)?;
            parse_count(&resp)
        }
    }

    impl Drop for Cluster {
        fn drop(&mut self) {
            for c in &mut self.children { let _ = c.kill(); }
            for c in &mut self.children { let _ = c.wait(); }
            for d in &self.data_dirs { let _ = std::fs::remove_dir_all(d); }
        }
    }

    fn http_get(host_port: &str, path: &str) -> std::io::Result<String> {
        let addr = host_port.parse().map_err(|_|
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "addr"))?;
        let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
        s.set_read_timeout(Some(Duration::from_secs(2)))?;
        write!(s, "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", path, host_port)?;
        s.flush()?;
        let mut buf = Vec::new();
        s.read_to_end(&mut buf)?;
        split_body(String::from_utf8_lossy(&buf).to_string())
    }

    fn http_post(host_port: &str, path: &str, body: &str) -> std::io::Result<String> {
        let addr = host_port.parse().map_err(|_|
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "addr"))?;
        let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
        s.set_read_timeout(Some(Duration::from_secs(2)))?;
        write!(s,
            "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            path, host_port, body.len(), body)?;
        s.flush()?;
        let mut buf = Vec::new();
        s.read_to_end(&mut buf)?;
        split_body(String::from_utf8_lossy(&buf).to_string())
    }

    fn split_body(s: String) -> std::io::Result<String> {
        if let Some(pos) = s.find("\r\n\r\n") { Ok(s[pos+4..].to_string()) } else { Ok(s) }
    }

    fn json_string(s: &str) -> String {
        let mut out = String::with_capacity(s.len() + 2);
        out.push('"');
        for c in s.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                c => out.push(c),
            }
        }
        out.push('"');
        out
    }

    fn parse_count(body: &str) -> std::io::Result<u64> {
        let key = "\"values\":[[";
        let idx = body.find(key).ok_or_else(||
            std::io::Error::new(std::io::ErrorKind::InvalidData, "no values"))?;
        let rest = &body[idx + key.len()..];
        let end = rest.find(']').ok_or_else(||
            std::io::Error::new(std::io::ErrorKind::InvalidData, "no ]"))?;
        rest[..end].trim().parse().map_err(|_|
            std::io::Error::new(std::io::ErrorKind::InvalidData, "not int"))
    }
}

// ────────────────────────────────────────────────────────────
// Cluster 共有 state
// ────────────────────────────────────────────────────────────

#[derive(Clone, Default)]
struct PeerState {
    label: String,
    peer_id: u32,
    records: usize,
    latest_hlc_wall: u64,
    lag_samples: VecDeque<u64>, // ms、直近 60 秒
    last_update_at: Option<Instant>,
}

#[derive(Default)]
struct ClusterState {
    peers: Vec<PeerState>,          // enchudb peers (origin + 3 replicas)
    rqlite_peers: Vec<PeerState>,   // rqlite nodes (leader + 2 followers), 空なら未起動
    rqlite_enabled: bool,
    total_published: usize,
    publish_rate_per_sec: f32,
    started_at: Option<Instant>,
}

// ────────────────────────────────────────────────────────────
// App
// ────────────────────────────────────────────────────────────

struct Dashboard {
    theme: Theme,
    state: Arc<RwLock<ClusterState>>,
    shutdown: Arc<AtomicBool>,
}

impl DeclarativeApp for Dashboard {
    fn title(&self) -> &str { "enchudb dist dashboard" }
    fn size(&self) -> (f32, f32) { (1200.0, 740.0) }

    fn fonts(&self) -> Vec<Vec<u8>> {
        vec![
            include_bytes!("../../sabitori/assets/fonts/Hack-Regular.ttf").to_vec(),
            include_bytes!("../../sabitori/assets/fonts/Hack-Bold.ttf").to_vec(),
        ]
    }

    fn view(&self, ctx: &ViewContext) -> Element {
        let t = &self.theme;
        let a = &t.ansi;
        let bg = Color::from_hex("#08080c");

        let state = self.state.read().unwrap();

        // ── 各 peer の quadrant 用データ ──
        let mk_quadrant = |ps: &PeerState| -> Element {
            let role = if ps.peer_id == 1 { "ORIGIN" } else { "REPLICA" };
            let role_color = if ps.peer_id == 1 { a.bright_green } else { a.bright_cyan };
            let records_str = format!("{}", ps.records);
            let hlc_str = format!("{}", ps.latest_hlc_wall);
            let lag_avg: u64 = if ps.lag_samples.is_empty() {
                0
            } else {
                ps.lag_samples.iter().sum::<u64>() / ps.lag_samples.len() as u64
            };
            let lag_str = if ps.peer_id == 1 {
                "—".to_string()
            } else {
                format!("{} ms", lag_avg)
            };

            // sparkline
            let spark = sparkline_string(&ps.lag_samples, 40);

            block(&ps.label)
                .border_color(t.border)
                .title_color(role_color)
                .bg(t.surface)
                .children([
                    div().flex_col().gap(4.0).children([
                        div().flex_row().gap(8.0).children([
                            text("role").mono().font_size(10.0).color(t.text_disabled).w(Px(64.0)),
                            text(role).mono().font_size(11.0).color(role_color).bold(),
                        ]),
                        div().flex_row().gap(8.0).children([
                            text("peer_id").mono().font_size(10.0).color(t.text_disabled).w(Px(64.0)),
                            text(&format!("{}", ps.peer_id)).mono().font_size(11.0).color(t.text_primary),
                        ]),
                        div().flex_row().gap(8.0).children([
                            text("records").mono().font_size(10.0).color(t.text_disabled).w(Px(64.0)),
                            text(&records_str).mono().font_size(14.0).bold().color(a.bright_yellow),
                        ]),
                        div().flex_row().gap(8.0).children([
                            text("hlc.wall").mono().font_size(10.0).color(t.text_disabled).w(Px(64.0)),
                            text(&hlc_str).mono().font_size(11.0).color(t.text_secondary),
                        ]),
                        div().flex_row().gap(8.0).children([
                            text("lag").mono().font_size(10.0).color(t.text_disabled).w(Px(64.0)),
                            text(&lag_str).mono().font_size(11.0).color(
                                if lag_avg < 20 { a.green }
                                else if lag_avg < 100 { a.yellow }
                                else { a.bright_red }
                            ),
                        ]),
                        div().h(Px(4.0)),
                        text(&spark).mono().font_size(10.0).color(a.cyan.with_alpha(0.8)),
                    ]),
                ])
        };

        let rqlite_on = state.rqlite_enabled;

        let peers = &state.peers;
        let (p0, p1, p2, p3) = (
            peers.get(0).cloned().unwrap_or_default(),
            peers.get(1).cloned().unwrap_or_default(),
            peers.get(2).cloned().unwrap_or_default(),
            peers.get(3).cloned().unwrap_or_default(),
        );

        let (rq0, rq1, rq2) = (
            state.rqlite_peers.get(0).cloned().unwrap_or_default(),
            state.rqlite_peers.get(1).cloned().unwrap_or_default(),
            state.rqlite_peers.get(2).cloned().unwrap_or_default(),
        );

        let total_published = state.total_published;
        let rate = state.publish_rate_per_sec;
        let uptime_secs = state.started_at
            .map(|s| s.elapsed().as_secs())
            .unwrap_or(0);

        drop(state);

        // enchudb: top half 4 分割、rqlite: bottom half 3 分割
        // rqlite 無効なら enchudb が全画面 4 分割
        let half_h = if rqlite_on { (ctx.height - 150.0) / 2.0 } else { ctx.height - 120.0 };
        let enchu_quadrant_w = (ctx.width - 36.0) / 2.0;
        let enchu_quadrant_h = if rqlite_on { (half_h - 40.0) / 2.0 } else { (half_h - 0.0) / 2.0 };
        let rqlite_card_w = (ctx.width - 44.0) / 3.0;
        let rqlite_card_h = half_h - 40.0;

        div()
            .w(Px(ctx.width)).h(Px(ctx.height))
            .bg(bg)
            .flex_col()
            .children([
                // header
                div().w_full().h(Px(28.0)).shrink(0.0)
                    .bg(t.surface_elevated)
                    .flex_row().items_center()
                    .px_pad(Px(10.0))
                    .children([
                        text("enchudb").mono().bold().font_size(12.0).color(t.primary).shrink(0.0),
                        text(" / ").mono().font_size(12.0).color(t.text_disabled).shrink(0.0),
                        text("distributed dashboard").mono().font_size(12.0).color(t.text_secondary).shrink(0.0),
                        div().flex_1(),
                        inline_stat(t, "published", &format!("{}", total_published), a.bright_yellow),
                        div().w(Px(12.0)).shrink(0.0),
                        inline_stat(t, "rate", &format!("{:.1}/s", rate), a.bright_cyan),
                        div().w(Px(12.0)).shrink(0.0),
                        inline_stat(t, "up", &format!("{}s", uptime_secs), a.green),
                    ]),
                hsep(t.border),

                // enchudb section
                div().flex_col().w_full().shrink(0.0).p_px(8.0).gap(6.0).children([
                    div().flex_row().items_center().gap(8.0).children([
                        text("── enchudb cluster").mono().font_size(11.0).color(a.bright_green).bold(),
                        text("origin + 3 replicas, HTTP pull sync").mono().font_size(10.0).color(t.text_disabled),
                    ]),
                    div().flex_col().gap(6.0).children([
                        div().flex_row().gap(8.0).children([
                            div().w(Px(enchu_quadrant_w)).h(Px(enchu_quadrant_h)).children([mk_quadrant(&p0)]),
                            div().w(Px(enchu_quadrant_w)).h(Px(enchu_quadrant_h)).children([mk_quadrant(&p1)]),
                        ]),
                        div().flex_row().gap(8.0).children([
                            div().w(Px(enchu_quadrant_w)).h(Px(enchu_quadrant_h)).children([mk_quadrant(&p2)]),
                            div().w(Px(enchu_quadrant_w)).h(Px(enchu_quadrant_h)).children([mk_quadrant(&p3)]),
                        ]),
                    ]),
                ]),
                hsep(t.border),

                // rqlite section (enabled なら表示)
                if rqlite_on {
                    div().flex_col().w_full().shrink(0.0).p_px(8.0).gap(6.0).children([
                        div().flex_row().items_center().gap(8.0).children([
                            text("── rqlite cluster").mono().font_size(11.0).color(a.bright_yellow).bold(),
                            text("Raft + SQLite, 3 nodes").mono().font_size(10.0).color(t.text_disabled),
                        ]),
                        div().flex_row().gap(8.0).children([
                            div().w(Px(rqlite_card_w)).h(Px(rqlite_card_h)).children([mk_quadrant(&rq0)]),
                            div().w(Px(rqlite_card_w)).h(Px(rqlite_card_h)).children([mk_quadrant(&rq1)]),
                            div().w(Px(rqlite_card_w)).h(Px(rqlite_card_h)).children([mk_quadrant(&rq2)]),
                        ]),
                    ])
                } else {
                    div().flex_col().w_full().shrink(0.0).p_px(8.0).children([
                        text("rqlite not available (brew install rqlite to enable vs mode)")
                            .mono().font_size(10.0).color(t.text_disabled),
                    ])
                },

                div().flex_1(),

                // footer
                hsep(t.border),
                div().w_full().h(Px(22.0)).shrink(0.0)
                    .bg(t.surface_elevated)
                    .flex_row().items_center()
                    .px_pad(Px(10.0))
                    .children([
                        text("enchudb v32 — in-process cluster demo").mono().font_size(10.0).color(t.text_disabled),
                        div().flex_1(),
                        text("[q] quit").mono().font_size(10.0).color(t.text_disabled),
                    ]),
            ])
    }

    fn on_click(&mut self, _id: &str) {}

    fn on_input(&mut self, event: &InputEvent) -> bool {
        match event {
            InputEvent::KeyInput { key: Key::Q, pressed: true, .. }
            | InputEvent::KeyInput { key: Key::Escape, pressed: true, .. } => {
                self.shutdown.store(true, Ordering::Release);
                std::process::exit(0);
            }
            _ => false,
        }
    }
}

fn inline_stat(t: &Theme, label: &str, value: &str, color: Color) -> Element {
    div().shrink(0.0).flex_row().gap(4.0).items_center().children([
        text(label).mono().font_size(10.0).color(t.text_disabled),
        text(value).mono().font_size(10.0).color(color).bold(),
    ])
}

// ────────────────────────────────────────────────────────────
// sparkline
// ────────────────────────────────────────────────────────────

const SPARK_CHARS: &[char] = &['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

fn sparkline_string(samples: &VecDeque<u64>, width: usize) -> String {
    if samples.is_empty() {
        return "▁".repeat(width);
    }
    let max = *samples.iter().max().unwrap_or(&1).max(&1);
    let taken: Vec<u64> = samples.iter().rev().take(width).rev().copied().collect();
    let pad = width.saturating_sub(taken.len());
    let mut out = String::with_capacity(width);
    for _ in 0..pad { out.push('▁'); }
    for v in &taken {
        let idx = ((*v as f64 / max as f64) * (SPARK_CHARS.len() as f64 - 1.0)).round() as usize;
        out.push(SPARK_CHARS[idx.min(SPARK_CHARS.len() - 1)]);
    }
    out
}

// ────────────────────────────────────────────────────────────
// Cluster worker
// ────────────────────────────────────────────────────────────

fn spawn_cluster(state: Arc<RwLock<ClusterState>>, shutdown: Arc<AtomicBool>) {
    std::thread::spawn(move || {
        // DB paths
        let pid = std::process::id();
        let paths: Vec<String> = (0..4)
            .map(|i| format!("/tmp/enchu_dashboard_{}_peer{}.db", pid, i))
            .collect();
        for p in &paths {
            for suf in ["", ".wal", ".crc"] {
                let _ = std::fs::remove_file(format!("{}{}", p, suf));
            }
        }

        // peer 0 = origin
        {
            let mut eng = Engine::create_compact(&paths[0]).unwrap();
            eng.define_himo("val", HimoType::Value, 100);
            eng.flush().unwrap();
        }
        let relay = HttpRelay::start_with_bootstrap("127.0.0.1:0", &paths[0]).unwrap();
        let url = format!("http://{}", relay.addr());

        // origin Engine は別 open (publish 用、今回は transport 直接使うので Engine 要らない)
        // ただ records count 取るため read-only で open しておく
        let origin_eng = Engine::open_replica(&paths[0]).unwrap();

        // replicas 3 つ
        let mut replicas: Vec<Arc<Engine>> = Vec::new();
        let mut pull_threads = Vec::new();
        for i in 1..4 {
            let client = HttpTransport::new(url.clone());
            let _ = client.bootstrap_to(&paths[i]).unwrap();
            let eng = Arc::new(Engine::open_replica(&paths[i]).unwrap());
            let peer_id = (i as u32) + 10; // 11, 12, 13
            eng.set_peer_id(peer_id);
            replicas.push(eng.clone());

            let url_clone = url.clone();
            let sh = shutdown.clone();
            let t = std::thread::spawn(move || {
                let transport: Arc<dyn Transport> = Arc::new(HttpTransport::new(url_clone));
                let syncer = Syncer::new(eng, transport);
                while !sh.load(Ordering::Acquire) {
                    let _ = syncer.pull_once(1);
                    std::thread::sleep(Duration::from_millis(100));
                }
            });
            pull_threads.push(t);
        }

        // rqlite cluster 起動試行
        let rqlite_cluster: Option<rqlite::Cluster> = match rqlite::Cluster::start(14401) {
            Ok(c) => {
                eprintln!("[rqlite] 3-node cluster up");
                // CREATE TABLE を leader に投げる
                let _ = c.execute("CREATE TABLE IF NOT EXISTS entities (eid INTEGER PRIMARY KEY, value INTEGER)");
                Some(c)
            }
            Err(e) => {
                eprintln!("[rqlite] not available: {} — enchudb only mode", e);
                None
            }
        };

        // 初期 state
        {
            let mut s = state.write().unwrap();
            s.started_at = Some(Instant::now());
            s.peers = vec![
                PeerState { label: "peer 1 · origin".to_string(), peer_id: 1, ..Default::default() },
                PeerState { label: "peer 11 · replica".to_string(), peer_id: 11, ..Default::default() },
                PeerState { label: "peer 12 · replica".to_string(), peer_id: 12, ..Default::default() },
                PeerState { label: "peer 13 · replica".to_string(), peer_id: 13, ..Default::default() },
            ];
            s.rqlite_enabled = rqlite_cluster.is_some();
            if s.rqlite_enabled {
                s.rqlite_peers = vec![
                    PeerState { label: "rqlite 1 · leader".to_string(), peer_id: 1, ..Default::default() },
                    PeerState { label: "rqlite 2 · follower".to_string(), peer_id: 2, ..Default::default() },
                    PeerState { label: "rqlite 3 · follower".to_string(), peer_id: 3, ..Default::default() },
                ];
            }
        }

        // publish loop — 毎 200ms に 1 record
        let pub_t = HttpTransport::new(url.clone());
        let himo_id = origin_eng.himo_id("val").unwrap() as u16;
        let mut counter = 0u32;
        let publish_tick = Duration::from_millis(200);
        let mut last_pub = Instant::now();
        let mut pending_writes: Vec<(u32, Instant)> = Vec::new(); // (local_id, publish_time)

        // state update ループ
        let mut last_count = 0usize;
        let mut last_rate_sample = Instant::now();
        let mut last_rqlite_poll = Instant::now() - Duration::from_secs(5);
        let rqlite_poll_interval = Duration::from_millis(500);
        loop {
            if shutdown.load(Ordering::Acquire) { break; }

            // publish 1 record
            if last_pub.elapsed() >= publish_tick && counter < 60_000 {
                let local = counter + 1;
                let eid = enchudb::make_eid(1, local);
                let wall = now_millis();
                let rec = WireRecord::unsigned(
                    Hlc { wall, logical: 0, peer: 1 }, 1,
                    DecodedOp::Tie { eid, himo_id, value: local },
                );
                pub_t.publish(1, vec![rec]);

                // 同じ record を rqlite にも INSERT (比較用)
                if let Some(rc) = &rqlite_cluster {
                    let sql = format!("INSERT INTO entities (eid, value) VALUES ({}, {})", local, local);
                    let _ = rc.execute(&sql);
                }

                pending_writes.push((local, Instant::now()));
                counter += 1;
                last_pub = Instant::now();
            }

            // state 更新 (16ms ≈ 60fps)
            {
                let mut s = state.write().unwrap();
                s.total_published = counter as usize;

                // rate (1 秒平均)
                if last_rate_sample.elapsed() >= Duration::from_secs(1) {
                    let delta = counter as usize - last_count;
                    s.publish_rate_per_sec = delta as f32;
                    last_count = counter as usize;
                    last_rate_sample = Instant::now();
                }

                // origin
                s.peers[0].records = counter as usize;
                s.peers[0].latest_hlc_wall = now_millis();
                s.peers[0].last_update_at = Some(Instant::now());

                // replicas
                for (i, r) in replicas.iter().enumerate() {
                    let pid = &mut s.peers[i + 1];
                    let count = r.entity_count() as usize;
                    pid.records = count;

                    // lag 計算: pending_writes 内で replica がまだ見てない最古の時刻差
                    // かんたんに: replica が見てる最大 local_id を用いて
                    // pending から該当 local_id 以上の最古 write を探す
                    let visible_local = {
                        let mut max_local = 0u32;
                        // replica は origin の local_id を持ってるので、最新を探す
                        // 簡易: 最後から逆順に get() して最初に Some になるところを探す (1000 件まで)
                        let limit = count.min(1000) as u32;
                        for back in 0..limit {
                            let try_local = counter.saturating_sub(back);
                            if try_local == 0 { break; }
                            let eid = enchudb::make_eid(1, try_local);
                            if r.get(eid, "val").is_some() {
                                max_local = try_local;
                                break;
                            }
                        }
                        max_local
                    };
                    if visible_local > 0 {
                        // pending から visible_local 以上の最初の未解決を探して lag 計算は諦め、
                        // 直近 pending で visible より上がどれくらいあるかを lag 代替にする
                        let unseen = pending_writes.iter()
                            .filter(|(l, _)| *l > visible_local)
                            .collect::<Vec<_>>();
                        let lag_ms = unseen.first()
                            .map(|(_, t)| t.elapsed().as_millis() as u64)
                            .unwrap_or(0);
                        pid.lag_samples.push_back(lag_ms);
                        if pid.lag_samples.len() > 300 {
                            pid.lag_samples.pop_front();
                        }
                    }
                    pid.latest_hlc_wall = now_millis().saturating_sub(
                        pid.lag_samples.back().copied().unwrap_or(0)
                    );
                    pid.last_update_at = Some(Instant::now());
                }

                // pending 掃除: 全 replica に反映済みは捨てる
                let min_replica_count = replicas.iter()
                    .map(|r| r.entity_count() as u32)
                    .min()
                    .unwrap_or(0);
                pending_writes.retain(|(l, _)| *l > min_replica_count);

            }

            // rqlite 各 node の count を取る (500ms 間隔、別ブロックで lock 取り直し)
            if let Some(rc) = &rqlite_cluster {
                if last_rqlite_poll.elapsed() >= rqlite_poll_interval {
                    let mut counts = [0u64; 3];
                    let mut ok = [false; 3];
                    for i in 0..3 {
                        if let Ok(cnt) = rc.count(i, "entities") {
                            counts[i] = cnt;
                            ok[i] = true;
                        }
                    }
                    let cur_counter = counter as u64;
                    let publish_interval_ms = publish_tick.as_millis() as u64;
                    let mut s = state.write().unwrap();
                    for i in 0..3 {
                        if !ok[i] { continue; }
                        let pid = &mut s.rqlite_peers[i];
                        pid.records = counts[i] as usize;
                        let behind = cur_counter.saturating_sub(counts[i]);
                        let lag_ms = behind * publish_interval_ms;
                        pid.lag_samples.push_back(lag_ms);
                        if pid.lag_samples.len() > 300 { pid.lag_samples.pop_front(); }
                        pid.latest_hlc_wall = now_millis();
                        pid.last_update_at = Some(Instant::now());
                    }
                    last_rqlite_poll = Instant::now();
                }
            }

            std::thread::sleep(Duration::from_millis(16));
        }

        // cleanup
        drop(relay);
        for t in pull_threads { let _ = t.join(); }
        for p in &paths {
            for suf in ["", ".wal", ".crc"] {
                let _ = std::fs::remove_file(format!("{}{}", p, suf));
            }
        }
    });
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ────────────────────────────────────────────────────────────
// main
// ────────────────────────────────────────────────────────────

fn main() {
    let state = Arc::new(RwLock::new(ClusterState::default()));
    let shutdown = Arc::new(AtomicBool::new(false));

    spawn_cluster(state.clone(), shutdown.clone());

    sabitori::run_declarative(Dashboard {
        theme: Theme::warp_dark(),
        state,
        shutdown,
    });
}
