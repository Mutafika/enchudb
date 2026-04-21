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
    role: String,
    peer_id: u32,
    records: usize,
    latest_seen: u32,
    latest_seen_at: Option<Instant>,
    last_delta_us: u64,     // 直近 1 件の propagation 時間 (マイクロ秒)
    lag_samples: VecDeque<u64>, // μs で保持
    is_primary: bool,
}

#[derive(Clone, Debug)]
struct EventLine {
    ts_ms: u64,           // epoch ms
    kind: EventKind,
    peer_label: String,
    write_id: u32,
    delta_us: Option<u64>, // propagation in μs
}

#[derive(Clone, Debug)]
enum EventKind {
    Publish,   // origin が書いた
    EnchuSeen, // enchudb replica が見えた
    RqliteSeen, // rqlite follower が見えた
}

#[derive(Default)]
struct ClusterState {
    peers: Vec<PeerState>,          // enchudb peers (origin + 3 replicas)
    rqlite_peers: Vec<PeerState>,   // rqlite nodes (leader + 2 followers)
    rqlite_enabled: bool,
    total_published: usize,
    publish_rate_per_sec: f32,
    started_at: Option<Instant>,
    events: VecDeque<EventLine>,
    latest_write_id: u32,
    // 読み取り QPS 比較 (1 秒毎にバックグラウンドで実測)
    enchudb_read_qps: u64,
    enchudb_read_ns_per_op: u64,
    rqlite_read_qps: u64,
    rqlite_read_ms_per_op: f64,
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

        // origin との遅れ (write 番号 ベース)
        let latest_write_id = state.latest_write_id;

        // ── 各 peer の quadrant 用データ ──
        let mk_quadrant = |ps: &PeerState| -> Element {
            let role = ps.role.as_str();
            let role_color = if ps.is_primary { a.bright_green } else { a.bright_cyan };
            let behind = latest_write_id.saturating_sub(ps.latest_seen);
            let caught_up = behind == 0 && ps.latest_seen > 0;

            // flash: 直近 400ms 以内に update あったらタイトル色を明るく
            let flash = ps.latest_seen_at
                .map(|t| t.elapsed() < Duration::from_millis(400))
                .unwrap_or(false);
            let title_color = if flash { a.bright_yellow } else { role_color };

            let seen_str = if ps.latest_seen == 0 { "—".to_string() } else { format!("#{}", ps.latest_seen) };
            let behind_str = if ps.is_primary {
                "(origin)".to_string()
            } else if caught_up {
                "✓ caught up".to_string()
            } else {
                format!("-{} behind", behind)
            };
            let behind_color = if ps.is_primary { t.text_disabled }
                else if caught_up { a.green }
                else if behind < 5 { a.yellow }
                else { a.bright_red };

            let delta_str = if ps.is_primary {
                "—".to_string()
            } else if ps.last_delta_us == 0 {
                "—".to_string()
            } else {
                format!("Δ {}", fmt_duration_us(ps.last_delta_us))
            };

            let spark = sparkline_string(&ps.lag_samples, 40);

            block(&ps.label)
                .border_color(if flash { a.yellow } else { t.border })
                .title_color(title_color)
                .bg(t.surface)
                .children([
                    div().flex_col().gap(6.0).children([
                        div().flex_row().gap(8.0).children([
                            text(role).mono().font_size(10.0).color(role_color).bold(),
                            text(&format!("#{}", ps.peer_id)).mono().font_size(10.0).color(t.text_disabled),
                        ]),
                        div().flex_row().gap(8.0).items_center().children([
                            text("seen").mono().font_size(10.0).color(t.text_disabled),
                            text(&seen_str).mono().font_size(28.0).bold().color(a.bright_yellow),
                        ]),
                        text(&behind_str).mono().font_size(11.0).color(behind_color).bold(),
                        div().flex_row().gap(8.0).children([
                            text("last propagation").mono().font_size(10.0).color(t.text_disabled),
                            text(&delta_str).mono().font_size(10.0).color(
                                if ps.last_delta_us < 1_000 { a.bright_green }
                                else if ps.last_delta_us < 30_000 { a.green }
                                else if ps.last_delta_us < 200_000 { a.yellow }
                                else { a.bright_red }
                            ).bold(),
                        ]),
                        div().h(Px(4.0)),
                        text(&spark).mono().font_size(10.0).color(a.cyan.with_alpha(0.7)),
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

        // event log (最新 40 行)
        let events: Vec<EventLine> = state.events.iter().rev().take(40).cloned().collect();

        let enchu_qps = state.enchudb_read_qps;
        let enchu_ns = state.enchudb_read_ns_per_op;
        let rq_qps = state.rqlite_read_qps;
        let rq_ms = state.rqlite_read_ms_per_op;

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

                // READ QPS 比較 bar — 本質的な差 (ns 対 ms) を見せる
                {
                    let enchu_qps_str = format_qps(enchu_qps);
                    let rq_qps_str = format_qps(rq_qps);
                    let enchu_ns_str = if enchu_ns == 0 { "—".to_string() } else { format!("{} ns/op", enchu_ns) };
                    let rq_ms_str = if rq_ms == 0.0 { "—".to_string() } else { format!("{:.2} ms/op", rq_ms) };
                    let ratio_str = if rq_qps > 0 && enchu_qps > rq_qps {
                        let r = enchu_qps / rq_qps;
                        if r >= 1_000_000 { format!("{}M×", r / 1_000_000) }
                        else if r >= 1_000 { format!("{}k×", r / 1_000) }
                        else { format!("{}×", r) }
                    } else { "—".to_string() };

                    div().w_full().h(Px(64.0)).shrink(0.0)
                        .bg(Color::from_hex("#0f0f16"))
                        .flex_row().items_center()
                        .px_pad(Px(12.0))
                        .gap(24.0)
                        .children([
                            text("READ QPS (single-node replica / follower)").mono().font_size(10.0).color(t.text_disabled).shrink(0.0).w(Px(170.0)),
                            div().flex_col().gap(2.0).children([
                                text("enchudb").mono().font_size(10.0).color(a.bright_green).bold(),
                                text(&enchu_qps_str).mono().font_size(20.0).bold().color(a.bright_green),
                                text(&enchu_ns_str).mono().font_size(9.0).color(t.text_disabled),
                            ]),
                            text("vs").mono().font_size(14.0).color(t.text_disabled),
                            div().flex_col().gap(2.0).children([
                                text("rqlite").mono().font_size(10.0).color(a.bright_yellow).bold(),
                                text(&rq_qps_str).mono().font_size(20.0).bold().color(a.bright_yellow),
                                text(&rq_ms_str).mono().font_size(9.0).color(t.text_disabled),
                            ]),
                            div().flex_1(),
                            div().flex_col().gap(2.0).items_end().children([
                                text("enchudb ahead by").mono().font_size(10.0).color(t.text_disabled),
                                text(&ratio_str).mono().font_size(24.0).bold().color(a.bright_cyan),
                            ]),
                        ])
                },
                hsep(t.border),

                // main body: 左=クラスタパネル、右=event log
                div().flex_row().flex_1().w_full().gap(8.0).p_px(8.0).children([
                    // 左: cluster panels
                    div().flex_col().flex_1().gap(6.0).children([
                        // enchudb section
                        div().flex_row().items_center().gap(8.0).children([
                            text("── enchudb cluster").mono().font_size(11.0).color(a.bright_green).bold(),
                            text("origin + 3 replicas, HTTP pull sync").mono().font_size(10.0).color(t.text_disabled),
                        ]),
                        div().flex_col().gap(6.0).children([
                            div().flex_row().gap(8.0).children([
                                div().flex_1().h(Px(enchu_quadrant_h)).children([mk_quadrant(&p0)]),
                                div().flex_1().h(Px(enchu_quadrant_h)).children([mk_quadrant(&p1)]),
                            ]),
                            div().flex_row().gap(8.0).children([
                                div().flex_1().h(Px(enchu_quadrant_h)).children([mk_quadrant(&p2)]),
                                div().flex_1().h(Px(enchu_quadrant_h)).children([mk_quadrant(&p3)]),
                            ]),
                        ]),

                        hsep(t.border),

                        if rqlite_on {
                            div().flex_col().gap(6.0).children([
                                div().flex_row().items_center().gap(8.0).children([
                                    text("── rqlite cluster").mono().font_size(11.0).color(a.bright_yellow).bold(),
                                    text("Raft + SQLite, 3 nodes").mono().font_size(10.0).color(t.text_disabled),
                                ]),
                                div().flex_row().gap(8.0).children([
                                    div().flex_1().h(Px(rqlite_card_h)).children([mk_quadrant(&rq0)]),
                                    div().flex_1().h(Px(rqlite_card_h)).children([mk_quadrant(&rq1)]),
                                    div().flex_1().h(Px(rqlite_card_h)).children([mk_quadrant(&rq2)]),
                                ]),
                            ])
                        } else {
                            div().flex_col().children([
                                text("rqlite not available (brew install rqlite to enable vs mode)")
                                    .mono().font_size(10.0).color(t.text_disabled),
                            ])
                        },
                    ]),

                    // 右: event log
                    div().w(Px(360.0)).flex_col().gap(6.0).children([
                        text("── live propagation").mono().font_size(11.0).color(t.primary).bold(),
                        div().flex_1().flex_col().gap(1.0)
                            .bg(t.surface)
                            .p_px(6.0)
                            .children(event_log_rows(t, &events)),
                    ]),
                ]),

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

fn event_log_rows(t: &Theme, events: &[EventLine]) -> Vec<Element> {
    let a = &t.ansi;
    events.iter().map(|e| {
        let (prefix, color) = match e.kind {
            EventKind::Publish => ("WRITE  →", a.bright_green),
            EventKind::EnchuSeen => ("enchu  ←", a.bright_cyan),
            EventKind::RqliteSeen => ("rqlite ←", a.bright_yellow),
        };
        let ms_frac = e.ts_ms % 1000;
        let secs = (e.ts_ms / 1000) % 60;
        let mins = (e.ts_ms / 60_000) % 60;
        let time_str = format!("{:02}:{:02}.{:03}", mins, secs, ms_frac);
        let delta_str = e.delta_us.map(|d| format!(" +{}", fmt_duration_us(d))).unwrap_or_default();
        div().flex_row().gap(4.0).h(Px(14.0)).items_center().children([
            text(&time_str).mono().font_size(9.0).color(t.text_disabled).w(Px(64.0)).shrink(0.0),
            text(prefix).mono().font_size(9.0).color(color).w(Px(54.0)).shrink(0.0).bold(),
            text(&e.peer_label).mono().font_size(9.0).color(t.text_primary).w(Px(80.0)).shrink(0.0),
            text(&format!("#{}{}", e.write_id, delta_str)).mono().font_size(9.0).color(t.text_secondary),
        ])
    }).collect()
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
                    // tight pull で propagation を sub-ms に
                    std::thread::sleep(Duration::from_millis(2));
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
                PeerState { label: "peer 1 · origin".to_string(), role: "ORIGIN".into(), peer_id: 1, is_primary: true, ..Default::default() },
                PeerState { label: "peer 11 · replica".to_string(), role: "REPLICA".into(), peer_id: 11, ..Default::default() },
                PeerState { label: "peer 12 · replica".to_string(), role: "REPLICA".into(), peer_id: 12, ..Default::default() },
                PeerState { label: "peer 13 · replica".to_string(), role: "REPLICA".into(), peer_id: 13, ..Default::default() },
            ];
            s.rqlite_enabled = rqlite_cluster.is_some();
            if s.rqlite_enabled {
                s.rqlite_peers = vec![
                    PeerState { label: "rqlite 1 · leader".to_string(), role: "LEADER".into(), peer_id: 1, is_primary: true, ..Default::default() },
                    PeerState { label: "rqlite 2 · follower".to_string(), role: "FOLLOWER".into(), peer_id: 2, ..Default::default() },
                    PeerState { label: "rqlite 3 · follower".to_string(), role: "FOLLOWER".into(), peer_id: 3, ..Default::default() },
                ];
            }
        }

        // read QPS 計測用のバックグラウンドスレッド:
        // enchudb replica に get() を叩きまくる + rqlite に HTTP query を叩きまくる
        // 1 秒毎に ClusterState を更新
        {
            let replica_for_bench = replicas[0].clone();
            let state_for_bench = state.clone();
            let sh_bench = shutdown.clone();
            let url_for_bench = url.clone();
            let rqlite_ports: Option<Vec<u16>> = rqlite_cluster.as_ref()
                .map(|rc| rc.http_ports.clone());
            std::thread::spawn(move || {
                let _ = url_for_bench;
                let mut last_run = Instant::now();
                loop {
                    if sh_bench.load(Ordering::Acquire) { break; }
                    std::thread::sleep(Duration::from_millis(100));
                    if last_run.elapsed() < Duration::from_secs(1) { continue; }
                    last_run = Instant::now();

                    // enchudb read bench: 100k ops
                    let iters = 100_000u64;
                    let t0 = Instant::now();
                    let mut sink: u64 = 0;
                    let ec = replica_for_bench.entity_count().max(1);
                    for i in 0..iters {
                        let eid = enchudb::make_eid(1, (i as u32 % ec) + 1);
                        if let Some(v) = replica_for_bench.get(eid, "val") {
                            sink = sink.wrapping_add(v as u64);
                        }
                    }
                    let enchu_elapsed = t0.elapsed();
                    let enchu_qps = (iters as f64 / enchu_elapsed.as_secs_f64()) as u64;
                    let enchu_ns = enchu_elapsed.as_nanos() as u64 / iters;
                    let _ = sink;

                    // rqlite read bench: 50 ops (HTTP + SQL なので遅い)
                    let (rq_qps, rq_ms) = if let Some(ports) = &rqlite_ports {
                        let rq_iters = 50u64;
                        let host = format!("127.0.0.1:{}", ports[1]); // follower を読む
                        let t0 = Instant::now();
                        let mut ok = 0u64;
                        for _ in 0..rq_iters {
                            if rqlite_http_query(&host, "SELECT value FROM entities WHERE eid = 1").is_ok() {
                                ok += 1;
                            }
                        }
                        let el = t0.elapsed();
                        if ok > 0 {
                            let qps = (ok as f64 / el.as_secs_f64()) as u64;
                            let per_ms = el.as_secs_f64() * 1000.0 / ok as f64;
                            (qps, per_ms)
                        } else {
                            (0, 0.0)
                        }
                    } else {
                        (0, 0.0)
                    };

                    let mut s = state_for_bench.write().unwrap();
                    s.enchudb_read_qps = enchu_qps;
                    s.enchudb_read_ns_per_op = enchu_ns;
                    s.rqlite_read_qps = rq_qps;
                    s.rqlite_read_ms_per_op = rq_ms;
                }
            });
        }

        // publish loop — 50ms に 1 record
        let pub_t = HttpTransport::new(url.clone());
        let himo_id = origin_eng.himo_id("val").unwrap() as u16;
        let mut counter = 0u32;
        let publish_tick = Duration::from_millis(50);
        let mut last_pub = Instant::now();
        let mut pending_writes: Vec<(u32, Instant)> = Vec::new();

        // rqlite INSERT 用のチャネル分離 (main loop を block しないため)
        let (rq_tx, rq_rx) = std::sync::mpsc::channel::<u32>();
        if let Some(_rc) = &rqlite_cluster {
            let ports = rqlite_cluster.as_ref().unwrap().http_ports.clone();
            let sh_rq = shutdown.clone();
            std::thread::spawn(move || {
                let host = format!("127.0.0.1:{}", ports[0]); // leader
                while !sh_rq.load(Ordering::Acquire) {
                    match rq_rx.recv_timeout(Duration::from_millis(200)) {
                        Ok(local) => {
                            let sql = format!("INSERT INTO entities (eid, value) VALUES ({}, {})", local, local);
                            let body = format!("[\"{}\"]", sql.replace('"', "\\\""));
                            let _ = rqlite_http_post(&host, "/db/execute", &body);
                        }
                        Err(_) => {}
                    }
                }
            });
        }

        // state update ループ
        let mut last_count = 0usize;
        let mut last_rate_sample = Instant::now();
        let mut last_rqlite_poll = Instant::now() - Duration::from_secs(5);
        let rqlite_poll_interval = Duration::from_millis(250);
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

                // 真の end-to-end latency を測るため、publish "開始前" の時刻を記録
                let t_start = Instant::now();
                pending_writes.push((local, t_start));

                pub_t.publish(1, vec![rec]);

                // rqlite INSERT はチャネル経由で別スレッドに fire-and-forget
                // (main loop を blocking させない → enchudb の真の replication 時間を測れる)
                if rqlite_cluster.is_some() {
                    let _ = rq_tx.send(local);
                }

                counter += 1;
                last_pub = Instant::now();

                // origin WRITE event
                {
                    let mut s = state.write().unwrap();
                    s.latest_write_id = local;
                    s.peers[0].latest_seen = local;
                    s.peers[0].latest_seen_at = Some(Instant::now());
                    if s.rqlite_enabled {
                        // rqlite leader も write と同時に見えたとみなす (同期 INSERT 済み)
                        s.rqlite_peers[0].latest_seen = local;
                        s.rqlite_peers[0].latest_seen_at = Some(Instant::now());
                    }
                    push_event(&mut s.events, EventLine {
                        ts_ms: now_millis(),
                        kind: EventKind::Publish,
                        peer_label: "origin".to_string(),
                        write_id: local,
                        delta_us: None,
                    });
                }
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

                s.peers[0].records = counter as usize;

                // replicas: まず情報収集して pid 更新、events 更新は後でまとめて
                let mut new_events: Vec<EventLine> = Vec::new();
                for (i, r) in replicas.iter().enumerate() {
                    let pid_prev_seen = s.peers[i + 1].latest_seen;
                    let peer_id_for_log = s.peers[i + 1].peer_id;
                    let count = r.entity_count() as usize;

                    // visible_local を探す
                    let visible_local = {
                        let mut max_local = 0u32;
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

                    let pid = &mut s.peers[i + 1];
                    pid.records = count;

                    if visible_local > pid_prev_seen {
                        for new_id in (pid_prev_seen + 1)..=visible_local {
                            if let Some(delta_us) = pending_writes.iter()
                                .find(|(l, _)| *l == new_id)
                                .map(|(_, t)| t.elapsed().as_micros() as u64)
                            {
                                pid.last_delta_us = delta_us;
                                pid.lag_samples.push_back(delta_us);
                                if pid.lag_samples.len() > 300 { pid.lag_samples.pop_front(); }
                                if i == 0 {
                                    new_events.push(EventLine {
                                        ts_ms: now_millis(),
                                        kind: EventKind::EnchuSeen,
                                        peer_label: format!("peer {}", peer_id_for_log),
                                        write_id: new_id,
                                        delta_us: Some(delta_us),
                                    });
                                }
                            }
                        }
                        pid.latest_seen = visible_local;
                        pid.latest_seen_at = Some(Instant::now());
                    }
                }
                for e in new_events {
                    s.events.push_back(e);
                }
                while s.events.len() > 200 { s.events.pop_front(); }

                // pending 掃除: 全 replica + rqlite node 全てが反映済みなら捨てる
                let min_enchu = replicas.iter()
                    .map(|r| r.entity_count() as u32)
                    .min()
                    .unwrap_or(0);
                let min_rqlite = if s.rqlite_enabled {
                    s.rqlite_peers.iter().map(|p| p.latest_seen).min().unwrap_or(0)
                } else {
                    u32::MAX // rqlite 無効なら制約なし
                };
                let min_all = min_enchu.min(min_rqlite);
                pending_writes.retain(|(l, _)| *l > min_all);

            }

            // rqlite 各 node の count を取る (250ms 間隔、変化があればイベント発火)
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
                    let mut s = state.write().unwrap();
                    let mut rq_events: Vec<EventLine> = Vec::new();
                    for i in 0..3 {
                        if !ok[i] { continue; }
                        let prev_seen = s.rqlite_peers[i].latest_seen;
                        let pid = &mut s.rqlite_peers[i];
                        let new_count = counts[i] as u32;
                        pid.records = counts[i] as usize;

                        if new_count > prev_seen {
                            let newest_id = new_count;
                            let delta_us = pending_writes.iter()
                                .find(|(l, _)| *l == newest_id)
                                .map(|(_, t)| t.elapsed().as_micros() as u64)
                                .unwrap_or(0);
                            pid.last_delta_us = delta_us;
                            pid.lag_samples.push_back(delta_us);
                            if pid.lag_samples.len() > 300 { pid.lag_samples.pop_front(); }
                            pid.latest_seen = newest_id;
                            pid.latest_seen_at = Some(Instant::now());

                            if i > 0 {
                                rq_events.push(EventLine {
                                    ts_ms: now_millis(),
                                    kind: EventKind::RqliteSeen,
                                    peer_label: format!("rqlite {}", i + 1),
                                    write_id: newest_id,
                                    delta_us: Some(delta_us),
                                });
                            }
                        }
                    }
                    for e in rq_events { s.events.push_back(e); }
                    while s.events.len() > 200 { s.events.pop_front(); }
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

fn rqlite_http_post(host_port: &str, path: &str, body: &str) -> std::io::Result<()> {
    use std::io::{Read as _, Write as _};
    use std::net::TcpStream;
    let addr = host_port.parse().map_err(|_|
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "addr"))?;
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
    s.set_read_timeout(Some(Duration::from_secs(5)))?;
    write!(s,
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        path, host_port, body.len(), body)?;
    s.flush()?;
    let mut buf = Vec::new();
    s.read_to_end(&mut buf)?;
    Ok(())
}

/// rqlite node に SELECT POST する超簡易クライアント (QPS bench 用)。
fn rqlite_http_query(host_port: &str, sql: &str) -> std::io::Result<()> {
    use std::io::{Read as _, Write as _};
    use std::net::TcpStream;
    let addr = host_port.parse().map_err(|_|
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "addr"))?;
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
    s.set_read_timeout(Some(Duration::from_secs(2)))?;
    let body = format!("[\"{}\"]", sql.replace('"', "\\\""));
    write!(s,
        "POST /db/query HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        host_port, body.len(), body)?;
    s.flush()?;
    let mut buf = Vec::new();
    s.read_to_end(&mut buf)?;
    Ok(())
}

fn push_event(events: &mut VecDeque<EventLine>, e: EventLine) {
    events.push_back(e);
    while events.len() > 200 {
        events.pop_front();
    }
}

/// 数値を読みやすい単位で: 1_234_567 → "1.2M", 12_345 → "12.3k"
fn format_qps(n: u64) -> String {
    if n == 0 { return "—".to_string(); }
    if n >= 1_000_000_000 { return format!("{:.1} G/s", n as f64 / 1_000_000_000.0); }
    if n >= 1_000_000 { return format!("{:.1} M/s", n as f64 / 1_000_000.0); }
    if n >= 1_000 { return format!("{:.1} k/s", n as f64 / 1_000.0); }
    format!("{}/s", n)
}

/// μs 単位の数値を適応的に表示: "850 ns" / "42 μs" / "3.2 ms"
fn fmt_duration_us(us: u64) -> String {
    if us == 0 { return "—".to_string(); }
    if us < 1 { return format!("{} ns", us * 1000); }  // (実質無いパス)
    if us < 1_000 { return format!("{} μs", us); }
    if us < 10_000 { return format!("{:.1} ms", us as f64 / 1000.0); }
    format!("{} ms", us / 1000)
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
