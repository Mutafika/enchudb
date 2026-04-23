//! HTTP transport — `std::net` のみで実装する最小 HTTP/1.1 relay。
//!
//! 目的:
//! - origin と edge replica を **別プロセス / 別マシン**で動かして sync を実現
//! - 2-node localhost demo を即作れる土台
//! - 外部 crate ゼロ (hyper も axum も使わない)、依存は std のみ
//!
//! # プロトコル
//!
//! ```text
//! GET  /pull?from=<peer>&wall=<u64>&logical=<u32>&peer=<u32>
//!   → 200 OK, body = encode_batch(records)
//!
//! POST /publish?peer=<peer>
//!   body = encode_batch(records)
//!   → 200 OK, body = empty
//! ```
//!
//! # 使い方
//!
//! ```
//! use std::sync::Arc;
//! use enchudb_transport::http::{HttpRelay, HttpTransport};
//! use enchudb::transport::Transport;
//! use enchudb::Hlc;
//!
//! // サーバー側 (origin) — ephemeral port で listen
//! let relay = HttpRelay::start("127.0.0.1:0").unwrap();
//! let url = format!("http://{}", relay.addr());
//!
//! // クライアント側 (edge)
//! let t: Arc<dyn Transport> = Arc::new(HttpTransport::new(url));
//! // 未知 peer から pull → 0 件
//! let recs = t.pull(1, Hlc::ZERO);
//! assert!(recs.is_empty());
//! ```
//!
//! # 注意
//! - HTTP/1.1 手書き、keep-alive なし、1 接続 1 リクエスト
//! - TLS なし、認証なし、信頼ネットワーク前提 (MVP)
//! - 本番用途は hyper / rustls / axum 等に載せ替え推奨

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use enchudb::transport::{decode_batch, encode_batch, Transport, WireRecord};
use enchudb::{Hlc, PeerId};

// ─────────────────────────────────────────────────────────────
// Server: HttpRelay
// ─────────────────────────────────────────────────────────────

/// peer → 昇順 HLC のレコード log。InMemoryTransport と同じ中身。
type Storage = Arc<Mutex<HashMap<PeerId, Vec<WireRecord>>>>;

/// Bootstrap の送信元情報。HttpRelay::start_with_bootstrap で設定する。
#[derive(Clone)]
struct BootstrapSource {
    db_path: String,
}

/// HttpRelay 内部で handle_connection に渡す state。
struct ServerState {
    storage: Storage,
    bootstrap: Option<BootstrapSource>,
}

/// HTTP relay サーバー。バックグラウンドスレッドで listen する。
/// drop すると shutdown 要求を出してスレッドを join する。
pub struct HttpRelay {
    storage: Storage,
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    addr: SocketAddr,
}

impl HttpRelay {
    /// 指定 addr (例: "127.0.0.1:8080" or "127.0.0.1:0" で ephemeral port) で listen 開始。
    /// 戻ってきた時点で accept ループは動いている。
    pub fn start(bind_addr: &str) -> io::Result<Self> {
        Self::start_inner(bind_addr, None)
    }

    /// bootstrap (DB ファイル配信) を有効化して listen 開始。
    /// 新 replica が `GET /bootstrap` で DB 丸ごと取得 → fresh 起動できる。
    pub fn start_with_bootstrap(bind_addr: &str, db_path: &str) -> io::Result<Self> {
        Self::start_inner(
            bind_addr,
            Some(BootstrapSource { db_path: db_path.to_string() }),
        )
    }

    fn start_inner(bind_addr: &str, bootstrap: Option<BootstrapSource>) -> io::Result<Self> {
        let listener = TcpListener::bind(bind_addr)?;
        let addr = listener.local_addr()?;
        // accept が shutdown をチェックできるよう、短い timeout を設定
        listener.set_nonblocking(true)?;

        let storage: Storage = Arc::new(Mutex::new(HashMap::new()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let state = Arc::new(ServerState {
            storage: storage.clone(),
            bootstrap,
        });

        let shutdown_bg = shutdown.clone();
        let handle = std::thread::spawn(move || {
            loop {
                if shutdown_bg.load(Ordering::Acquire) { break; }
                match listener.accept() {
                    Ok((stream, _)) => {
                        let st = state.clone();
                        std::thread::spawn(move || {
                            let _ = handle_connection(stream, st);
                        });
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => {
                        // 致命でないエラーは無視、loop 続ける
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
            }
        });

        Ok(Self { storage, shutdown, handle: Some(handle), addr })
    }

    /// 実際に listen してる SocketAddr。ephemeral port 利用時に引く。
    pub fn addr(&self) -> SocketAddr { self.addr }

    /// 手動 shutdown。drop でも同じことが走る。
    pub fn stop(mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }

    /// テスト用: 保持レコード総数。
    pub fn record_count(&self) -> usize {
        self.storage.lock().unwrap().values().map(|v| v.len()).sum()
    }

    /// この relay がレコードを保持している peer_id の一覧。
    /// daemon が「自 relay に溜まった他 peer のレコード」を drain するのに使う。
    pub fn known_peer_ids(&self) -> Vec<PeerId> {
        self.storage.lock().unwrap().keys().copied().collect()
    }
}

impl Drop for HttpRelay {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn handle_connection(mut stream: TcpStream, state: Arc<ServerState>) -> io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    let storage = state.storage.clone();

    // Request line + headers を \r\n\r\n まで読む
    let mut buf = Vec::with_capacity(1024);
    let mut tmp = [0u8; 512];
    let headers_end;
    loop {
        let n = stream.read(&mut tmp)?;
        if n == 0 { return Ok(()); } // client closed
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = find_header_end(&buf) {
            headers_end = pos;
            break;
        }
        if buf.len() > 16 * 1024 {
            return send_response(&mut stream, 413, b"headers too large");
        }
    }

    let header_str = std::str::from_utf8(&buf[..headers_end])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-utf8 headers"))?;
    let mut lines = header_str.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split(' ');
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");

    // Content-Length を header から取得
    let mut content_length: usize = 0;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse().unwrap_or(0);
            }
        }
    }

    // body を最後まで読む (既に buf に一部入ってる場合がある)
    let body_start = headers_end + 4;
    let mut body = if buf.len() >= body_start {
        buf[body_start..].to_vec()
    } else {
        Vec::new()
    };
    while body.len() < content_length {
        let remaining = content_length - body.len();
        let mut chunk = vec![0u8; remaining.min(8192)];
        let n = stream.read(&mut chunk)?;
        if n == 0 { break; }
        chunk.truncate(n);
        body.extend_from_slice(&chunk);
    }

    // path を splits: "/pull?from=1&wall=..."
    let (route, query) = match path.split_once('?') {
        Some((r, q)) => (r, q),
        None => (path, ""),
    };

    match (method, route) {
        ("GET", "/pull") => {
            let params = parse_query(query);
            let from: PeerId = params.get("from").and_then(|s| s.parse().ok()).unwrap_or(0);
            let wall: u64 = params.get("wall").and_then(|s| s.parse().ok()).unwrap_or(0);
            let logical: u32 = params.get("logical").and_then(|s| s.parse().ok()).unwrap_or(0);
            let peer: u32 = params.get("peer").and_then(|s| s.parse().ok()).unwrap_or(0);
            let since = Hlc { wall, logical, peer };
            let guard = storage.lock().unwrap();
            let recs: Vec<WireRecord> = guard.get(&from)
                .map(|log| log.iter().filter(|r| r.hlc > since).cloned().collect())
                .unwrap_or_default();
            drop(guard);
            let body_out = encode_batch(&recs);
            send_response_with_body(&mut stream, 200, &body_out)
        }
        ("POST", "/publish") => {
            let params = parse_query(query);
            let peer: PeerId = params.get("peer").and_then(|s| s.parse().ok()).unwrap_or(0);
            match decode_batch(&body) {
                Ok(mut records) => {
                    if records.is_empty() {
                        return send_response(&mut stream, 200, b"");
                    }
                    records.sort_by_key(|r| r.hlc);
                    let mut guard = storage.lock().unwrap();
                    let log = guard.entry(peer).or_insert_with(Vec::new);
                    log.extend(records);
                    log.sort_by_key(|r| r.hlc);
                    send_response(&mut stream, 200, b"")
                }
                Err(e) => {
                    send_response(&mut stream, 400, format!("decode failed: {}", e).as_bytes())
                }
            }
        }
        ("GET", "/bootstrap") => {
            let src = match &state.bootstrap {
                Some(s) => s,
                None => return send_response(&mut stream, 404, b"bootstrap not enabled"),
            };
            // snapshot 時点の max HLC を記録 (replica はここから sync 再開)
            let snapshot_hlc = {
                let guard = storage.lock().unwrap();
                guard.values()
                    .flat_map(|log| log.iter())
                    .map(|r| r.hlc)
                    .max()
                    .unwrap_or(Hlc::ZERO)
            };
            // DB ファイルを open して sparse 形式で stream 送信
            // enchudb のファイルは mmap 前提で巨大 sparse ファイル (論理 GB、実質 MB)
            // 生バイト送ると何 GB も流れるため、ゼロラン圧縮必須
            let mut file = match std::fs::File::open(&src.db_path) {
                Ok(f) => f,
                Err(_) => return send_response(&mut stream, 500, b"db file not readable"),
            };
            let metadata = match file.metadata() {
                Ok(m) => m,
                Err(_) => return send_response(&mut stream, 500, b"metadata failed"),
            };
            let size = metadata.len();

            // Content-Length 不明 (圧縮後サイズ) なので Connection: close + EOF で終端
            let header = format!(
                "HTTP/1.1 200 OK\r\nConnection: close\r\n\
                 X-Enchu-Bootstrap-Format: sparse-v1\r\n\
                 X-Enchu-Bootstrap-Size: {}\r\n\
                 X-Enchu-Hlc-Wall: {}\r\nX-Enchu-Hlc-Logical: {}\r\nX-Enchu-Hlc-Peer: {}\r\n\r\n",
                size, snapshot_hlc.wall, snapshot_hlc.logical, snapshot_hlc.peer
            );
            stream.write_all(header.as_bytes())?;
            stream_sparse_file(&mut file, &mut stream, size)?;
            stream.flush()?;
            // FIN を送って send 完了を通知。close (drop) だけだとカーネルバッファに残ったデータが
            // 未送信のまま破棄される可能性あるので明示的に shutdown。
            let _ = stream.shutdown(Shutdown::Write);
            Ok(())
        }
        _ => send_response(&mut stream, 404, b"not found"),
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// sparse file 形式でストリーム送信する。
///
/// 形式: ループで次のいずれか
/// ```text
/// tag=0 (ZERO_RUN): [1B: 0] [8B LE: ゼロ連続バイト数]
/// tag=1 (DATA):     [1B: 1] [4B LE: N] [NB: 実データ]
/// ```
/// 64KB チャンクで読み、全ゼロなら ZERO_RUN、そうでなければ DATA。
/// 合計 `size` バイト送ったら終了。
fn stream_sparse_file<R: Read, W: Write>(file: &mut R, stream: &mut W, size: u64) -> io::Result<()> {
    const CHUNK: usize = 64 * 1024;
    let mut buf = vec![0u8; CHUNK];
    let mut sent: u64 = 0;
    while sent < size {
        let want = ((size - sent) as usize).min(CHUNK);
        let mut filled = 0;
        while filled < want {
            let n = file.read(&mut buf[filled..want])?;
            if n == 0 { break; }
            filled += n;
        }
        if filled == 0 { break; }
        let is_zero = buf[..filled].iter().all(|&b| b == 0);
        if is_zero {
            stream.write_all(&[0u8])?;
            stream.write_all(&(filled as u64).to_le_bytes())?;
        } else {
            stream.write_all(&[1u8])?;
            stream.write_all(&(filled as u32).to_le_bytes())?;
            stream.write_all(&buf[..filled])?;
        }
        sent += filled as u64;
    }
    Ok(())
}

/// sparse stream を decode して file に書き戻す。
/// ZERO_RUN は seek でスキップ → sparse hole が再現される。
fn decode_sparse_stream<R: Read>(
    reader: &mut R,
    file: &mut std::fs::File,
    total_size: u64,
) -> io::Result<()> {
    use std::io::{Seek, SeekFrom};
    let mut written: u64 = 0;
    let mut tag = [0u8; 1];
    let mut len_buf8 = [0u8; 8];
    let mut len_buf4 = [0u8; 4];
    while written < total_size {
        match reader.read_exact(&mut tag) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        match tag[0] {
            0 => {
                reader.read_exact(&mut len_buf8).map_err(|e| io::Error::new(
                    e.kind(),
                    format!("sparse decode: reading ZERO_RUN len at offset {}: {}", written, e),
                ))?;
                let count = u64::from_le_bytes(len_buf8);
                // seek で穴を空ける (sparse 維持)
                file.seek(SeekFrom::Current(count as i64))?;
                written += count;
            }
            1 => {
                reader.read_exact(&mut len_buf4).map_err(|e| io::Error::new(
                    e.kind(),
                    format!("sparse decode: reading DATA len at offset {}: {}", written, e),
                ))?;
                let len = u32::from_le_bytes(len_buf4) as usize;
                let mut data = vec![0u8; len];
                reader.read_exact(&mut data).map_err(|e| io::Error::new(
                    e.kind(),
                    format!("sparse decode: reading DATA {} bytes at offset {}: {}", len, written, e),
                ))?;
                file.write_all(&data)?;
                written += len as u64;
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown sparse tag: {} at offset {}", other, written),
                ));
            }
        }
    }
    // trailing sparse hole を確定 (seek で進めただけなら set_len で length 確定)
    file.set_len(total_size)?;
    Ok(())
}

fn parse_query(q: &str) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for kv in q.split('&') {
        if let Some((k, v)) = kv.split_once('=') {
            m.insert(k.to_string(), v.to_string());
        }
    }
    m
}

fn send_response(stream: &mut TcpStream, status: u16, body: &[u8]) -> io::Result<()> {
    send_response_with_body(stream, status, body)
}

fn send_response_with_body(stream: &mut TcpStream, status: u16, body: &[u8]) -> io::Result<()> {
    let status_text = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        413 => "Payload Too Large",
        500 => "Internal Server Error",
        _ => "Unknown",
    };
    let header = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        status, status_text, body.len()
    );
    stream.write_all(header.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()?;
    let _ = stream.shutdown(Shutdown::Write);
    Ok(())
}

// ─────────────────────────────────────────────────────────────
// Client: HttpTransport
// ─────────────────────────────────────────────────────────────

/// HTTP client として Transport を実装。同期ブロッキング。
pub struct HttpTransport {
    base_url: String,
}

impl HttpTransport {
    /// `base_url` = "http://host:port" (path 無し、末尾 / 不要)
    pub fn new(base_url: String) -> Self {
        let base = base_url.trim_end_matches('/').to_string();
        Self { base_url: base }
    }

    fn host_port(&self) -> io::Result<(String, u16)> {
        // http:// プレフィックス剥がす、後ろに path 付いてても取らない
        let s = self.base_url.strip_prefix("http://").unwrap_or(&self.base_url);
        let hostport = s.split('/').next().unwrap_or(s);
        let (host, port) = match hostport.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), p.parse().unwrap_or(80)),
            None => (hostport.to_string(), 80),
        };
        Ok((host, port))
    }

    fn connect(&self) -> io::Result<TcpStream> {
        let (host, port) = self.host_port()?;
        let addr = format!("{}:{}", host, port);
        let sock_addr = addr.to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "no address"))?;
        let stream = TcpStream::connect_timeout(&sock_addr, Duration::from_secs(10))?;
        stream.set_read_timeout(Some(Duration::from_secs(30)))?;
        stream.set_write_timeout(Some(Duration::from_secs(30)))?;
        Ok(stream)
    }

    /// origin から DB ファイル全体を受信して `local_path` に保存。
    /// origin の snapshot 時 HLC を返す。以降の sync はこの HLC 以降だけ pull すれば良い。
    ///
    /// 使い方:
    /// ```
    /// use enchudb_transport::http::{HttpRelay, HttpTransport};
    ///
    /// // bootstrap が無効な relay は 404 で Err を返す
    /// let relay = HttpRelay::start("127.0.0.1:0").unwrap();
    /// let url = format!("http://{}", relay.addr());
    /// let client = HttpTransport::new(url);
    /// let target = format!("/tmp/enchudb-doc-bootstrap-{}.db", std::process::id());
    /// let result = client.bootstrap_to(&target);
    /// assert!(result.is_err(), "bootstrap should 404 without src");
    /// # let _ = std::fs::remove_file(&target);
    /// ```
    pub fn bootstrap_to(&self, local_path: &str) -> io::Result<Hlc> {
        let mut stream = self.connect()?;
        let (host, _) = self.host_port()?;
        let req_head = format!(
            "GET /bootstrap HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            host
        );
        stream.write_all(req_head.as_bytes())?;
        stream.flush()?;

        // header を \r\n\r\n まで読む
        let mut headers = Vec::new();
        let mut tmp = [0u8; 512];
        let extra_body: Vec<u8>;
        loop {
            let n = stream.read(&mut tmp)?;
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "no response"));
            }
            headers.extend_from_slice(&tmp[..n]);
            if let Some(pos) = find_header_end(&headers) {
                extra_body = headers[pos + 4..].to_vec();
                headers.truncate(pos);
                break;
            }
            if headers.len() > 64 * 1024 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "headers too large"));
            }
        }

        let header_str = std::str::from_utf8(&headers)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-utf8 headers"))?;
        let mut lines = header_str.split("\r\n");
        let first = lines.next().unwrap_or("");
        let status: u16 = first.split(' ').nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
        if status != 200 {
            return Err(io::Error::new(io::ErrorKind::Other, format!("status {}", status)));
        }

        // header parse: Format / Size / HLC
        let mut format = String::new();
        let mut total_size: Option<u64> = None;
        let mut wall = 0u64;
        let mut logical = 0u32;
        let mut peer = 0u32;
        for line in lines {
            if let Some((k, v)) = line.split_once(':') {
                let k = k.trim();
                let v = v.trim();
                if k.eq_ignore_ascii_case("x-enchu-bootstrap-format") {
                    format = v.to_string();
                } else if k.eq_ignore_ascii_case("x-enchu-bootstrap-size") {
                    total_size = v.parse().ok();
                } else if k.eq_ignore_ascii_case("x-enchu-hlc-wall") {
                    wall = v.parse().unwrap_or(0);
                } else if k.eq_ignore_ascii_case("x-enchu-hlc-logical") {
                    logical = v.parse().unwrap_or(0);
                } else if k.eq_ignore_ascii_case("x-enchu-hlc-peer") {
                    peer = v.parse().unwrap_or(0);
                }
            }
        }

        if format != "sparse-v1" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported bootstrap format: '{}'", format),
            ));
        }
        let size = total_size
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no X-Enchu-Bootstrap-Size"))?;

        let mut file = std::fs::File::create(local_path)?;
        // extra_body (レスポンス body 冒頭ですでに読んでしまった分) と後続 stream を連結して decoder に渡す
        let prepended = std::io::Cursor::new(extra_body).chain(stream);
        let mut reader = prepended;
        decode_sparse_stream(&mut reader, &mut file, size)?;
        file.sync_all()?;

        Ok(Hlc { wall, logical, peer })
    }

    fn request(&self, method: &str, path: &str, body: &[u8]) -> io::Result<(u16, Vec<u8>)> {
        let mut stream = self.connect()?;
        let (host, _) = self.host_port()?;
        let req_head = format!(
            "{} {} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            method, path, host, body.len()
        );
        stream.write_all(req_head.as_bytes())?;
        if !body.is_empty() {
            stream.write_all(body)?;
        }
        stream.flush()?;

        // response を全部読み込む
        let mut resp = Vec::with_capacity(1024);
        let mut tmp = [0u8; 4096];
        loop {
            match stream.read(&mut tmp) {
                Ok(0) => break,
                Ok(n) => resp.extend_from_slice(&tmp[..n]),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }

        let headers_end = find_header_end(&resp)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no headers end"))?;
        let header_str = std::str::from_utf8(&resp[..headers_end])
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-utf8 headers"))?;
        let first_line = header_str.split("\r\n").next().unwrap_or("");
        // "HTTP/1.1 200 OK"
        let status: u16 = first_line.split(' ')
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let body = resp[headers_end + 4..].to_vec();
        Ok((status, body))
    }
}

impl Transport for HttpTransport {
    fn pull(&self, from: PeerId, since: Hlc) -> Vec<WireRecord> {
        let path = format!(
            "/pull?from={}&wall={}&logical={}&peer={}",
            from, since.wall, since.logical, since.peer
        );
        match self.request("GET", &path, &[]) {
            Ok((200, body)) => decode_batch(&body).unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    fn publish(&self, peer: PeerId, records: Vec<WireRecord>) {
        if records.is_empty() { return; }
        let body = encode_batch(&records);
        let path = format!("/publish?peer={}", peer);
        let _ = self.request("POST", &path, &body);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use enchudb::wal::DecodedOp;

    fn rec(hlc_wall: u64, peer: PeerId, eid: u64, value: u32) -> WireRecord {
        WireRecord::unsigned(
            Hlc { wall: hlc_wall, logical: 0, peer },
            peer,
            DecodedOp::Tie { eid, himo_id: 0, value },
        )
    }

    #[test]
    fn relay_start_and_stop() {
        let relay = HttpRelay::start("127.0.0.1:0").unwrap();
        let addr = relay.addr();
        assert!(addr.port() != 0);
        drop(relay);
    }

    #[test]
    fn publish_and_pull_roundtrip() {
        let relay = HttpRelay::start("127.0.0.1:0").unwrap();
        let url = format!("http://{}", relay.addr());
        let client = HttpTransport::new(url);

        client.publish(1, vec![
            rec(100, 1, 10, 1000),
            rec(200, 1, 20, 2000),
        ]);

        // 50ms fixed sleep は CI/loaded machine で足りなくなる。
        // 2 件揃うまで最大 2s ポーリング。
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while relay.record_count() < 2 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(relay.record_count(), 2);

        let pulled = client.pull(1, Hlc::ZERO);
        assert_eq!(pulled.len(), 2);
        assert_eq!(pulled[0].hlc.wall, 100);
        assert_eq!(pulled[1].hlc.wall, 200);
    }

    #[test]
    fn pull_with_since_filters() {
        let relay = HttpRelay::start("127.0.0.1:0").unwrap();
        let url = format!("http://{}", relay.addr());
        let client = HttpTransport::new(url);

        client.publish(1, vec![
            rec(100, 1, 1, 10),
            rec(200, 1, 2, 20),
            rec(300, 1, 3, 30),
        ]);
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while relay.record_count() < 3 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }

        let pulled = client.pull(1, Hlc { wall: 150, logical: 0, peer: 1 });
        assert_eq!(pulled.len(), 2);
        assert_eq!(pulled[0].hlc.wall, 200);
        assert_eq!(pulled[1].hlc.wall, 300);
    }

    #[test]
    fn pull_unknown_peer_empty() {
        let relay = HttpRelay::start("127.0.0.1:0").unwrap();
        let url = format!("http://{}", relay.addr());
        let client = HttpTransport::new(url);

        let pulled = client.pull(42, Hlc::ZERO);
        assert!(pulled.is_empty());
    }

    #[test]
    fn bootstrap_returns_404_when_disabled() {
        let relay = HttpRelay::start("127.0.0.1:0").unwrap();
        let url = format!("http://{}", relay.addr());
        let client = HttpTransport::new(url);
        let target = format!("/tmp/enchu_bootstrap_test_{}", std::process::id());
        let result = client.bootstrap_to(&target);
        assert!(result.is_err(), "bootstrap should 404 without src");
        let _ = std::fs::remove_file(&target);
    }

    #[test]
    fn bootstrap_downloads_file_and_returns_hlc() {
        // 元ファイルを作る (小さめ、sparse でない普通のファイル)
        let src = format!("/tmp/enchu_bootstrap_src_{}", std::process::id());
        let dst = format!("/tmp/enchu_bootstrap_dst_{}", std::process::id());
        let _ = std::fs::remove_file(&src);
        let _ = std::fs::remove_file(&dst);

        std::fs::write(&src, b"enchu bootstrap test payload with some bytes\x00\x01\xff").unwrap();

        let relay = HttpRelay::start_with_bootstrap("127.0.0.1:0", &src).unwrap();
        let url = format!("http://{}", relay.addr());
        let pub_t = HttpTransport::new(url.clone());
        pub_t.publish(1, vec![rec(500, 1, 1, 1)]);
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while relay.record_count() < 1 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }

        let client = HttpTransport::new(url);
        let hlc = client.bootstrap_to(&dst).unwrap();
        assert_eq!(hlc.wall, 500);
        assert_eq!(hlc.peer, 1);

        let content = std::fs::read(&dst).unwrap();
        assert_eq!(content, b"enchu bootstrap test payload with some bytes\x00\x01\xff");

        let _ = std::fs::remove_file(&src);
        let _ = std::fs::remove_file(&dst);
    }

    #[test]
    fn sparse_stream_encode_decode_roundtrip() {
        // encoder/decoder の直接テスト、HTTP 抜きで round-trip 確認
        // 64KB 単位で読むので、ゼロ圧縮効くには 64KB 以上のゼロ領域が必要
        use std::io::Cursor;

        // 元データ: 64KB 'A' + 256KB hole + 64KB 'B' = 384KB
        let mut src_data = vec![b'A'; 64 * 1024];
        src_data.extend(vec![0u8; 256 * 1024]);
        src_data.extend(vec![b'B'; 64 * 1024]);
        let total_size = src_data.len() as u64;

        // encode
        let mut src_cur = Cursor::new(src_data.clone());
        let mut encoded = Vec::new();
        stream_sparse_file(&mut src_cur, &mut encoded, total_size).unwrap();

        // 圧縮されてるはず (zero run が縮む)
        assert!(encoded.len() < src_data.len(),
            "encoded {} should be smaller than raw {}", encoded.len(), src_data.len());

        // decode
        let dst_path = format!("/tmp/enchu_sparse_rt_{}", std::process::id());
        let _ = std::fs::remove_file(&dst_path);
        let mut dst_file = std::fs::File::create(&dst_path).unwrap();
        let mut enc_cur = Cursor::new(encoded);
        decode_sparse_stream(&mut enc_cur, &mut dst_file, total_size).unwrap();
        dst_file.sync_all().unwrap();
        drop(dst_file);

        let got = std::fs::read(&dst_path).unwrap();
        assert_eq!(got.len(), src_data.len());
        assert_eq!(got, src_data);

        let _ = std::fs::remove_file(&dst_path);
    }

    #[test]
    fn bootstrap_handles_sparse_file_efficiently() {
        // 256KB 非ゼロデータ + 4MB ゼロ hole + 256KB 非ゼロ の sparse ファイル
        // sparse stream で圧縮されてると早く終わる
        use std::io::{Seek, SeekFrom};
        let src = format!("/tmp/enchu_bootstrap_sparse_src_{}", std::process::id());
        let dst = format!("/tmp/enchu_bootstrap_sparse_dst_{}", std::process::id());
        let _ = std::fs::remove_file(&src);
        let _ = std::fs::remove_file(&dst);

        const HEAD_SIZE: usize = 256 * 1024;
        const HOLE_SIZE: usize = 4 * 1024 * 1024;
        const TAIL_SIZE: usize = 256 * 1024;
        let expected_size = HEAD_SIZE + HOLE_SIZE + TAIL_SIZE;

        let mut f = std::fs::File::create(&src).unwrap();
        f.write_all(&vec![b'A'; HEAD_SIZE]).unwrap();
        f.seek(SeekFrom::Current(HOLE_SIZE as i64)).unwrap();
        f.write_all(&vec![b'B'; TAIL_SIZE]).unwrap();
        f.sync_all().unwrap();
        drop(f);

        let relay = HttpRelay::start_with_bootstrap("127.0.0.1:0", &src).unwrap();
        let url = format!("http://{}", relay.addr());
        let client = HttpTransport::new(url);

        let _ = client.bootstrap_to(&dst).unwrap();

        let meta = std::fs::metadata(&dst).unwrap();
        assert_eq!(meta.len(), expected_size as u64);

        let head = std::fs::read(&dst).unwrap();
        assert!(head[..HEAD_SIZE].iter().all(|&b| b == b'A'));
        let tail_offset = HEAD_SIZE + HOLE_SIZE;
        assert!(head[tail_offset..tail_offset + TAIL_SIZE].iter().all(|&b| b == b'B'));
        assert!(head[HEAD_SIZE..tail_offset].iter().all(|&b| b == 0));

        let _ = std::fs::remove_file(&src);
        let _ = std::fs::remove_file(&dst);
    }
}
