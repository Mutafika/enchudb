//! relay の応答が socket 送信バッファより大きい時の途中切断回帰テスト。
//!
//! `HttpRelay` は accept 検出のため listener を non-blocking にするが、 BSD/macOS と
//! Windows では **accept した接続 socket が non-blocking を継承**する。 そのまま
//! `write_all` すると送信バッファが埋まった時点で `WouldBlock` がエラー扱いになり、
//! ハンドラが死んで Content-Length より短い body で接続が閉じる（読み手が遅いほど
//! 確実に再現）。 実機では下流 syncretic の「初回フル pull（数 MB）が毎回失敗し、
//! cursor が永遠に 0 のまま 1 件も同期しない」として発現した。 ws.rs は既に
//! `set_nonblocking(false)` 済みで、 http.rs だけ取り残されていた。

use std::io::{Read as _, Write as _};
use std::sync::Arc;
use std::time::Duration;

use enchudb::transport::{Transport, WireRecord};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::Hlc;
use enchudb_transport::http::{HttpRelay, HttpTransport};

#[test]
fn relay_pull_large_body_survives_slow_reader() {
    let relay = HttpRelay::start("127.0.0.1:0").unwrap();
    let url = format!("http://{}", relay.addr());

    // 8MB 分の Content record を relay に積む（loopback の送信バッファを確実に超す）
    let publisher: Arc<dyn Transport> = Arc::new(HttpTransport::new(url));
    let records: Vec<WireRecord> = (0..8u64)
        .map(|i| {
            WireRecord::unsigned(
                Hlc { wall: 100 + i, logical: 0, peer: 1 },
                1,
                DecodedOp::Content {
                    eid: enchudb_oplog::make_eid(1, (i + 1) as u32),
                    key: "k".to_string(),
                    data: vec![0xAB; 1024 * 1024],
                },
            )
        })
        .collect();
    publisher.publish(1, records);

    // 遅い読み手: request 送信後 500ms 読まずに置く → server 側の write が
    // 送信バッファ満杯に到達する（non-blocking のままなら即 WouldBlock で死ぬ）
    let mut sock = std::net::TcpStream::connect(relay.addr()).unwrap();
    sock.write_all(b"GET /pull?from=1&wall=0&logical=0&peer=0 HTTP/1.1\r\nHost: x\r\n\r\n")
        .unwrap();
    std::thread::sleep(Duration::from_millis(500));
    let mut resp = Vec::new();
    sock.read_to_end(&mut resp).unwrap();

    let head_end = resp
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response headers")
        + 4;
    let head = std::str::from_utf8(&resp[..head_end]).unwrap();
    let mut content_length = None;
    for line in head.lines() {
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse::<usize>().ok();
            }
        }
    }
    let content_length = content_length.expect("Content-Length header");
    assert!(
        content_length > 8 * 1024 * 1024,
        "8MB 積んだのに Content-Length が {content_length}B しかない"
    );
    assert_eq!(
        resp.len() - head_end,
        content_length,
        "body が途中切断された ({} / {} bytes)",
        resp.len() - head_end,
        content_length
    );
}
