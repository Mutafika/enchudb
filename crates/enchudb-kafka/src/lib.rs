//! [`enchudb_connect`] の Kafka アダプタ。 Kafka 互換 (Redpanda / WarpStream など) にもそのまま繋がる。
//!
//! - [`KafkaSource`]: topic の全 partition を読む [`Source`]。 読んだ位置は enchu 側 ([`Ingest`] が DB に
//!   書く) が持つので、 Kafka の consumer group の offset commit は使わない。 位置を覚えていない
//!   partition は、 残っている最古の record から読む
//! - [`KafkaSink`]: topic に書く [`Sink`]。 partition は key の murmur2 (Java の既定の partitioner と
//!   同じ振り分け = 同じ key は同じ partition に順に入る)
//!
//! 純 Rust ([rskafka](https://docs.rs/rskafka)) で C のライブラリは要らない。 中で tokio の
//! current-thread runtime を持ち、 呼び出しは同期 (block_on)。
//!
//! ```no_run
//! use enchudb_connect::{Ingest, JsonRows, LiveExport};
//! use enchudb_kafka::{KafkaSink, KafkaSource};
//! # fn run(db: &enchudb_schema::Database, ex: &mut LiveExport) -> std::io::Result<()> {
//! let brokers = vec!["localhost:9092".to_string()];
//! let mut src = KafkaSource::connect(brokers.clone(), "orders")?;
//! let mut sink = KafkaSink::connect(brokers, "orders-live")?;
//! let ing = Ingest::new(db, JsonRows).unwrap();
//! ing.resume(&mut src)?;
//! loop {
//!     ing.run_once(&mut src, 10_000)?;
//!     ex.pump(&mut sink)?;
//! }
//! # }
//! ```
//!
//! [`Ingest`]: enchudb_connect::Ingest

use enchudb_connect::{Message, OutMessage, Position, Sink, Source};
use rskafka::client::partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;
use std::collections::BTreeMap;
use std::io;
use tokio::runtime::Runtime;

fn err(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

fn runtime() -> io::Result<Runtime> {
    tokio::runtime::Builder::new_current_thread().enable_all().build()
}

async fn partitions(client: &Client, topic: &str) -> io::Result<Vec<i32>> {
    let topics = client.list_topics().await.map_err(err)?;
    let t = topics.into_iter().find(|t| t.name == topic).ok_or_else(|| err(format!("unknown topic {topic}")))?;
    Ok(t.partitions.into_iter().collect())
}

/// topic がまだ無ければ作る (partition 数 `partitions`、 複製 `replication`)。
pub fn ensure_topic(brokers: Vec<String>, topic: &str, partitions: i32, replication: i16) -> io::Result<()> {
    let rt = runtime()?;
    rt.block_on(async {
        let client = ClientBuilder::new(brokers).build().await.map_err(err)?;
        if client.list_topics().await.map_err(err)?.iter().any(|t| t.name == topic) {
            return Ok(());
        }
        client.controller_client().map_err(err)?.create_topic(topic, partitions, replication, 5_000).await.map_err(err)
    })
}

struct Part {
    client: PartitionClient,
    /// 次に読む offset (None = まだ決めていない → 最古から)。
    next: Option<i64>,
    /// fetch したが返していない record。
    buf: std::collections::VecDeque<Message>,
}

/// topic の全 partition を読む。
pub struct KafkaSource {
    rt: Runtime,
    topic: String,
    parts: BTreeMap<i32, Part>,
    /// 1 回の fetch で partition から取る最大 bytes。
    max_bytes: i32,
    /// record が無い時に broker が待つ時間。
    max_wait_ms: i32,
}

impl KafkaSource {
    /// `brokers` (`"host:port"`) に繋ぎ、 `topic` の全 partition を読む。
    pub fn connect(brokers: Vec<String>, topic: &str) -> io::Result<Self> {
        let rt = runtime()?;
        let parts = rt.block_on(async {
            let client = ClientBuilder::new(brokers).build().await.map_err(err)?;
            let mut parts = BTreeMap::new();
            for p in partitions(&client, topic).await? {
                let c = client.partition_client(topic, p, UnknownTopicHandling::Retry).await.map_err(err)?;
                parts.insert(p, Part { client: c, next: None, buf: Default::default() });
            }
            Ok::<_, io::Error>(parts)
        })?;
        Ok(KafkaSource { rt, topic: topic.into(), parts, max_bytes: 4 << 20, max_wait_ms: 100 })
    }

    /// record が無い時に broker が待つ時間 (既定 100 ms)。
    pub fn max_wait_ms(mut self, ms: i32) -> Self {
        self.max_wait_ms = ms;
        self
    }
}

impl Source for KafkaSource {
    fn fetch(&mut self, max: usize) -> io::Result<Vec<Message>> {
        let (topic, max_bytes, wait) = (self.topic.clone(), self.max_bytes, self.max_wait_ms);
        let mut out = Vec::new();
        // 溜めてある分から。 足りなければ partition ごとに 1 回 fetch
        for p in self.parts.values_mut() {
            while out.len() < max && let Some(m) = p.buf.pop_front() {
                out.push(m);
            }
        }
        if out.len() >= max {
            return Ok(out);
        }
        let n_parts = self.parts.len().max(1) as i32;
        for (&pid, p) in self.parts.iter_mut() {
            let next = match p.next {
                Some(n) => n,
                None => self.rt.block_on(p.client.get_offset(OffsetAt::Earliest)).map_err(err)?,
            };
            // 空の partition で他を待たせないよう、 待つ時間は partition 数で割る
            let (records, _hw) =
                self.rt.block_on(p.client.fetch_records(next, 1..max_bytes, wait / n_parts)).map_err(err)?;
            let mut n = next;
            for r in records {
                // 圧縮された batch は要求より前の offset から返ることがある
                if r.offset < next {
                    continue;
                }
                n = r.offset + 1;
                p.buf.push_back(Message {
                    position: Position { stream: topic.clone(), partition: pid as u32, offset: r.offset as u64 },
                    key: r.record.key,
                    payload: r.record.value,
                });
            }
            p.next = Some(n);
            while out.len() < max && let Some(m) = p.buf.pop_front() {
                out.push(m);
            }
        }
        Ok(out)
    }

    fn seek(&mut self, stream: &str, partition: u32, offset: u64) -> io::Result<()> {
        if stream != self.topic {
            return Ok(());
        }
        if let Some(p) = self.parts.get_mut(&(partition as i32)) {
            p.next = Some(offset as i64);
            p.buf.clear();
        }
        Ok(())
    }
}

/// topic に書く。
pub struct KafkaSink {
    rt: Runtime,
    parts: Vec<PartitionClient>,
    compression: Compression,
}

impl KafkaSink {
    pub fn connect(brokers: Vec<String>, topic: &str) -> io::Result<Self> {
        let rt = runtime()?;
        let parts = rt.block_on(async {
            let client = ClientBuilder::new(brokers).build().await.map_err(err)?;
            let mut parts = Vec::new();
            for p in partitions(&client, topic).await? {
                parts.push(client.partition_client(topic, p, UnknownTopicHandling::Retry).await.map_err(err)?);
            }
            Ok::<_, io::Error>(parts)
        })?;
        if parts.is_empty() {
            return Err(err(format!("topic {topic} has no partition")));
        }
        Ok(KafkaSink { rt, parts, compression: Compression::NoCompression })
    }

    pub fn compression(mut self, c: Compression) -> Self {
        self.compression = c;
        self
    }
}

impl Sink for KafkaSink {
    fn send(&mut self, msgs: &[OutMessage]) -> io::Result<()> {
        let mut by_part: Vec<Vec<Record>> = vec![Vec::new(); self.parts.len()];
        let now = chrono::Utc::now();
        for m in msgs {
            let p = partition_for(&m.key, self.parts.len());
            by_part[p].push(Record {
                key: Some(m.key.clone()),
                value: Some(m.payload.clone()),
                headers: Default::default(),
                timestamp: now,
            });
        }
        for (client, records) in self.parts.iter().zip(by_part) {
            if !records.is_empty() {
                self.rt.block_on(client.produce(records, self.compression)).map_err(err)?;
            }
        }
        Ok(())
    }
}

/// Java の既定の partitioner と同じ振り分け: `toPositive(murmur2(key)) % partitions`。
pub fn partition_for(key: &[u8], partitions: usize) -> usize {
    (murmur2(key) & 0x7fff_ffff) as usize % partitions
}

/// Kafka の murmur2 (seed 0x9747b28c)。
pub fn murmur2(data: &[u8]) -> u32 {
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;
    let mut h: u32 = 0x9747_b28c ^ data.len() as u32;
    let chunks = data.chunks_exact(4);
    let rest = chunks.remainder();
    for c in chunks {
        let mut k = u32::from_le_bytes([c[0], c[1], c[2], c[3]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    match rest.len() {
        3 => {
            h ^= (rest[2] as u32) << 16;
            h ^= (rest[1] as u32) << 8;
            h ^= rest[0] as u32;
            h = h.wrapping_mul(M);
        }
        2 => {
            h ^= (rest[1] as u32) << 8;
            h ^= rest[0] as u32;
            h = h.wrapping_mul(M);
        }
        1 => {
            h ^= rest[0] as u32;
            h = h.wrapping_mul(M);
        }
        _ => {}
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Kafka (Java) の `Utils.murmur2` の既知の値と一致する (Kafka の UtilsTest と同じ入力)。
    #[test]
    fn murmur2_matches_kafka() {
        assert_eq!(murmur2(b"21") as i32, -973932308);
        assert_eq!(murmur2(b"foobar") as i32, -790332482);
        assert_eq!(murmur2(b"a-little-bit-long-string") as i32, -985981536);
        assert_eq!(murmur2(b"a-little-bit-longer-string") as i32, -1486304829);
        assert_eq!(murmur2(b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8") as i32, -58897971);
        assert_eq!(murmur2(b"abc") as i32, 479470107);
    }
}
