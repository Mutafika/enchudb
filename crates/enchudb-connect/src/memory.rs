//! 同じプロセス内の topic (partition 1 つ)。 テストと、 DB から DB へのつなぎに使う。
//! clone は同じ topic を指す。

use crate::{Message, OutMessage, Position, Sink, Source};
use std::io;
use std::sync::{Arc, Mutex};

type Log = Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>;

#[derive(Clone)]
pub struct Topic {
    name: String,
    log: Arc<Mutex<Log>>,
}

impl Topic {
    pub fn new(name: &str) -> Self {
        Topic { name: name.into(), log: Arc::default() }
    }

    fn log(&self) -> std::sync::MutexGuard<'_, Log> {
        self.log.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// 末尾に 1 件足す (`payload` None = tombstone)。 足した offset を返す。
    pub fn push(&self, key: Option<&[u8]>, payload: &[u8]) -> u64 {
        let mut l = self.log();
        l.push((key.map(<[u8]>::to_vec), Some(payload.to_vec())));
        l.len() as u64 - 1
    }

    /// 今までの全件 (key, payload)。 tombstone は payload が空。
    pub fn messages(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.log().iter().map(|(k, p)| (k.clone().unwrap_or_default(), p.clone().unwrap_or_default())).collect()
    }

    /// 先頭から読む source。
    pub fn source(&self) -> TopicSource {
        TopicSource { topic: self.clone(), next: 0 }
    }

    /// 末尾に足す sink。
    pub fn sink(&self) -> TopicSink {
        TopicSink { topic: self.clone() }
    }
}

pub struct TopicSource {
    topic: Topic,
    next: u64,
}

impl Source for TopicSource {
    fn fetch(&mut self, max: usize) -> io::Result<Vec<Message>> {
        let l = self.topic.log();
        let out: Vec<Message> = l
            .iter()
            .enumerate()
            .skip(self.next as usize)
            .take(max)
            .map(|(i, (k, p))| Message {
                position: Position { stream: self.topic.name.clone(), partition: 0, offset: i as u64 },
                key: k.clone(),
                payload: p.clone(),
            })
            .collect();
        self.next += out.len() as u64;
        Ok(out)
    }

    fn seek(&mut self, stream: &str, partition: u32, offset: u64) -> io::Result<()> {
        if stream == self.topic.name && partition == 0 {
            self.next = offset;
        }
        Ok(())
    }
}

pub struct TopicSink {
    topic: Topic,
}

impl Sink for TopicSink {
    fn send(&mut self, msgs: &[OutMessage]) -> io::Result<()> {
        let mut l = self.topic.log();
        l.extend(msgs.iter().map(|m| (Some(m.key.clone()), Some(m.payload.clone()))));
        Ok(())
    }
}
