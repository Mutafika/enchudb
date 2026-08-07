//! `growable_map` の非 unix 版 stub。
//!
//! 本体 (`growable_map.rs`) は匿名 `PROT_NONE` mmap で address space を予約し
//! `MAP_FIXED` で file を貼り直す設計で、 Windows の `MapViewOfFileEx` は
//! 空きアドレスにしかマップできないため同じ手が使えない。
//!
//! ただし growable backing が要るのは **DB の新規作成時だけ**である:
//! `Engine::open` は growable で作った DB でも `validate_file_size` で
//! sparse 拡張したうえで常に素の `MmapMut` で開き直す (`Backing::Growable` を
//! 構築するのは `create_growable*` 経路の 1 箇所のみ)。 よって非 unix では
//! 「構築できない型」を置き、 eager な `create_*` 系を使えば DB は普通に
//! 読み書きできる。 失うのは「空 DB が見かけ上も小さく始まる」性質だけ。
//!
//! 将来ちゃんと対応するなら Win10 1803+ の placeholder API
//! (`VirtualAlloc2` + `MapViewOfFile3`) で本体を書き直す。

use std::fs::File;
use std::io;

/// 非 unix では構築できない (uninhabited)。 `new` が必ず `Unsupported` を返す
/// ので、 `&self` を取る他のメソッドは到達しない。
pub enum GrowableMap {}

impl GrowableMap {
    pub fn new(_file: File, _reserve: usize, _initial: usize) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "growable backing is unix-only; use Engine::create_with_capacity \
             (eager mmap) on this platform",
        ))
    }

    pub fn grow_to(&self, _new_size: usize) -> io::Result<()> {
        match *self {}
    }

    pub fn grow_amortized(&self, _needed: usize) -> io::Result<()> {
        match *self {}
    }

    pub fn base(&self) -> *mut u8 {
        match *self {}
    }

    pub fn committed(&self) -> usize {
        match *self {}
    }

    pub fn reserved(&self) -> usize {
        match *self {}
    }

    pub fn flush(&self, _offset: usize, _len: usize) -> io::Result<()> {
        match *self {}
    }

    pub fn mark_dirty(&self, _offset: usize, _len: usize) {
        match *self {}
    }

    pub fn flush_aligned(&self, _offset: usize, _len: usize) -> io::Result<()> {
        match *self {}
    }

    pub fn flush_dirty(&self) -> io::Result<()> {
        match *self {}
    }
}
