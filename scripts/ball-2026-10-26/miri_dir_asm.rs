use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;

pub struct Chunked {
    pub chunks: Box<[OnceLock<Box<[AtomicU64]>>]>,
}

#[unsafe(no_mangle)]
#[inline(never)]
pub fn get_slice(x: &Chunked, i: u32) -> Option<&AtomicU64> {
    let c = x.chunks.get((i >> 16) as usize)?.get()?;
    Some(&c[i as usize & 0xffff])
}

#[unsafe(no_mangle)]
#[inline(never)]
pub fn get_raw(x: &Chunked, i: u32) -> Option<&AtomicU64> {
    let k = (i >> 16) as usize;
    if k >= (&raw const *x.chunks).len() {
        return None;
    }
    let c = x.chunks[k].get()?;
    Some(&c[i as usize & 0xffff])
}
