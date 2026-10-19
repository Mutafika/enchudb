//! peer / relay から届く batch の bytes (`transport::decode_batch`)。 壊れた / 悪意のある入力で panic も abort もしない
//! (#362: 件数をそのまま確保して abort した)。 読めた batch は encode → decode → encode で同じ bytes に戻る。
#![no_main]
use enchudb_engine::transport::{decode_batch, encode_batch};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(recs) = decode_batch(data) else { return };
    let once = encode_batch(&recs);
    let back = decode_batch(&once).expect("encode した batch が読めない");
    assert_eq!(back.len(), recs.len());
    assert_eq!(encode_batch(&back), once, "encode → decode → encode で bytes が変わった");
});
