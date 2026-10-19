//! `_sync_ops` の行の payload (`decode_sync_ops_payload`、 中継した record の署名 + header + op)。 header の検査の後に
//! op の decoder (`decode_op`、 全部の op の種類) に届く。 panic しない。 author / HLC だけを読む関数と答えが合う。
#![no_main]
use enchudb_oplog::oplog::{decode_sync_ops_payload, sync_ops_payload_author_hlc};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let rec = decode_sync_ops_payload(data);
    let head = sync_ops_payload_author_hlc(data);
    if let Some(r) = rec {
        assert_eq!(head, Some((r.author_peer, r.hlc)), "record は読めたのに author / HLC が合わない");
    }
});
