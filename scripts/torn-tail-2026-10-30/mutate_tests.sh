#!/bin/sh
# #669 の変異の試験: oplog の単体 + 電源断の模擬 (区画を移った直後の像、 #665 の古い区画の header)
export CARGO_TARGET_DIR=/Users/kubo/Desktop/mutafika/enchudb-fix669/target
cd /Users/kubo/Desktop/mutafika/enchudb-fix669 || exit 2
cargo test -q -p enchudb-oplog --lib || exit 1
cargo test -q --release -p enchudb-engine --features crashsim --test power_loss -- writes_after_a_torn switching_wal_halves || exit 1
