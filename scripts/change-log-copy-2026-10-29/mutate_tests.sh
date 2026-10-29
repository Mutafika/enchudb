#!/bin/sh
# #667 の変異の試験: 記録の単体 + 畳む時の写しの単体 + oplog の switch hook + changefeed の結合試験
export CARGO_TARGET_DIR=/Users/kubo/Desktop/mutafika/enchudb-fix667/target
cd /Users/kubo/Desktop/mutafika/enchudb-fix667 || exit 2
cargo test -q -p enchudb-engine --lib -- change_log fold_copy || exit 1
cargo test -q -p enchudb-oplog --lib -- switch || exit 1
cargo test -q -p enchudb-engine --test changefeed_log || exit 1
cargo test -q -p enchudb --test change_listener || exit 1
