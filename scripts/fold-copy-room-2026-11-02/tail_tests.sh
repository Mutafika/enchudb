#!/bin/sh
# 閉じていない末尾の試験だけを回し、 出力を 1 run ごとに残す (mutate.py は出力を捨てるので)。 $FC_LOG に置く
: "${FC_LOG:?}"
mkdir -p "$FC_LOG"
cargo test --no-fail-fast -p enchudb-engine --lib -- fold_copy_tests::an_open_tail > "$FC_LOG/$(date +%s)-$$.log" 2>&1
