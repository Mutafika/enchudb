#!/bin/sh
# 16 回目の変異の試験 (出力を run ごとに残す)。 $1 = a (schema の球の試験) | b (engine の記録と球の単体)
cd /Users/kubo/Desktop/mutafika/enchudb-ball || exit 2
S=/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/71308e5d-c18e-4313-bf9c-4a796bf6373b/scratchpad
export CARGO_TARGET_DIR=$S/tgt-ball16
L=$S/mut16$1/$(date +%s).log
case $1 in
  a) cargo test --no-fail-fast -p enchudb-schema --test ball_lazy --test ball_reopen --test ball_history > "$L" 2>&1 ;;
  b) cargo test --no-fail-fast -p enchudb-engine --lib -- change_log ball > "$L" 2>&1 ;;
esac
