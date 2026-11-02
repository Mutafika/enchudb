#!/bin/sh
# 19 回目 (T2) の変異の試験 (16 回目の mut16_tests.sh と同じ中身。 11-02 に Mac が再起動して /tmp の scratchpad ごと消えたので repo に置く)。
# $1 = a (schema の球の試験) | b (engine の記録と球の単体)。 BALL_S = target と run ごとの出力を置く directory
: "${BALL_S:?}"
export CARGO_TARGET_DIR="$BALL_S/tgt-ball"
mkdir -p "$BALL_S/mut19$1"
L="$BALL_S/mut19$1/$(date +%s).log"
case $1 in
  a) cargo test --no-fail-fast -p enchudb-schema --test ball_lazy --test ball_reopen --test ball_history > "$L" 2>&1 ;;
  b) cargo test --no-fail-fast -p enchudb-engine --lib -- change_log ball > "$L" 2>&1 ;;
  *) exit 2 ;;
esac
