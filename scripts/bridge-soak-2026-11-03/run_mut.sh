#!/bin/bash
# mutate.py から呼ぶ: 変異の入った tree で bridge_soak を組み直して 60 秒回す。 止まり・落ち・取りこぼし・truncation があれば exit 1
#   python3 scripts/mutate.py scripts/bridge-soak-2026-11-03/mutations.json -- scripts/bridge-soak-2026-11-03/run_mut.sh
# 終わった後の target は変異入りの build のまま。 組み直してから他に使うこと
set -u
cd "$(dirname "$0")/../.." || exit 2
cargo build --release -q -p enchudb-sync --example bridge_soak || exit 2
TGT=${CARGO_TARGET_DIR:-target}
LOG=$(mktemp "${TMPDIR:-/tmp}/bridge_soak_mut.XXXXXX")
SOAK_SECS=${SOAK_SECS:-60} "$TGT/release/examples/bridge_soak" > "$LOG" 2> "$LOG.err"
rc=$?
echo "log=$LOG rc=$rc"
grep -E '"kind":"(stalled|recovered|dropped|done)"' "$LOG" | cut -c1-400
grep -c '#268' "$LOG.err" | sed 's/^/engine_268_warnings=/'
exit $rc
