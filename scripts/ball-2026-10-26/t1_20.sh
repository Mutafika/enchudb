#!/bin/sh
# 20 回目 (T1): engine + schema + oplog の試験 (--no-fail-fast) を nice で。 30 秒ごとに 時刻 / 負荷 / swap の使用 / メモリの空き (%) を控える。
# $BALL_S = target の場所 (tgt-ball の親)
: "${BALL_S:?}"
export CARGO_TARGET_DIR="$BALL_S/tgt-ball"
S=$(cd "$(dirname "$0")" && pwd)
( while :; do echo "$(date +%T) $(sysctl -n vm.loadavg) $(sysctl -n vm.swapusage | awk '{print $6}') $(memory_pressure | awk '/free percentage/ {print $5}')" >> "$S/t1_20_sys.log"; sleep 30; done ) &
M=$!
nice -n 10 cargo test --no-fail-fast -p enchudb-engine -p enchudb-schema -p enchudb-oplog > "$S/t1_20.log" 2>&1
rc=$?
kill $M
exit $rc
