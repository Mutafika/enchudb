#!/bin/sh
# 20 回目 (P2a): 持つ回の内訳 (held_parts_ns / held_parts_n)。 19 回目の P2 と同じ形 (`ballp l`、 書き手 1 本、 listener あり、
# BALL_REFOLD_WAIT=1000000,3 で 4 回目を持って読ませる)、 WAL 64 MiB × 記録 16 / 64 MiB、 持つ回だけ。 $B = ballp の binary、 $V = 版の名前
: "${B:?}" "${V:?}"
S=$(cd "$(dirname "$0")" && pwd)
RES=${RES:-$S/results_ballp_p2a_20.jsonl}
export BALL_DIR=${BALL_DIR:-$S/balldb} BALL_LISTEN=1
mkdir -p "$BALL_DIR"
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
quiet() { t=0; while [ "$(load1 | awk '{print ($1 < 8) ? 1 : 0}')" != 1 ]; do sleep 5; t=$((t + 5)); [ $t -ge 1800 ] && break; done; }
for seed in ${SEEDS:-41 42 43 44 45 46}; do
  for wal in ${WALS:-64}; do
    for spec in "16 160000" "64 640000"; do
      set -- $spec
      quiet; l=$(load1)
      BALL_WAL_MIB=$wal BALL_REFOLD_WAIT=1000000,3 "$B" l "$1" "$2" "$seed" | sed "s/^{/{\"v\":\"$V\",\"load\":$l,/" >> "$RES"
      echo "$V wal=${wal}MiB log=${1}MiB seed=$seed load=$l"
    done
  done
done
