#!/bin/sh
# 19 回目 (P2): 持って読む回の値段。 `ballp l` (書き手 1 本、 listener あり) で、 組みの最初の 3 回の読みを読み直しにして (BALL_REFOLD_WAIT=1000000,3)、
# 4 回目を change_emit を持って読ませる (held)。 同じ形で球なし (BALL_NONE、 none) と seed ごとに交互。 WAL 4 / 64 MiB × 記録 16 / 64 MiB。 $B = ballp-v19
S=$(cd "$(dirname "$0")" && pwd)
RES=${RES:-$S/results_ballp_p2_19.jsonl}
: > "$RES"
export BALL_DIR=$S/balldb BALL_LISTEN=1
mkdir -p "$BALL_DIR"
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
quiet() { t=0; while [ "$(load1 | awk '{print ($1 < 8) ? 1 : 0}')" != 1 ]; do sleep 5; t=$((t + 5)); [ $t -ge 1800 ] && break; done; }
for seed in ${SEEDS:-41 42 43 44 45 46}; do
  for wal in 4 64; do
    for spec in "16 160000" "64 640000"; do
      set -- $spec
      if [ $((seed % 2)) = 0 ]; then order="none held"; else order="held none"; fi
      for v in $order; do
        quiet; l=$(load1)
        if [ $v = held ]; then
          BALL_WAL_MIB=$wal BALL_REFOLD_WAIT=1000000,3 "$B" l "$1" "$2" "$seed" | sed "s/^{/{\"v\":\"$v\",\"load\":$l,/" >> "$RES"
        else
          BALL_WAL_MIB=$wal BALL_NONE=1 "$B" l "$1" "$2" "$seed" | sed "s/^{/{\"v\":\"$v\",\"load\":$l,/" >> "$RES"
        fi
        echo "$v wal=${wal}MiB log=${1}MiB seed=$seed load=$l"
      done
    done
  done
done
