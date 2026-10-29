#!/bin/sh
# 15 回目 (P1): 14 回目 (0.37.0 の上、 $B14) と 15 回目 (0.37.1 の上、 $B15) の ballp `l` を seed ごとに交互に。 WAL は BALL_WAL_MIB。
# 14 回目の run_l14.sh と同じ形 (v13 / v14 → v14 / v15)。
S=$(cd "$(dirname "$0")" && pwd)
RES=${RES:-$S/results_ballp_l15.jsonl}
: > "$RES"
export BALL_DIR=$S/balldb
mkdir -p "$BALL_DIR"
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
quiet() { t=0; while [ "$(load1 | awk '{print ($1 < 8) ? 1 : 0}')" != 1 ]; do sleep 5; t=$((t + 5)); [ $t -ge 1800 ] && break; done; }
for seed in ${SEEDS:-41 42 43 44 45 46}; do
  for spec in "16 160000" "64 640000"; do
    set -- $spec
    if [ $((seed % 2)) = 0 ]; then order="v15 v14"; else order="v14 v15"; fi
    for v in $order; do
      [ $v = v14 ] && B=$B14 || B=$B15
      quiet; l=$(load1)
      "$B" l "$1" "$2" "$seed" | sed "s/^{/{\"v\":\"$v\",\"load\":$l,/" >> "$RES"
      echo "$v l log=${1}MiB seed=$seed load=$l"
    done
  done
done
