#!/bin/sh
# 17 回目 (P1): 16 回目 (0.37.2 の上、 $B16) と 17 回目 (master 47433de の上、 $B17) の ballp `l` を seed ごとに交互に。 WAL は BALL_WAL_MIB。
# 16 回目の run_l16.sh と同じ形 (v15 / v16 → v16 / v17)。
S=$(cd "$(dirname "$0")" && pwd)
RES=${RES:-$S/results_ballp_l17.jsonl}
: > "$RES"
export BALL_DIR=$S/balldb
mkdir -p "$BALL_DIR"
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
quiet() { t=0; while [ "$(load1 | awk '{print ($1 < 8) ? 1 : 0}')" != 1 ]; do sleep 5; t=$((t + 5)); [ $t -ge 1800 ] && break; done; }
for seed in ${SEEDS:-41 42 43 44 45 46}; do
  for spec in "16 160000" "64 640000"; do
    set -- $spec
    if [ $((seed % 2)) = 0 ]; then order="v17 v16"; else order="v16 v17"; fi
    for v in $order; do
      [ $v = v16 ] && B=$B16 || B=$B17
      quiet; l=$(load1)
      "$B" l "$1" "$2" "$seed" | sed "s/^{/{\"v\":\"$v\",\"load\":$l,/" >> "$RES"
      echo "$v l log=${1}MiB seed=$seed load=$l"
    done
  done
done
