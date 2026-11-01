#!/bin/sh
# 18 回目 (P1): 17 回目 (master 47433de の上、 $B17) と 18 回目 (追いつきの読み直しに上限、 $B18) の ballp `l` を seed ごとに交互に。 WAL は BALL_WAL_MIB。
# 17 回目の run_l17.sh と同じ形 (v16 / v17 → v17 / v18)。
S=$(cd "$(dirname "$0")" && pwd)
RES=${RES:-$S/results_ballp_l18.jsonl}
: > "$RES"
export BALL_DIR=$S/balldb
mkdir -p "$BALL_DIR"
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
quiet() { t=0; while [ "$(load1 | awk '{print ($1 < 8) ? 1 : 0}')" != 1 ]; do sleep 5; t=$((t + 5)); [ $t -ge 1800 ] && break; done; }
for seed in ${SEEDS:-41 42 43 44 45 46}; do
  for spec in "16 160000" "64 640000"; do
    set -- $spec
    if [ $((seed % 2)) = 0 ]; then order="v18 v17"; else order="v17 v18"; fi
    for v in $order; do
      [ $v = v17 ] && B=$B17 || B=$B18
      quiet; l=$(load1)
      "$B" l "$1" "$2" "$seed" | sed "s/^{/{\"v\":\"$v\",\"load\":$l,/" >> "$RES"
      echo "$v l log=${1}MiB seed=$seed load=$l"
    done
  done
done
