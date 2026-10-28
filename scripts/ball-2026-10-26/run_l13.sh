#!/bin/sh
# 13 回目 (P1): 12 回目 (544ca62 の ballp) と 13 回目の ballp を seed ごとに交互に、 `l` (書き手 1 本が書き続ける中で最初の読みが組む)。
# WAL は BALL_WAL_MIB (256 = 組む間に畳まない)。 binary は $B12 / $B13 (scratchpad で組んだ物)。 基準は BALL_PLAN.md の 13 回目
S=$(cd "$(dirname "$0")" && pwd)
RES=${RES:-$S/results_ballp_l13ab.jsonl}
: > "$RES"
export BALL_DIR=$S/balldb
mkdir -p "$BALL_DIR"
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
quiet() { t=0; while [ "$(load1 | awk '{print ($1 < 8) ? 1 : 0}')" != 1 ]; do sleep 5; t=$((t + 5)); [ $t -ge 1800 ] && break; done; }
for seed in ${SEEDS:-41 42 43 44 45 46}; do
  for spec in "16 160000" "64 640000"; do
    set -- $spec
    for v in v12 v13; do
      [ $v = v12 ] && B=$B12 || B=$B13
      quiet; l=$(load1)
      "$B" l "$1" "$2" "$seed" | sed "s/^{/{\"v\":\"$v\",\"load\":$l,/" >> "$RES"
      echo "$v l log=${1}MiB seed=$seed load=$l"
    done
  done
done
