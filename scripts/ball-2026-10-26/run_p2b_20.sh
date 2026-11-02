#!/bin/sh
# 20 回目 (P2b): 持つ回の記録の先読み (直し 7) の前後。 P2a と同じ形 (`ballp l`、 書き手 1 本、 listener あり、 BALL_REFOLD_WAIT=1000000,3)、
# WAL 64 MiB × 記録 16 / 64 MiB、 20a (測りだけ) と 20b / 20c (直し 7、 $VB) を seed ごとに交互 (偶数の seed は B が先)。 $A / $B = 20a / B の binary
: "${A:?}" "${B:?}"
VB=${VB:-v20b}  # B の版の名前 (2 回目は v20c)
S=$(cd "$(dirname "$0")" && pwd)
RES=${RES:-$S/results_ballp_p2b_20.jsonl}
export BALL_DIR=${BALL_DIR:-$S/balldb} BALL_LISTEN=1
mkdir -p "$BALL_DIR"
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
quiet() { t=0; while [ "$(load1 | awk '{print ($1 < 8) ? 1 : 0}')" != 1 ]; do sleep 5; t=$((t + 5)); [ $t -ge 1800 ] && break; done; }
for seed in ${SEEDS:-41 42 43 44 45 46}; do
  for spec in "16 160000" "64 640000"; do
    set -- $spec
    if [ $((seed % 2)) = 0 ]; then order="$VB v20a"; else order="v20a $VB"; fi
    for v in $order; do
      if [ $v = v20a ]; then bin=$A; else bin=$B; fi
      quiet; l=$(load1)
      BALL_WAL_MIB=64 BALL_REFOLD_WAIT=1000000,3 "$bin" l "$1" "$2" "$seed" | sed "s/^{/{\"v\":\"$v\",\"load\":$l,/" >> "$RES"
      echo "$v log=${1}MiB seed=$seed load=$l"
    done
  done
done
