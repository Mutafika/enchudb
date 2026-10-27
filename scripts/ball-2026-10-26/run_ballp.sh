#!/bin/sh
# 11 回目 (球の土台) の測り。 基準は BALL_PLAN.md の 11 回目 (回す前に書いた):
# (P1) 組み直しの時間: 記録の record 1 件あたり ≤ 1 µs。 段ごと (読む / 当てる / 今と比べる) に出す
# (P2) 記録が捨て続ける時、 土台あり / なし で WAL 付きの書き手 1 本の書き換えの時間の差 ≤ 5% (記録 1 MiB。 16 MiB も並べて出す、
#      16 MiB は回す前の基準に無い: 1 MiB の初回が +26% だった後に足した)
# (P3) 開く時間: 土台があっても変わらない (2 回目の土台あり vs 土台を消した後、 どちらも温まった後)
# 負荷 > 8 の回は待つ (30 分で諦めて印を付けて回す)。 binary は $B (scratchpad の ballp crate を release で組んだ物)
S=$(cd "$(dirname "$0")" && pwd)
B=${B:-$S/ballp-v11.bin}
export BALL_DIR=$S/balldb
mkdir -p "$BALL_DIR"
R=${RES_R:-$S/results_ballp_r_v11.jsonl}
W=${RES_W:-$S/results_ballp_w_v11.jsonl}
: > "$R"
: > "$W"
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
freeg() { df -g / | awk 'NR==2 {print $4}'; }
quiet() { t=0; while [ "$(load1 | awk '{print ($1 < 8) ? 1 : 0}')" != 1 ]; do sleep 5; t=$((t + 5)); [ $t -ge 1800 ] && break; done; }
for seed in 41 42 43; do
  for spec in "16 160000" "64 640000" "256 2600000"; do
    set -- $spec
    quiet; l=$(load1); f=$(freeg)
    "$B" r "$1" "$2" "$seed" | sed "s/^{/{\"load\":$l,\"free_g\":$f,/" >> "$R"
    echo "r log=${1}MiB writes=$2 seed=$seed load=$l"
  done
done
for seed in 41 42 43 44 45 46; do
  for m in 1 16; do
    for v in base nobase; do
      quiet; l=$(load1); f=$(freeg)
      BALL_LOG_MIB=$m "$B" w "$v" 400000 "$seed" | sed "s/^{/{\"load\":$l,\"free_g\":$f,/" >> "$W"
      echo "w log=${m}MiB $v seed=$seed load=$l"
    done
  done
done
