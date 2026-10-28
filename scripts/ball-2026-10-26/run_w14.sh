#!/bin/sh
# 14 回目 (P2 / P3): 書き込みの値段。 VS の各要素は 「版:球」 (球 = 1 なら a / b / w を球にする)。 binary は $BIN_DIR/ballb-<版>.bin。
# seed ごとに順を回して交互に。 TH = 書き手の本数、 BALL_WAL=1 で WAL 付き (bench の BALL_WAL)。 基準は BALL_PLAN.md の 14 回目。
# 負荷 > 8 の回は待つ (30 分で諦めて印を付けて回す)、 空き 2 GiB 未満の回は判定から外す。
S=$(cd "$(dirname "$0")" && pwd)
RES=${RES:-$S/results_ballb_w14.jsonl}
SEEDS=${SEEDS:-"41 42 43 44 45 46"}
VS=${VS:-"v037:0 v14:0 v14:1"}
TH=${TH:-1}
: > "$RES"
export BALL_DIR=$S/balldb
mkdir -p "$BALL_DIR"
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
freeg() { df -g / | awk 'NR==2 {print $4}'; }
quiet() { t=0; while [ "$(load1 | awk '{print ($1 < 8) ? 1 : 0}')" != 1 ]; do sleep 5; t=$((t + 5)); [ $t -ge 1800 ] && break; done; }
k=0
for seed in $SEEDS; do
  set -- $VS; n=$#; i=0; order=""
  while [ $i -lt $n ]; do j=$(( (i + k) % n + 1 )); eval "order=\"\$order \${$j}\""; i=$((i + 1)); done
  k=$((k + 1))
  for vb in $order; do
    v=${vb%:*}; ball=${vb#*:}
    name=$v; [ "$ball" = 1 ] && name=${v}_on || name=${v}_off
    for th in $TH; do
      quiet; l=$(load1); f=$(freeg)
      BALL=$ball "$BIN_DIR/ballb-$v.bin" w $th $seed | sed "s/^{/{\"v\":\"$name\",\"load\":$l,\"free_g\":$f,/" >> "$RES"
      echo "$name th=$th seed=$seed load=$l free=${f}G"
    done
  done
done
