#!/bin/sh
# 13 回目 (W1): 12 回目の書き手の値段 +10 ns の理由。 11 回目 (v11) / 12 回目 (v12) / 12 回目から 1 つずつ戻した版 (v12a = WAL なしの印の
# 比べを外す、 v12b = for_write を get に戻す = 状態と閾値の load を外す、 v12ab = 両方) を、 書き手 1 本・WAL なし・球あり (BALL=1) で
# seed ごとに順を回して交互に回す。 binary は $BIN_DIR/ballb-<版>.bin (scratchpad で組んだ物、 commit しない)。
# 見る前に決めた基準 (BALL_PLAN.md の 13 回目): v12 − v11 の差の 2/3 以上を説明する版を名指しする (v12ab ≈ v11 なら a と b の和)。
# 負荷 > 8 の回は待つ (30 分で諦めて印を付けて回す)、 空き 2 GiB 未満の回は判定から外す。
S=$(cd "$(dirname "$0")" && pwd)
RES=${RES:-$S/results_ballb_w13.jsonl}
SEEDS=${SEEDS:-"41 42 43 44 45 46"}
VS=${VS:-"v11 v12 v12a v12b v12ab"}
TH=${TH:-1}
: > "$RES"
export BALL_DIR=$S/balldb BALL=1
mkdir -p "$BALL_DIR"
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
freeg() { df -g / | awk 'NR==2 {print $4}'; }
quiet() { t=0; while [ "$(load1 | awk '{print ($1 < 8) ? 1 : 0}')" != 1 ]; do sleep 5; t=$((t + 5)); [ $t -ge 1800 ] && break; done; }
k=0
for seed in $SEEDS; do
  # 順を seed ごとに 1 つずらす (いつも同じ版が先にならない)
  set -- $VS; n=$#; i=0; order=""
  while [ $i -lt $n ]; do j=$(( (i + k) % n + 1 )); eval "order=\"\$order \${$j}\""; i=$((i + 1)); done
  k=$((k + 1))
  for v in $order; do
    for th in $TH; do
      quiet; l=$(load1); f=$(freeg); t0=$(date +%s)
      "$BIN_DIR/ballb-$v.bin" w $th $seed | sed "s/^{/{\"v\":\"$v\",\"load\":$l,\"free_g\":$f,/" >> "$RES"
      echo "$v th=$th seed=$seed load=$l free=${f}G $(( $(date +%s) - t0 ))s"
    done
  done
done
