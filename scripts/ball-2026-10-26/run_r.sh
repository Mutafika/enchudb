#!/bin/sh
# 読みの段 (ballb r) を前の版の binary と交互に回す (seed 41〜43)。 VERSIONS の名前: cur = 今の target、 vN = 控えた ballb-proto-vN.bin
# 見る前に決めた基準 (6 回目、 回す前に書いた):
# (R1) 全部の回に ok の行がある (今と t5 の答え合わせが binary の中で通った)
# (R2) pull_at_t5 (真ん中の時点) を前の版と並べる。 見込み: 6 回目は 5 回目 (269 µs) の半分以下
# (R3) pull_at_now / pull_now / value_at_t5 は前の版の ±20% に収まる (bit の表は差分の少ない読みでは使わない)
# (R4) 負荷 > 8 の回は判定から外す (印を付けて残す)
S=$(cd "$(dirname "$0")" && pwd)
RES=${RES:-$S/results_ballb_r.jsonl}
: > "$RES"
export BALL_DIR=$S/balldb
mkdir -p "$BALL_DIR"
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
quiet() { t=0; while [ "$(load1 | awk '{print ($1 < 8) ? 1 : 0}')" != 1 ]; do sleep 5; t=$((t + 5)); [ $t -ge 1800 ] && break; done; }
for seed in ${SEEDS:-41 42 43}; do for v in ${VERSIONS:-v5 cur}; do
  case $v in
    cur) B=$S/target-ballb-proto/release/ballb ;;
    *) B=$S/ballb-proto-$v.bin ;;
  esac
  quiet; l=$(load1)
  BALL=1 "$B" r $seed | sed "s/^{/{\"v\":\"$v\",\"load\":$l,/" >> "$RES"
  echo "$v seed=$seed load=$l"
done; done
