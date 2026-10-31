#!/bin/bash
# 16 回目 (ii): 記録の読みの crc を lock の外へ。 前 (ballp-v16.bin) / 後 (ballp-v16c.bin) の ballp `l` を seed ごとに交互に (WAL 64 MiB、 負荷待ちは 60 秒まで)
S=/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/71308e5d-c18e-4313-bf9c-4a796bf6373b/scratchpad
RES=$S/w16/c2/results.jsonl
: > $RES
export BALL_DIR=$S/w16/c2/db BALL_WAL_MIB=64
mkdir -p $BALL_DIR
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
quiet() { t=0; while [ "$(load1 | awk '{print ($1 < 8) ? 1 : 0}')" != 1 ] && [ $t -lt 60 ]; do sleep 5; t=$((t+5)); done; }
for seed in 41 42 43 44 45 46 47 48 49 50 51 52; do
  for spec in "16 160000" "64 640000"; do
    set -- $spec
    if [ $((seed % 2)) = 0 ]; then order="v16c v16"; else order="v16 v16c"; fi
    for v in $order; do
      quiet; l=$(load1)
      perl -e 'alarm shift; exec @ARGV' 120 $S/ballp-$v.bin l $1 $2 $seed | sed "s/^{/{\"v\":\"$v\",\"load\":$l,/" >> $RES
      echo "$v $1 $seed load=$l"
    done
  done
done
