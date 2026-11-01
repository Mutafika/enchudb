#!/bin/bash
# #683 の続き (記録への写しを直接) の A/B: base (master) と new (この branch) を seed ごとに順を入れ替えて交互に。 全力 4 秒。
# 使い方: BIN_base=<master の foldstall> BIN_new=<この branch の foldstall> FOLD_DIR=<作業 dir> run_ab_direct.sh <出力 jsonl> <seed の始め> <seed の終わり>
# MODES で形を (既定 "log none")。 foldstall は scripts/fold-stall-2026-10-29/ (組み方は同じ README)
OUT=$1; A=$2; Z=$3
for seed in $(seq $A $Z); do
  if (( seed % 2 )); then order="base new"; else order="new base"; fi
  for mode in ${MODES:-log none}; do
    for v in $order; do
      load=$(sysctl -n vm.loadavg | awk '{print $2}')
      bin=$(eval echo \$BIN_$v)
      line=$(FOLD_RATE=0 FOLD_BACKLOG_MIB=4096 $bin $mode 4 $seed)
      echo "{\"v\":\"$v\",\"load\":$load,${line:1}" >> $OUT
    done
  done
done
