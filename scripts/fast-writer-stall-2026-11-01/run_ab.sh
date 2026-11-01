#!/bin/bash
# #683 の A/B: 版ごとの foldstall (foldstall_probe.rs を、 probe.patch を当てた版の checkout に向けて組んだもの) を seed ごとに順を回して交互に。
# 全力 4 秒、 WAL 64 MiB、 記録 64 MiB、 listener の溜めの上限 4 GiB。 連想配列を使わない (macOS の /bin/bash は 3.2)。
# 使い方: BIN_base=<path> BIN_fix=<path> [BIN_f1=<path>] VERS="base f1 fix" MODES="none log" FOLD_DIR=<dir> run_ab.sh <出力 jsonl> <seed の始め> <seed の終わり>
# 段ごとの時刻は TRACE_DIR を置くと <TRACE_DIR>/<版>-<形>-<seed>.txt に書く (stages.py / busy.py で読む)
OUT=$1; A=$2; Z=$3
for seed in $(seq $A $Z); do
  set -- ${VERS:-base fix}
  k=$(( seed % $# )); order=""
  for i in $(seq 0 $(($# - 1))); do j=$(( (i + k) % $# + 1 )); order="$order ${!j}"; done
  for mode in ${MODES:-none log loglisten}; do
    for v in $order; do
      load=$(sysctl -n vm.loadavg | awk '{print $2}')
      bin=$(eval echo \$BIN_$v)
      trace=${TRACE_DIR:+$TRACE_DIR/$v-$mode-$seed.txt}
      line=$(env ${trace:+ENCHU_T683=$trace} FOLD_RATE=0 FOLD_BACKLOG_MIB=4096 $bin $mode 4 $seed)
      echo "{\"v\":\"$v\",\"load\":$load,${line:1}" >> $OUT
    done
  done
done
