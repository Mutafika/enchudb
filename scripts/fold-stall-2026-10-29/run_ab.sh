#!/bin/zsh
# 2 つの版 (BASE_BIN / FIX_BIN) を seed ごとに交互に。 形 4 つ × 速さ 2 つ (全力 / 50 万件/秒)、 4 秒ずつ。
# 使い方: BASE_BIN=... FIX_BIN=... FOLD_DIR=/tmp/fold OUT=results_ab.jsonl ./run_ab.sh
mkdir -p $FOLD_DIR
for seed in 1 2 3; do
  for rate in 0 500000; do
    for mode in none log loglisten listen; do
      if (( seed % 2 )); then order=(base fix); else order=(fix base); fi
      for ver in $order; do
        if [[ $ver == base ]]; then bin=$BASE_BIN; else bin=$FIX_BIN; fi
        load=$(sysctl -n vm.loadavg | awk '{print $2}')
        line=$(FOLD_RATE=$rate FOLD_BACKLOG_MIB=4096 $bin $mode 4 $seed)
        echo "{\"ver\":\"$ver\",\"load\":$load,${line#\{}" >> $OUT
      done
    done
  done
done
