#!/bin/sh
# #667 の退行の確かめ: 0.37.1 (base) と直し (D) を、 #662 の測りの形 4 つ × 速さ 2 つ (全力 / 50 万件/秒) で seed ごとに交互に。
# WAL 64 MiB、 記録 64 MiB、 listener の溜めの上限 4 GiB (#664 の表と同じ)、 4 秒。 1 回ごとに負荷 12 を切るのを待つ。
SP=/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/71308e5d-c18e-4313-bf9c-4a796bf6373b/scratchpad
OUT=${OUT:-$SP/ab667w.jsonl}
: > $OUT
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
quiet() { t=0; while [ "$(load1 | awk '{print ($1 < 12) ? 1 : 0}')" != 1 ]; do sleep 5; t=$((t + 5)); [ $t -ge 1800 ] && break; done; }
for seed in 1 2 3; do
  for rate in 0 500000; do
    for mode in none log loglisten listen; do
      if [ $((seed % 2)) = 1 ]; then order="base D"; else order="D base"; fi
      for v in $order; do
        [ $v = base ] && B=$SP/foldstall_663/target/release/foldstall_663 || B=$SP/fs667D/target/release/foldstallD
        quiet; l=$(load1)
        line=$(FOLD_DIR=$SP/fold256 FOLD_RATE=$rate FOLD_BACKLOG_MIB=4096 $B $mode 4 $seed)
        echo "{\"ver\":\"$v\",\"load\":$l,${line#\{}" >> $OUT
      done
    done
  done
done
