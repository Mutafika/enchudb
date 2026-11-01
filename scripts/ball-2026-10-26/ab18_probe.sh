#!/bin/bash
# 18 回目 (M): probe18 を merge の前 (cea09f4) と後 (87e4559) で交互に。 1 行 = {v, r, load, PROBE18 の中身}
S=/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/71308e5d-c18e-4313-bf9c-4a796bf6373b/scratchpad
N=${1:-6}
BIN_pre=$S/tgt-ball16pre/debug/deps/probe18-41ecbeb7a6bc5d83
BIN_post=$S/tgt-ball16/debug/deps/probe18-428a56b7ed2eefb6
DIR_pre=/Users/kubo/Desktop/mutafika/enchudb-ball16/crates/enchudb-schema
DIR_post=/Users/kubo/Desktop/mutafika/enchudb-ball/crates/enchudb-schema
for r in $(seq 1 $N); do
  if (( r % 2 )); then order="pre post"; else order="post pre"; fi
  for v in $order; do
    bin=$(eval echo \$BIN_$v); dir=$(eval echo \$DIR_$v)
    load=$(sysctl -n vm.loadavg | awk '{print $2}')
    line=$(cd $dir && PROBE_SECS=8 $bin --ignored --nocapture 2>&1 | grep '^PROBE18 ' | sed 's/^PROBE18 //')
    echo "{\"v\":\"$v\",\"r\":$r,\"load\":$load,\"p\":${line:-null}}"
  done
done
