#!/bin/bash
# 18 回目: T7 を単独で最後まで、 17 回目 (879b61c) と直した後 (v18a) を交互に。 上限 CAP 秒
S=/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/71308e5d-c18e-4313-bf9c-4a796bf6373b/scratchpad
N=${1:-4}
CAP=${CAP:-200}
BIN_v17=$S/r18/ball_lazy-v17.bin
BIN_v18=${BIN18:-$S/r18/ball_lazy-v18a.bin}
DIR_v17=/Users/kubo/Desktop/mutafika/enchudb-ball17/crates/enchudb-schema
DIR_v18=/Users/kubo/Desktop/mutafika/enchudb-ball/crates/enchudb-schema
O=$S/r18/ab1718
mkdir -p $O
for r in $(seq 1 $N); do
  if (( r % 2 )); then order="v17 v18"; else order="v18 v17"; fi
  for v in $order; do
    bin=$(eval echo \$BIN_$v); dir=$(eval echo \$DIR_$v)
    load=$(sysctl -n vm.loadavg | awk '{print $2}')
    out=$O/$v-$r.out
    (cd $dir && exec $bin --exact a_build_drops_what_it_already_applied_from_the_side_notes --nocapture > $out 2>&1) &
    pid=$!
    t=0
    while [ $t -lt $CAP ] && kill -0 $pid 2>/dev/null; do sleep 1; t=$((t+1)); done
    if kill -0 $pid 2>/dev/null; then
      kill $pid; wait $pid 2>/dev/null; res=capped
      rm -rf "/tmp/enchudb-ball-lazy-t7-${pid:?}"
    else
      wait $pid; rc=$?; res="rc$rc"
    fi
    echo "{\"v\":\"$v\",\"r\":$r,\"load\":$load,\"t\":$t,\"res\":\"$res\",\"lines\":$(grep '^組む:' $out | python3 -I -c 'import json,sys; print(json.dumps([l.strip() for l in sys.stdin]))')}"
  done
done
