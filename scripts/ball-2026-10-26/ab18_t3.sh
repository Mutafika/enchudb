#!/bin/bash
# 18 回目: T3 (the_first_read_builds_while_writers_write) だけを P 本並べて、 v17 と v18e を交互に N 回ずつ。 答えの食い違い (panicked) を数える
S=/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/71308e5d-c18e-4313-bf9c-4a796bf6373b/scratchpad
N=${1:-5}
P=${P:-12}
BIN_v17=$S/r18/ball_lazy-v17.bin
BIN_v18=${BIN18:-$S/r18/ball_lazy-v18e.bin}
DIR_v17=/Users/kubo/Desktop/mutafika/enchudb-ball17/crates/enchudb-schema
DIR_v18=/Users/kubo/Desktop/mutafika/enchudb-ball/crates/enchudb-schema
O=$S/r18/abt3
mkdir -p $O
for r in $(seq 1 $N); do
  if (( r % 2 )); then order="v17 v18"; else order="v18 v17"; fi
  for v in $order; do
    bin=$(eval echo \$BIN_$v); dir=$(eval echo \$DIR_$v)
    load=$(sysctl -n vm.loadavg | awk '{print $2}')
    pids=""
    for k in $(seq 1 $P); do
      (cd $dir && exec $bin --exact the_first_read_builds_while_writers_write > $O/$v-$r-$k.out 2>&1) &
      pids="$pids $!"
    done
    for p in $pids; do wait $p; done
    ok=$(grep -l 'test result: ok' $O/$v-$r-*.out | wc -l | tr -d ' ')
    bad=$(grep -l 'panicked' $O/$v-$r-*.out | wc -l | tr -d ' ')
    echo "$v r=$r load=$load ok=$ok/$P bad=$bad $(grep -h -A1 'panicked at' $O/$v-$r-*.out | grep -v panicked | grep -v '^--' | cut -c1-60 | sort | uniq -c | tr '\n' ' ')"
  done
done
