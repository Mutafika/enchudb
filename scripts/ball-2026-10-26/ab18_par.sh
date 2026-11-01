#!/bin/bash
# 18 回目: ball_lazy の binary 全体を 8 本並べる形を、 v17 と v18e で交互に N 回ずつ。 panic の場所 (file:行) ごとに数える
S=/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/71308e5d-c18e-4313-bf9c-4a796bf6373b/scratchpad
N=${1:-6}
for r in $(seq 1 $N); do
  if (( r % 2 )); then order="v17 v18"; else order="v18 v17"; fi
  for v in $order; do
    if [ $v = v17 ]; then B=$S/r18/ball_lazy-v17.bin; D=/Users/kubo/Desktop/mutafika/enchudb-ball17/crates/enchudb-schema; else B=${BIN18:-$S/r18/ball_lazy-v18e.bin}; D=/Users/kubo/Desktop/mutafika/enchudb-ball/crates/enchudb-schema; fi
    load=$(sysctl -n vm.loadavg | awk '{print $2}')
    BIN=$B DIR=$D O=$S/r18/abpar/$v CAP=200 bash $S/r18/par8n.sh $r > /dev/null 2>&1
    echo "$v r=$r load=$load ok=$(grep -l 'test result: ok' $S/r18/abpar/$v/$r-*.out | wc -l | tr -d ' ')/8 panics: $(grep -h -o 'panicked at crates/[a-z_/-]*\.rs:[0-9]*' $S/r18/abpar/$v/$r-*.out | sed 's/panicked at //' | sort | uniq -c | tr '\n' ' ')"
  done
done
