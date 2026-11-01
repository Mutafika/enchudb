#!/bin/bash
# 18 回目 (食い違いの調べ): ball_lazy を 8 本並べて N 回。 BALL_KEEP で、 食い違った試験の開く前の写しと控えを残す。 上限 CAP 秒で止める
S=/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/71308e5d-c18e-4313-bf9c-4a796bf6373b/scratchpad
BIN=${BIN:-$S/r18/ball_lazy-v18f.bin}
CAP=${CAP:-300}
N=${1:-6}
O=$S/r18/par8k
K=$S/r18/keep
mkdir -p $O $K
cd /Users/kubo/Desktop/mutafika/enchudb-ball/crates/enchudb-schema || exit 2
for r in $(seq 1 $N); do
  load=$(sysctl -n vm.loadavg | awk '{print $2}')
  pids=""
  for k in 1 2 3 4 5 6 7 8; do
    BALL_KEEP=$K $BIN --nocapture > $O/$r-$k.out 2>&1 &
    pids="$pids $!"
  done
  t=0
  while [ $t -lt $CAP ]; do
    alive=0; for p in $pids; do kill -0 $p 2>/dev/null && alive=1; done
    [ $alive = 0 ] && break
    sleep 2; t=$((t+2))
  done
  for p in $pids; do kill -0 $p 2>/dev/null && { kill $p; rm -rf /private/tmp/enchudb-ball-lazy-t*-${p:?}; }; done
  load2=$(sysctl -n vm.loadavg | awk '{print $2}')
  ok=$(grep -l '^test result: ok' $O/$r-*.out | wc -l | tr -d ' ')
  ref=$(grep -ho '断られた [0-9]*' $O/$r-*.out | awk '{s+=$2} END {print s+0}')
  echo "r=$r t=$t load=${load}-${load2} ok=$ok/8 refused=$ref $(grep -h -A1 'panicked at' $O/$r-*.out | grep -v 'panicked at' | grep -v '^--' | cut -c1-70 | sort | uniq -c | tr '\n' ' ')"
done
echo "kept: $(ls $K | tr '\n' ' ')"
