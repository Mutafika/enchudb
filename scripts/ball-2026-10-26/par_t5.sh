#!/bin/bash
# 18 回目 (食い違いの調べ): T5 だけを P 本と ball_lazy 全体を Q 本並べて N 回 (負荷を上げて T5 の食い違いを捕まえる)。 BALL_KEEP で写しを残す
S=/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/71308e5d-c18e-4313-bf9c-4a796bf6373b/scratchpad
BIN=${BIN:-$S/r18/ball_lazy-v18g.bin}
CAP=${CAP:-300}
N=${1:-20}
P=${P:-12}
Q=${Q:-4}
O=$S/r18/part5
K=$S/r18/keep5
mkdir -p $O $K
cd /Users/kubo/Desktop/mutafika/enchudb-ball/crates/enchudb-schema || exit 2
for r in $(seq 1 $N); do
  load=$(sysctl -n vm.loadavg | awk '{print $2}')
  pids=""
  for k in $(seq 1 $P); do
    BALL_KEEP=$K $BIN --exact a_build_continues_the_wal_where_it_stopped --nocapture > $O/$r-t5-$k.out 2>&1 &
    pids="$pids $!"
  done
  for k in $(seq 1 $Q); do
    BALL_KEEP=$K $BIN --nocapture > $O/$r-all-$k.out 2>&1 &
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
  bad=$(grep -l '書き手と並んで組んだ後: 回\|開き直した後: 回\|面の数: 球' $O/$r-*.out 2>/dev/null | wc -l | tr -d ' ')
  echo "r=$r t=$t load=${load}-${load2} mismatch=$bad $(grep -h -A1 'panicked at' $O/$r-*.out | grep -v 'panicked at' | grep -v '^--' | cut -c1-50 | sed -E 's/[0-9]+/N/g' | sort | uniq -c | tr '\n' ' ')"
done
