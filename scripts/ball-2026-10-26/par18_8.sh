#!/bin/bash
# 18 回目: ball_lazy を 8 本並べて 1 回 (--nocapture で、 組む側の thread の panic もその場で出す)。 上限 CAP 秒で止める。 $1 = 回の番号
S=/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/71308e5d-c18e-4313-bf9c-4a796bf6373b/scratchpad
BIN=${BIN:-$S/r18/ball_lazy-v18d.bin}
CAP=${CAP:-300}
O=${O:-$S/r18/par8n}
mkdir -p $O
cd ${DIR:-/Users/kubo/Desktop/mutafika/enchudb-ball/crates/enchudb-schema} || exit 2
pids=""
for k in 1 2 3 4 5 6 7 8; do
  $BIN --nocapture > $O/$1-$k.out 2>&1 &
  pids="$pids $!"
done
t=0
while [ $t -lt $CAP ]; do
  alive=0; for p in $pids; do kill -0 $p 2>/dev/null && alive=1; done
  [ $alive = 0 ] && break
  sleep 2; t=$((t+2))
done
for p in $pids; do kill -0 $p 2>/dev/null && { kill $p; rm -rf /private/tmp/enchudb-ball-lazy-t*-${p:?}; }; done
for k in 1 2 3 4 5 6 7 8; do
  echo "$1-$k t=${t} $(grep -E '^test result' $O/$1-$k.out || echo 'NO RESULT') $(grep -E '\.\.\. FAILED' $O/$1-$k.out | sed 's/test //; s/ \.\.\. FAILED//' | tr '\n' ' ') $(grep -c '持って読んだのに畳まれた' $O/$1-$k.out) asserts"
done
