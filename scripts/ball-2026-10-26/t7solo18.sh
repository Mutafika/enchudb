#!/bin/bash
# 18 回目 (T7): 直した後の T7 を単独で最後まで N 回 (上限 CAP 秒)。 1 行 = {r, load, t, res, session ごとの 「組む:」 の行}
S=/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/71308e5d-c18e-4313-bf9c-4a796bf6373b/scratchpad
N=${1:-8}
CAP=${CAP:-400}
BIN=${BIN:-$S/r18/ball_lazy-v18a.bin}
O=$S/r18/t7solo
mkdir -p $O
for r in $(seq 1 $N); do
  load=$(sysctl -n vm.loadavg | awk '{print $2}')
  out=$O/$r.out
  (cd /Users/kubo/Desktop/mutafika/enchudb-ball/crates/enchudb-schema && exec $BIN --exact a_build_drops_what_it_already_applied_from_the_side_notes --nocapture > $out 2>&1) &
  pid=$!
  t=0
  while [ $t -lt $CAP ] && kill -0 $pid 2>/dev/null; do sleep 1; t=$((t+1)); done
  if kill -0 $pid 2>/dev/null; then
    kill $pid; wait $pid 2>/dev/null; res=capped
    rm -rf "/tmp/enchudb-ball-lazy-t7-${pid:?}"
  else
    wait $pid; rc=$?; res="rc$rc"
  fi
  echo "{\"v\":\"v18\",\"r\":$r,\"load\":$load,\"t\":$t,\"res\":\"$res\",\"lines\":$(grep '^組む:' $out | python3 -I -c 'import json,sys; print(json.dumps([l.strip() for l in sys.stdin]))')}"
done
