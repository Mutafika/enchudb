#!/bin/bash
# 17 回目: T7 を単独で最後まで (3 session + 照合)、 merge の前 (cea09f4) と後 (87e4559) を交互に。 上限 CAP 秒で止める。
# 終わったか / 時間 / 負荷 / session ごとの 「組む:」 の行を残す
S=/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/71308e5d-c18e-4313-bf9c-4a796bf6373b/scratchpad
N=${1:-4}
CAP=${CAP:-400}
O=$S/hang17/abfull
mkdir -p $O
BIN_pre=$S/tgt-ball16pre/debug/deps/ball_lazy-5f1429a5b057ff7d
BIN_post=$S/tgt-ball16/debug/deps/ball_lazy-7acdd4e82ec46d01
DIR_pre=/Users/kubo/Desktop/mutafika/enchudb-ball16/crates/enchudb-schema
DIR_post=/Users/kubo/Desktop/mutafika/enchudb-ball/crates/enchudb-schema
for r in $(seq 1 $N); do
  if (( r % 2 )); then order="pre post"; else order="post pre"; fi
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
