#!/bin/bash
# 17 回目: T7 (a_build_drops_what_it_already_applied_from_the_side_notes) を単独で、 merge の前 (cea09f4) と後 (87e4559) を交互に。
# 1 回目の session の組みが終わって 「組む:」 の行が出たら (または 300 秒で) 止める (照合は待たない)。 行と負荷と時間を残す
S=/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/71308e5d-c18e-4313-bf9c-4a796bf6373b/scratchpad
N=${1:-4}
O=$S/hang17/ab
mkdir -p $O
BIN_pre=$S/tgt-ball16pre/debug/deps/$(ls $S/tgt-ball16pre/debug/deps | grep -E '^ball_lazy-[0-9a-f]+$' | head -1)
BIN_post=$S/tgt-ball16/debug/deps/ball_lazy-7acdd4e82ec46d01
DIR_pre=/Users/kubo/Desktop/mutafika/enchudb-ball16/crates/enchudb-schema
DIR_post=/Users/kubo/Desktop/mutafika/enchudb-ball/crates/enchudb-schema
cmp -s "$BIN_pre" "$BIN_post" && { echo "pre と post が同じ binary"; exit 1; }
for r in $(seq 1 $N); do
  if (( r % 2 )); then order="pre post"; else order="post pre"; fi
  for v in $order; do
    bin=$(eval echo \$BIN_$v); dir=$(eval echo \$DIR_$v)
    load=$(sysctl -n vm.loadavg | awk '{print $2}')
    out=$O/$v-$r.out
    (cd $dir && exec $bin --exact a_build_drops_what_it_already_applied_from_the_side_notes --nocapture > $out 2>&1) &
    pid=$!
    t=0
    while [ $t -lt 300 ] && kill -0 $pid 2>/dev/null && ! grep -q '^組む:' $out; do sleep 1; t=$((t+1)); done
    kill $pid 2>/dev/null; wait $pid 2>/dev/null
    rm -rf "/tmp/enchudb-ball-lazy-t7-${pid:?}"
    echo "{\"v\":\"$v\",\"r\":$r,\"load\":$load,\"t\":$t,\"line\":$(grep -m1 '^組む:' $out | python3 -I -c 'import json,sys; print(json.dumps(sys.stdin.read().strip()))')}"
  done
done
