#!/bin/sh
# #667 の退行の確かめ: WAL ありの書き手 (dbw の enchu-oplog、 WAL 64 MiB、 changefeed なし)。 0.37.1 (base = dbw-new) と直し (dbw-fix)、
# 社員 100 万人、 書き手 1 本と 8 本、 seed 1〜6 を交互に。 1 回ごとに負荷 12 を切るのを待つ。
SP=/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/71308e5d-c18e-4313-bf9c-4a796bf6373b/scratchpad
W=$SP/dbw667/run; mkdir -p $W/db
OUT=$SP/dbw667.jsonl; : > $OUT
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
quiet() { t=0; while [ "$(load1 | awk '{print ($1 < 12) ? 1 : 0}')" != 1 ]; do sleep 5; t=$((t + 5)); [ $t -ge 1800 ] && break; done; }
for T in 1 8; do for s in 1 2 3 4 5 6; do
  if [ $((s % 2)) = 0 ]; then order="base fix"; else order="fix base"; fi
  for v in $order; do
    [ $v = base ] && B=$SP/dbw0371/new/target/release/dbw-new || B=$SP/dbw667/target/release/dbw-fix
    quiet; l=$(load1)
    DBW_DIR=$W/db $B enchu-oplog $T 1000000 $((8000000 / T)) $s 2>/dev/null | sed "s/^{/{\"load1\":$l,\"label\":\"$v\",/" >> $OUT
  done
done; done
