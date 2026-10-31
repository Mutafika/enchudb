#!/bin/sh
# #667: 0.37.1 (base) と直しの段 (A = 捨てる時に crc を取らない / B = A + 新しい entry の crc を並べる / DmC = B + 区画を移ったら起こす /
# D = DmC + tick ごとに写す) を seed ごとに順を回して。 記録 64 MiB、 全力、 4 秒。 1 回ごとに 1 分の負荷が 12 を切るのを待つ (上限 30 分)。
SP=/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/71308e5d-c18e-4313-bf9c-4a796bf6373b/scratchpad
OUT=${OUT:-$SP/ab667v.jsonl}
: > $OUT
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
quiet() { t=0; while [ "$(load1 | awk '{print ($1 < 12) ? 1 : 0}')" != 1 ]; do sleep 5; t=$((t + 5)); [ $t -ge 1800 ] && break; done; }
bin() {
  case $1 in
    base) echo $SP/foldstall_663/target/release/foldstall_663 ;;
    *) echo $SP/fs667$1/target/release/foldstall$1 ;;
  esac
}
for seed in 1 2 3; do
  case $seed in
    1) order="base A B DmC D" ;;
    2) order="D DmC B A base" ;;
    3) order="B D base DmC A" ;;
  esac
  for wal in 64 256; do
    for v in $order; do
      quiet; l=$(load1)
      line=$(FOLD_DIR=$SP/fold256 FOLD_WAL_MIB=$wal FOLD_LOG_MIB=64 FOLD_RATE=0 $(bin $v) log 4 $seed)
      echo "{\"ver\":\"$v\",\"load\":$l,${line#\{}" >> $OUT
      echo "$v wal=$wal seed=$seed load=$l"
    done
  done
done
