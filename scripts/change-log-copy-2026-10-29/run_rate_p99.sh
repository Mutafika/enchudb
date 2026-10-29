#!/bin/sh
SP=/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/71308e5d-c18e-4313-bf9c-4a796bf6373b/scratchpad
: > $SP/ab667p.jsonl
for seed in 1 2 3 4; do
  case $seed in 1) o="base B DmC D";; 2) o="D DmC B base";; 3) o="B base D DmC";; 4) o="DmC D base B";; esac
  for v in $o; do
    if [ $v = base ]; then B=$SP/foldstall_663/target/release/foldstall_663; else B=$SP/fs667$v/target/release/foldstall$v; fi
    l=$(sysctl -n vm.loadavg | awk '{print $2}')
    line=$(FOLD_DIR=$SP/fold256 FOLD_RATE=500000 $B log 4 $seed)
    echo "{\"ver\":\"$v\",\"load\":$l,${line#\{}" >> $SP/ab667p.jsonl
  done
done
