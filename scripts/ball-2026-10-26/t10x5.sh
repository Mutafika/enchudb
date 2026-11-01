#!/bin/sh
# 18 回目: T10 を 5 回 (変異の検出率を見る)。 落ちた数を出し、 1 回でも落ちたら fail
cd /Users/kubo/Desktop/mutafika/enchudb-ball || exit 2
S=/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/71308e5d-c18e-4313-bf9c-4a796bf6373b/scratchpad
export CARGO_TARGET_DIR=$S/tgt-ball16
L=$S/r18/mut18b/$(date +%s).log
cargo test -p enchudb-schema --test ball_lazy --no-run >> "$L" 2>&1 || exit 3
f=0
for i in 1 2 3 4 5; do
  cargo test -p enchudb-schema --test ball_lazy -- --exact a_build_does_not_continue_from_the_head_after_a_fold >> "$L" 2>&1 || f=$((f+1))
done
echo "T10 落ちた $f/5" | tee -a "$L"
[ $f = 0 ]
