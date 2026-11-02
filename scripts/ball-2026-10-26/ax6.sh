#!/bin/sh
# 18 回目: 球の schema の試験 3 本を 6 回 (確率で落ちる変異の検出率)。 落ちた回を数え、 1 回でも落ちたら fail
cd /Users/kubo/Desktop/mutafika/enchudb-ball || exit 2
S=/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/71308e5d-c18e-4313-bf9c-4a796bf6373b/scratchpad
export CARGO_TARGET_DIR=$S/tgt-ball16
L=$S/r18/muth/$(date +%s).log
cargo test -p enchudb-schema --test ball_lazy --test ball_reopen --test ball_history --no-run >> "$L" 2>&1 || exit 3
f=0
for i in 1 2 3 4 5 6; do
  cargo test --no-fail-fast -p enchudb-schema --test ball_lazy --test ball_reopen --test ball_history >> "$L" 2>&1 || f=$((f+1))
done
echo "落ちた $f/6" | tee -a "$L"
[ $f = 0 ]
