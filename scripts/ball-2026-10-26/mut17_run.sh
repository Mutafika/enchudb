#!/bin/sh
# 17 回目 (T2): merge の後に球の変異 6 組 39 本 (v14a〜c / v16 / v16b / v16c) を回し直す。 試験は 16 回目と同じ (schema の球 3 本 + engine の記録と球の単体)
cd /Users/kubo/Desktop/mutafika/enchudb-ball || exit 2
T=/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/71308e5d-c18e-4313-bf9c-4a796bf6373b/scratchpad/mut16_tests.sh
for f in ball_mut_v14a ball_mut_v14b ball_mut_v14c ball_mut_v16 ball_mut_v16b ball_mut_v16c; do
  python3 -I scripts/mutate.py --timeout 1200 --build-timeout 1200 scripts/ball-2026-10-26/$f.json -- sh -c "$T a && $T b" > scripts/ball-2026-10-26/mut_v17_$f.log 2>&1
  echo "$f done $(date +%H:%M)"; tail -4 scripts/ball-2026-10-26/mut_v17_$f.log
done
git status --short | head -5
