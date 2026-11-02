#!/bin/sh
# 18 回目 (T2): 17 回目に回せなかった球の変異 6 組 39 本 + 18 回目の 4 本 (ball_mut_v18) + 1 本 (ball_mut_v18b)。 試験は 16 回目と同じ (schema の球 3 本 + engine の記録と球の単体)
cd /Users/kubo/Desktop/mutafika/enchudb-ball || exit 2
T=/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/71308e5d-c18e-4313-bf9c-4a796bf6373b/scratchpad/mut16_tests.sh
for f in ball_mut_v18 ball_mut_v18b ball_mut_v14a ball_mut_v14b ball_mut_v14c ball_mut_v16 ball_mut_v16b ball_mut_v16c; do
  python3 -I scripts/mutate.py --timeout 1200 --build-timeout 1200 scripts/ball-2026-10-26/$f.json -- sh -c "$T a && $T b" > scripts/ball-2026-10-26/mut_v18_$f.log 2>&1
  echo "$f done $(date +%H:%M)"; tail -4 scripts/ball-2026-10-26/mut_v18_$f.log
done
git status --short | head -5
