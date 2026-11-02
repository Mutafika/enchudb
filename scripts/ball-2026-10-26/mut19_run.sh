#!/bin/sh
# 19 回目 (T2): 18 回目と同じ球の変異 7 組 43 本を 19 回目の code で (v19 の 3 本は先に回した = mut_v19_ball_mut_v19.log)。 BALL_S は mut19_tests.sh と同じ
: "${BALL_S:?}"
cd /Users/kubo/Desktop/mutafika/enchudb-ball || exit 2
T=/Users/kubo/Desktop/mutafika/enchudb-ball/scripts/ball-2026-10-26/mut19_tests.sh
for f in ball_mut_v18 ball_mut_v14a ball_mut_v14b ball_mut_v14c ball_mut_v16 ball_mut_v16b ball_mut_v16c; do
  python3 -I scripts/mutate.py --timeout 1200 --build-timeout 1200 scripts/ball-2026-10-26/$f.json -- sh -c "$T a && $T b" > scripts/ball-2026-10-26/mut_v19t2_$f.log 2>&1
  echo "$f done $(date +%H:%M)"; tail -4 scripts/ball-2026-10-26/mut_v19t2_$f.log
done
git status --short | head -5
