#!/bin/sh
# 20 回目 (T2): v20 の 4 本 + v19 の 3 本 + 18 回目と同じ球の変異 7 組 43 本を 20 回目の code で。 BALL_S は mut20_tests.sh と同じ
: "${BALL_S:?}"
cd /Users/kubo/Desktop/mutafika/enchudb-ball || exit 2
T=/Users/kubo/Desktop/mutafika/enchudb-ball/scripts/ball-2026-10-26/mut20_tests.sh
for f in ${SETS:-ball_mut_v20 ball_mut_v19 ball_mut_v18 ball_mut_v14a ball_mut_v14b ball_mut_v14c ball_mut_v16 ball_mut_v16b ball_mut_v16c}; do
  python3 -I scripts/mutate.py --timeout 1200 --build-timeout 1200 scripts/ball-2026-10-26/$f.json -- sh -c "$T a && $T b" > scripts/ball-2026-10-26/mut_v20t2_$f.log 2>&1
  echo "$f done $(date +%H:%M)"; tail -4 scripts/ball-2026-10-26/mut_v20t2_$f.log
done
git status --short | head -5
