#!/usr/bin/env python3
"""13 回目 (W1) の組ごとの差: python3 -I sum_w13_paired.py <jsonl>...
seed ごとに (版の cycle の中央値 − v11 の cycle の中央値) を取り、 その中央値と、 v11 より遅かった seed の数 / seed の数、
並べ替えで作った 95% の幅 (seed の差を 2000 回選び直した中央値の 2.5〜97.5%)。"""
import json, random, statistics as st, sys
from collections import defaultdict

rows = [json.loads(l) for p in sys.argv[1:] for l in open(p) if l.strip()]
per = defaultdict(list)
for r in rows:
    if r["kind"] != "ok":
        per[(r["v"], r["kind"], r["seed"])].append(r["ns"])
vs = sorted({r["v"] for r in rows} - {"v11"})
rng = random.Random(1)
for s in ["w_ins", "w_upd", "w_untie", "w_retie"]:
    out = []
    for v in vs:
        seeds = sorted({k[2] for k in per if k[0] == v and k[1] == s and ("v11", s, k[2]) in per})
        d = [st.median(per[(v, s, x)]) - st.median(per[("v11", s, x)]) for x in seeds]
        boot = sorted(st.median(rng.choices(d, k=len(d))) for _ in range(2000))
        out.append(f"{v} {st.median(d):+.1f} [{boot[50]:+.1f}, {boot[1949]:+.1f}] 遅い {sum(x > 0 for x in d)}/{len(d)}")
    print(f"{s:<8} " + "  ".join(out))
