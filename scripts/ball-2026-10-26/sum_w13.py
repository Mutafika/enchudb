#!/usr/bin/env python3
"""13 回目 (W1) の要約: python3 -I sum_w13.py results_ballb_w13.jsonl
段ごと・書き手の本数ごとに、 版ごとの seed × cycle の中央値 (ns / 1 回)、 v11 との差、 組 (seed) ごとの差の幅。"""
import json, statistics as st, sys
from collections import defaultdict

rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
bad = {(r["v"], r["threads"], r["seed"]) for r in rows if r["load"] > 8 or r["free_g"] < 2}
ns, per = defaultdict(list), defaultdict(list)
for r in rows:
    if r["kind"] == "ok" or (r["v"], r["threads"], r["seed"]) in bad:
        continue
    ns[(r["v"], r["threads"], r["kind"])].append(r["ns"])
    per[(r["v"], r["threads"], r["kind"], r["seed"])].append(r["ns"])
vs = sorted({r["v"] for r in rows}, key=lambda v: (v != "v11", v))
print(f"{sys.argv[1]} (外した回 {len(bad)}、 負荷 {min(r['load'] for r in rows)}〜{max(r['load'] for r in rows)})")
for th in sorted({r["threads"] for r in rows}):
    print(f"  書き手 {th} 本")
    for s in ["w_ins", "w_upd", "w_untie", "w_retie"]:
        base = ns.get(("v11", th, s))
        if not base:
            continue
        mb = st.median(base)
        out = [f"v11 {mb:.1f}"]
        for v in vs[1:]:
            xs = ns.get((v, th, s))
            if not xs:
                continue
            seeds = sorted({k[3] for k in per if k[0] == v and k[1] == th and k[2] == s})
            d = [st.median(per[(v, th, s, x)]) - st.median(per[("v11", th, s, x)]) for x in seeds if ("v11", th, s, x) in per]
            out.append(f"{v} {st.median(xs) - mb:+.1f} ({min(d):+.1f}〜{max(d):+.1f})")
        print(f"    {s:<8} " + "  ".join(out))
