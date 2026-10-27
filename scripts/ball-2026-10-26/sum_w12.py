#!/usr/bin/env python3
"""12 回目の書き込み (P2): 11 回目 (v11) と 12 回目 (v12) を交互に回した run_ballb.sh の結果を比べる。
usage: python3 -I sum_w12.py results_ballb_v12.jsonl [results_ballb_wal_v12.jsonl]
段ごと・書き手の本数ごとに、 seed × cycle の中央値 (ns / 1 回) と v12 − v11、 組 (seed) ごとの差の幅。"""
import json, statistics as st, sys
from collections import defaultdict

for path in sys.argv[1:]:
    rows = [json.loads(l) for l in open(path) if l.strip()]
    bad = {(r["v"], r["threads"], r["seed"]) for r in rows if r["load"] > 8 or r["free_g"] < 2}
    ns = defaultdict(list)
    per = defaultdict(list)
    for r in rows:
        if r["kind"] == "ok" or (r["v"], r["threads"], r["seed"]) in bad:
            continue
        ns[(r["v"], r["threads"], r["kind"])].append(r["ns"])
        per[(r["v"], r["threads"], r["kind"], r["seed"])].append(r["ns"])
    print(f"{path} (外した回 {len(bad)})")
    for th in (1, 4):
        print(f"  書き手 {th} 本")
        for s in ["w_ins", "w_upd", "w_untie", "w_retie"]:
            line = []
            for mode in ("on", "off"):
                a, b = ns.get((f"v11_{mode}", th, s)), ns.get((f"v12_{mode}", th, s))
                if not a or not b:
                    continue
                ma, mb = st.median(a), st.median(b)
                seeds = sorted({k[3] for k in per if k[0] == f"v12_{mode}" and k[1] == th and k[2] == s})
                d = [st.median(per[(f"v12_{mode}", th, s, x)]) - st.median(per[(f"v11_{mode}", th, s, x)]) for x in seeds if (f"v11_{mode}", th, s, x) in per]
                line.append(f"{mode}: v11 {ma:.1f} / v12 {mb:.1f} ({mb - ma:+.1f}、 組ごと {min(d):+.1f}〜{max(d):+.1f})")
            print(f"    {s:<8} " + "   ".join(line))
