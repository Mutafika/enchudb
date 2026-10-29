#!/usr/bin/env python3
"""15 回目 (P1): 14 回目と 15 回目を交互に回した `l` の比べ。 python3 -I sum_l15.py results_ballp_l15_64.jsonl
書き手の 10 ms を越えた書き込み (`slow` = [始まり, 長さ, 空き作りで畳んだ, 区画を畳んだ, 区画を移った, 組む窓に掛かる]) を、 始まりが組む窓
(b0 = 書き始めて 300 ms、 b1 = b0 + build_ns) の中か外かで分ける (14 回目の sum_l13.py の 「組む間」 は窓に掛かった止まりを全部入れるので、
組む前から続いた止まりも組む間に数えていた)。"""
import json, statistics as st, sys
from collections import defaultdict

B0 = 300e6
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
by = defaultdict(list)
for d in rows:
    by[(d["wal_mib"], d["log_mib"], d["v"])].append(d)
print(f"{sys.argv[1]} (負荷 {min(d['load'] for d in rows)}〜{max(d['load'] for d in rows)})")
for k in sorted(by):
    ds = by[k]
    ins, outs, pre = [], [], []
    for d in ds:
        b1 = B0 + d["build_ns"]
        s_in = [s[1] for s in d["slow"] if B0 <= s[0] <= b1]
        s_pre = [s[1] for s in d["slow"] if s[0] < B0 and s[0] + s[1] >= B0]
        s_out = [s[1] for s in d["slow"] if not (B0 <= s[0] <= b1)]
        ins.append(max(s_in, default=0)); pre.append(max(s_pre, default=0)); outs.append(max(s_out, default=0))
    f = lambda xs: f"{st.median(xs)/1e6:.1f} ({min(xs)/1e6:.1f}〜{max(xs)/1e6:.1f})"
    print(f"  WAL {k[0]} 記録 {k[1]} {k[2]} ({len(ds)} 回): 組む {f([d['build_ns'] for d in ds])} ms / 窓の中で始まった止まりの最大 {f(ins)} ms"
          f" (10 ms 以下は 0) / 組む前から窓に続いた止まり {f(pre)} ms / 窓の外で始まった止まりの最大 {f(outs)} ms / 公開中 {st.median(d['publish_ns'] for d in ds)/1e3:.0f} µs")
