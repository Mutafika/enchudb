#!/usr/bin/env python3
"""11 回目の測りの要約: sum_p.py results_ballp_r_v11.jsonl results_ballp_w_v11.jsonl"""
import json, statistics as st, sys
from collections import defaultdict

r = [json.loads(l) for l in open(sys.argv[1])]
by = defaultdict(list)
for d in r:
    by[d["log_mib"]].append(d)
print("組み直し (seed ごとの中央値)")
for m in sorted(by):
    ds = by[m]
    med = lambda k: st.median(d[k] for d in ds)
    print(f"  記録 {m} MiB: record {med('records'):.0f} 件、 宣言 {med('declare_ns')/1e6:.1f} ms = {med('ns_per_record'):.0f} ns/件 "
          f"(読む+当てる {med('log_ns')/1e6:.1f} ms のうち当てる {med('apply_ns')/1e6:.1f} ms、 今と比べる {med('check_ns')/1e6:.2f} ms)、 "
          f"最初の宣言 {med('declare_first_ns')/1e6:.1f} ms、 fixed {max(d['fixed'] for d in ds)}、 控え 地平以降 {med('checks_after_horizon'):.0f}/{ds[0]['checks']}")
    print(f"    開く: 土台あり 1 回目 {med('open_base_ns')/1e6:.2f} ms / 2 回目 {med('open_base2_ns')/1e6:.2f} ms / 土台なし {med('open_nobase_ns')/1e6:.2f} ms "
          f"(組ごとの差 2 回目 − なし: {', '.join('%+.2f' % ((d['open_base2_ns'] - d['open_nobase_ns'])/1e6) for d in ds)} ms)")
w = [json.loads(l) for l in open(sys.argv[2])]
print("書き込み (書き手 1 本、 400k 回の書き換え)")
for m in sorted({d["log_mib"] for d in w}):
    rows = []
    for seed in sorted({d["seed"] for d in w}):
        b = [d for d in w if d["log_mib"] == m and d["seed"] == seed and d["v"] == "base"]
        n = [d for d in w if d["log_mib"] == m and d["seed"] == seed and d["v"] == "nobase"]
        if b and n:
            rows.append((seed, b[0]["ns_per_write"], n[0]["ns_per_write"]))
    bm, nm = st.median(x[1] for x in rows), st.median(x[2] for x in rows)
    ratios = [x[1] / x[2] for x in rows]
    print(f"  記録 {m} MiB: 土台あり {bm:.0f} ns / なし {nm:.0f} ns = {bm/nm:.3f} (組ごとの比 {', '.join('%.3f' % r for r in ratios)}、 中央値 {st.median(ratios):.3f})")
