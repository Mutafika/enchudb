#!/usr/bin/env python3
"""13 回目 (P1): 12 回目と 13 回目を交互に回した `l` の比べ。 python3 -I sum_l13.py results_ballp_l13ab.jsonl
記録の大きさごとに、 組む時間 / 公開中 / 組む間の書き込みの最大と p99 の中央値と範囲、 組む時間 / 記録の record。"""
import json, statistics as st, sys
from collections import defaultdict

rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
by = defaultdict(list)
for d in rows:
    by[(d["wal_mib"], d["log_mib"], d["v"])].append(d)
print(f"{sys.argv[1]} (負荷 {min(d['load'] for d in rows)}〜{max(d['load'] for d in rows)})")
f = lambda xs, k=1e6, u="ms": f"{st.median(xs)/k:.1f} {u} ({min(xs)/k:.1f}〜{max(xs)/k:.1f})"
for (w, l, v) in sorted(by):
    ds = by[(w, l, v)]
    print(f"  WAL {w} MiB 記録 {l} MiB {v} ({len(ds)} 回): 組む {f([d['build_ns'] for d in ds])} / 公開中 {f([d['publish_ns'] for d in ds], 1e3, 'µs')}"
          f" / 組む間の書き込み 最大 {f([d['in_max_ns'] for d in ds])} p99 {f([d['in_p99_ns'] for d in ds], 1e3, 'µs')}"
          f" / 1 record {st.median(d['build_ns'] / d['records'] for d in ds):.0f} ns")
    over = [d['in_max_ns'] - d['out_max_ns'] for d in ds]
    print(f"      組む間の最大 − 組む外の最大: {', '.join(f'{x/1e6:+.1f}' for x in over)} ms")
