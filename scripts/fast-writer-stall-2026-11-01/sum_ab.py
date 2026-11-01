import json, sys, statistics as st, collections
rows=[json.loads(l) for l in open(sys.argv[1])]
by=collections.defaultdict(dict)
for r in rows: by[(r['mode'],r['seed'])][r['v']]=r
print("| 形 | seed | 負荷 | 書けた数 (万) base → fix | 最大 (ms) | 10 ms 超え (回 / 計 ms) | 空け作り | 区画を移った |")
print("|---|---|---|---|---|---|---|---|")
for (m,s),d in sorted(by.items()):
    if 'base' not in d or 'fix' not in d: continue
    b,f=d['base'],d['fix']
    print(f"| {m} | {s} | {b['load']:.1f}/{f['load']:.1f} | {b['n']/1e4:.0f} → {f['n']/1e4:.0f} | {b['max_ms']} → {f['max_ms']} | {b['slow']}/{b['slow_ms']} → {f['slow']}/{f['slow_ms']} | {b['room_folds']} → {f['room_folds']} | {b['switches']} → {f['switches']} |")
print()
for m in ['none','log','loglisten']:
    ps=[(d['base'],d['fix']) for (mm,s),d in by.items() if mm==m and 'base' in d and 'fix' in d]
    if not ps: continue
    ratio=st.median(f['n']/b['n'] for b,f in ps)
    print(f"{m}: n 比の中央値 {ratio:.3f}  base max {min(b['max_ms'] for b,_ in ps)}〜{max(b['max_ms'] for b,_ in ps)}  fix max {min(f['max_ms'] for _,f in ps)}〜{max(f['max_ms'] for _,f in ps)}  slow 計 {sum(b['slow_ms'] for b,_ in ps)} → {sum(f['slow_ms'] for _,f in ps)}")
