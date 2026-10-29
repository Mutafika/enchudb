import json, sys, collections
rows = [json.loads(l) for l in open(sys.argv[1])]
g = collections.defaultdict(list)
for r in rows:
    g[(r['rate'], r['mode'], r['ver'])].append(r)
def rng(xs, f="{:.1f}"):
    lo, hi = min(xs), max(xs)
    return f.format(lo) if f.format(lo) == f.format(hi) else f"{f.format(lo)}〜{f.format(hi)}"
print("| 速さ | 形 | 版 | 書けた数 (万) | p99 µs | 最大 ms | 10 ms 超え | 止まりの計 ms | 外れた listener | 負荷 |")
print("|---|---|---|---|---|---|---|---|---|---|")
for rate in (0, 500000):
    for mode in ("none", "log", "loglisten", "listen"):
        for ver in ("base", "fix"):
            rs = g.get((rate, mode, ver))
            if not rs: continue
            print(f"| {'全力' if rate == 0 else f'{rate//10000} 万/秒'} | {mode} | {'0.37.0' if ver == 'base' else '直し'} | "
                  f"{rng([r['n']/1e4 for r in rs], '{:.0f}')} | {rng([r['p99_ns']/1e3 for r in rs], '{:.2f}')} | {rng([r['max_ms'] for r in rs])} | "
                  f"{rng([r['slow'] for r in rs], '{:.0f}')} | {rng([r['slow_ms'] for r in rs], '{:.0f}')} | {rng([r['detached'] for r in rs], '{:.0f}')} | {rng([r['load'] for r in rs])} |")
