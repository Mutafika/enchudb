import json, sys, statistics as st
rows = [json.loads(l) for l in open(sys.argv[1])]
for size in [16, 64]:
    for v in ['v16', 'v16c']:
        rs = [r for r in rows if r['v'] == v and r['log_mib'] == size]
        if not rs: continue
        inb = [max([s[1] for s in r['slow'] if s[5] == 1], default=0) / 1e6 for r in rs]
        outb = [max([s[1] for s in r['slow'] if s[5] == 0], default=0) / 1e6 for r in rs]
        allm = [max(a, b) for a, b in zip(inb, outb)]
        build = [r['build_ns'] / 1e6 for r in rs]
        pub = [r['publish_ns'] / 1e3 for r in rs]
        q = lambda xs, p: sorted(xs)[min(len(xs) - 1, int(len(xs) * p))]
        print(f'log{size} {v:5} n={len(rs)}: 組む間の最大の止まり 中央値 {st.median(inb):.1f} / 75% {q(inb,.75):.1f} / 最大 {max(inb):.1f} ms | 外 中央値 {st.median(outb):.1f} 最大 {max(outb):.1f} | 全体 中央値 {st.median(allm):.1f} | 組む {st.median(build):.0f} ms | 公開中 最大 {max(pub):.0f} µs')
        print(f'   組む間 per seed {[round(x) for x in inb]}')
