import json, sys, statistics as st, collections
# results_direct_ab.jsonl を回ごと・形ごとに: 書けた数の比の中央値、 最大、 10 ms 超えの計
rows = [json.loads(l) for l in open(sys.argv[1])]
by = collections.defaultdict(dict)
for r in rows:
    by[(r["round"], r["mode"], r["seed"])][r["v"]] = r
for rnd in sorted({k[0] for k in by}):
    for m in ["log", "none"]:
        ps = [(d["base"], d["new"]) for (rr, mm, s), d in by.items() if rr == rnd and mm == m and "base" in d and "new" in d]
        if not ps:
            continue
        ratio = st.median(n["n"] / b["n"] for b, n in ps)
        loads = [x["load"] for p in ps for x in p]
        print(f"回 {rnd} {m}: {len(ps)} seed、 負荷 {min(loads)}〜{max(loads)}、 比の中央値 {ratio:.3f}、 "
              f"最大 {min(b['max_ms'] for b, _ in ps)}〜{max(b['max_ms'] for b, _ in ps)} → "
              f"{min(n['max_ms'] for _, n in ps)}〜{max(n['max_ms'] for _, n in ps)} ms、 "
              f"10 ms 超えの計 {sum(b['slow_ms'] for b, _ in ps)} → {sum(n['slow_ms'] for _, n in ps)} ms")
