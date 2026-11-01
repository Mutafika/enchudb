import json, sys, statistics as st
# ab_probe.sh の行: 書き手の速さ、 区画を移った / 畳んだ回数、 移ってから畳むまで、 区画の頭から読んだ時間と畳まれた割合
rows = [json.loads(l) for l in open(sys.argv[1])]
for r in rows:
    p = r["p"]
    if p is None:
        print(r["v"], r["r"], "null")
        continue
    o, n = p["old"], p["new"]
    print(f'{r["v"]:4} r{r["r"]} load {r["load"]:5}  書き手 {p["writes_per_s"] / 1e4:5.1f}万/s 移り {p["switches"]:3} 畳み {p["folds"]:3} '
          f'遅れ 中央 {p["lag_med_ms"]:6.1f} 最大 {p["lag_max_ms"]:6.1f} ms | 古い区画 畳まれ {o["gone"]}/{o["n"]} {o["ms_med"]:6.1f} ms {o["seen_med"] / 1e4:5.1f}万件 '
          f'(読む前に無い {p["old_missing"]}) | 今の区画 畳まれ {n["gone"]}/{n["n"]} {n["ms_med"]:6.1f} ms')
for v in ["pre", "post"]:
    ps = [r["p"] for r in rows if r["v"] == v and r["p"]]
    m = lambda f: st.median(f(p) for p in ps)
    print(f'{v}: 書き手 {m(lambda p: p["writes_per_s"]) / 1e4:.1f}万/s、 遅れ {m(lambda p: p["lag_med_ms"]):.0f} ms、 '
          f'古い区画を読む {m(lambda p: p["old"]["ms_med"]):.0f} ms で畳まれた {sum(p["old"]["gone"] for p in ps)}/{sum(p["old"]["n"] for p in ps)}、 '
          f'読む前に無い {sum(p["old_missing"] for p in ps)}、 今の区画 {sum(p["new"]["gone"] for p in ps)}/{sum(p["new"]["n"] for p in ps)}')
