import json, re, sys
# ab_t7full.sh の行: 版、 負荷、 時間、 終わり方、 session ごとの (組む時間 / 組む間の書き込み / 回 / 読み直し)
for l in open(sys.argv[1]):
    r = json.loads(l)
    ss = []
    for s in r["lines"]:
        g = lambda k: int(re.search(k + r": (\d+)", s).group(1))
        d = int(re.search(r"組む間の書き込み (\d+)", s).group(1))
        n = int(re.search(r"、 回 (\d+)", s).group(1))
        ss.append(f'{g("total_ns") / 1e9:.1f}s/{d / 1e4:.0f}万/回{n}/読直{g("refolds")}')
    print(r["v"], "r%d" % r["r"], "load", r["load"], "t", r["t"], r["res"], " | ".join(ss))
