import json, re, sys
# ab_t7_1718.sh / t7_solo.sh の行: session ごとの (組む時間 / 組む間の書き込み / 読み直し / 続いた最大 / 持って読んだ / 重なり / 控え)
for l in open(sys.argv[1]):
    r = json.loads(l)
    ss = []
    for s in r["lines"]:
        g = lambda k: (m := re.search(k + r": (\d+)", s)) and int(m.group(1))
        d = int(re.search(r"組む間の書き込み (\d+)", s).group(1))
        ss.append(f'{g("total_ns") / 1e9:.1f}s/{d / 1e4:.0f}万/読直{g("refolds")}/続{g("refold_run_max")}/持{g("held_reads")}/重{g("dups")}/捨{g("side_ops") / 1e4:.0f}万')
    print(r["v"], "r%d" % r["r"], "load", r["load"], "t", r["t"], r["res"], " | ".join(ss))
