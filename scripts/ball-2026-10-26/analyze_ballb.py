"""run_ballb.sh の結果を (A1)〜(A4) で読む。 usage: python3 -I analyze_ballb.py results_ballb.jsonl"""
import json
import statistics
import sys
from collections import defaultdict

rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
# (A4) 外す回: (v, threads, seed) ごとに始まりの負荷 / 空き
bad = {(r["v"], r["threads"], r["seed"]) for r in rows if r["load"] > 8 or r["free_g"] < 2}
ok = {(r["v"], r["threads"], r["seed"]) for r in rows if r["kind"] == "ok"}
runs = {(r["v"], r["threads"], r["seed"]) for r in rows}
print(f"回の数 {len(runs)}、 ok の無い回 {sorted(runs - ok)}  (A1)")
print(f"外した回 (負荷 > 8 / 空き < 2 GiB) {sorted(bad)}  (A4)")
ns = defaultdict(list)
for r in rows:
    if r["kind"] == "ok" or (r["v"], r["threads"], r["seed"]) in bad:
        continue
    ns[(r["v"], r["threads"], r["kind"])].append(r["ns"])
med = {k: statistics.median(v) for k, v in ns.items()}
stages = ["w_ins", "w_upd", "w_untie", "w_retie"]
fail = []
for th in (1, 4):
    print(f"\n書き手 {th} 本 (ns / 1 回、 中央値、 seed × cycle)")
    print(f"{'段':<8} {'base':>8} {'base2':>8} {'off':>8} {'on':>8} {'off/base':>9} {'線':>6} {'on/base':>8} {'on-base':>8} {'lclk':>8} {'on-lclk':>8}")
    for s in stages:
        b, b2, off, on = (med.get((v, th, s)) for v in ("base", "base2", "proto_off", "proto_on"))
        if None in (b, b2, off, on):
            continue
        line = max(1.10, b2 / b + 0.05)
        if off / b > line:
            fail.append((th, s, off / b, line))
        lc = med.get(("lclk_on", th, s))
        extra = f" {lc:8.1f} {on - lc:8.1f}" if lc is not None else ""
        print(f"{s:<8} {b:8.1f} {b2:8.1f} {off:8.1f} {on:8.1f} {off / b:9.3f} {line:6.3f} {on / b:8.3f} {on - b:8.1f}{extra}")
print(f"\n(A2) 宣言なしの値段が線を越えた段: {fail if fail else 'なし'}")
okr = [r for r in rows if r["kind"] == "ok" and r["v"] == "proto_on"]
for th in (1, 4):
    x = [r for r in okr if r["threads"] == th]
    if x:
        print(f"球の面 (書き手 {th} 本): faces {statistics.median(r['faces'] for r in x):.0f}、 取った byte {statistics.median(r['ball_bytes'] for r in x) / 2**20:.1f} MiB")
for v in ("base", "proto_off", "proto_on", "lclk_on"):
    x = [r["rss_peak_kb"] for r in rows if r["kind"] == "ok" and r["v"] == v]
    if x:
        print(f"RSS の峰 {v}: {statistics.median(x) / 1024:.0f} MiB")
