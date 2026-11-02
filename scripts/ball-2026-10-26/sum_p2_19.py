import json, sys
# 19 回目 (P2): results_ballp_p2_19.jsonl を WAL・記録ごとに。 held = 4 回目を持って読ませた組み、 none = 球なし (同じ書き手)
rows = [json.loads(l) for l in open(sys.argv[1])]
ms = lambda ns: round(ns / 1e6, 1)
for wal in sorted({r["wal_mib"] for r in rows}):
    for log in sorted({r["log_mib"] for r in rows}):
        h = [r for r in rows if r["v"] == "held" and r["wal_mib"] == wal and r["log_mib"] == log]
        n = [r for r in rows if r["v"] == "none" and r["wal_mib"] == wal and r["log_mib"] == log]
        if not h:
            continue
        fill = [ms(r["w_ns"] / max(r["switches"], 1)) for r in h + n]
        print(f"WAL {wal} MiB 記録 {log} MiB (held {len(h)} / none {len(n)}、 負荷 {min(r['load'] for r in h + n)}〜{max(r['load'] for r in h + n)})")
        print(f"  区画が埋まるまで (ms、 書き手の時間 / 区画を移った回数): {sorted(fill)}")
        print(f"  持った回 {[r['held_reads'] for r in h]}、 持った長さの最大 (ms) {[ms(r['held_max_ns']) for r in h]}")
        print(f"  held 組む間の書き手の最大の止まり (ms) {[ms(r['in_max_ns']) for r in h]}、 外 {[ms(r['out_max_ns']) for r in h]}")
        print(f"  none 書き手の最大の止まり (ms) {[ms(r['out_max_ns']) for r in n]}")
        slow_h = [s for r in h for s in r["slow"] if s[5] == 1]
        print(f"  held の組む間の 10 ms 超え (長さ ms, 空け作り, 畳み, 移った) {[(ms(s[1]), s[2], s[3], s[4]) for s in slow_h]}")
