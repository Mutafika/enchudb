import json, sys
# 18 回目 (P1): results_ballp_l18_{64,256}.jsonl を版・記録の大きさごとに。 公開中 / 書き手の最大の止まり (組む間 in・外 out) / 組む時間 / 読み直し / 持って読んだ回
rows = [json.loads(l) for l in open(sys.argv[1])]
for v in ("v17", "v18"):
    for log in sorted({r["log_mib"] for r in rows}):
        rs = [r for r in rows if r["v"] == v and r["log_mib"] == log]
        if not rs:
            continue
        pub = max(r["publish_ns"] for r in rs) / 1e3
        stall = [max(r["in_max_ns"], r["out_max_ns"]) / 1e6 for r in rs]
        build = sorted(r["build_ns"] / 1e6 for r in rs)
        extra = ""
        if "held_reads" in rs[0]:
            extra = f" 読み直し {[r['refolds'] for r in rs]} 続いた最大 {max(r['refold_run_max'] for r in rs)} 持って読んだ {sum(r['held_reads'] for r in rs)}"
        print(f"{v} WAL {rs[0]['wal_mib']} MiB 記録 {log} MiB n={len(rs)} 負荷 {min(r['load'] for r in rs)}〜{max(r['load'] for r in rs)}: 公開中の最大 {pub:.0f} µs、"
              f" 書き手の最大の止まり {['%.1f' % s for s in stall]} ms、 組む時間の中央 {build[len(build) // 2]:.0f} ms{extra}")
