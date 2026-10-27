#!/usr/bin/env python3
"""12 回目の測りの要約: sum_l.py results_ballp_l_v12.jsonl [results_ballp_o_v12.jsonl]"""
import json, statistics as st, sys
from collections import defaultdict

rows = [json.loads(l) for l in open(sys.argv[1])]
by = defaultdict(list)
for d in rows:
    by[(d["wal_mib"], d["log_mib"])].append(d)
print("書き手 1 本が書き続ける中で最初の読みが組む (seed ごと)")
for k in sorted(by):
    print(f"  WAL {k[0]} MiB 記録 {k[1]} MiB")
    for d in by[k]:
        pp = d.get("publish_parts_ns", [0, 0, 0, 0])
        print(
            f"    seed {d['seed']} load {d['load']}: record {d['records']} 件 / 組む {d['build_ns']/1e6:.0f} ms "
            f"(記録 {d['log_ns']/1e6:.0f} / 追いつき {d['catch_ns']/1e6:.0f} ms、 控え {d['side_ops']}) "
            f"公開中 {d['publish_ns']/1e3:.0f} µs (書き終え {pp[0]/1e3:.0f} / 記録 {pp[1]/1e3:.0f} / lock の下 {pp[2]/1e3:.0f} / WAL {pp[3]/1e3:.0f})"
        )
        print(
            f"      書き込み 1 回: 組む間 n={d['in_n']} p99 {d['in_p99_ns']/1e3:.1f} µs 最大 {d['in_max_ns']/1e6:.2f} ms / "
            f"外 n={d['out_n']} p99 {d['out_p99_ns']/1e3:.1f} µs 最大 {d['out_max_ns']/1e6:.2f} ms"
        )
    ns = [d["build_ns"] / max(d["records"], 1) for d in by[k]]
    print(f"    組む時間 / 記録の record: 中央値 {st.median(ns):.0f} ns")
if len(sys.argv) > 2:
    for l in open(sys.argv[2]):
        d = json.loads(l)
        print(f"開く ({d['log_mib']} MiB、 交互 {d['k']} 回の中央値): 土台あり {d['open_base_med_ns']/1e6:.2f} ms / なし {d['open_nobase_med_ns']/1e6:.2f} ms")
