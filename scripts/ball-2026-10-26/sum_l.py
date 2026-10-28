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
            f"公開中 {d['publish_ns']/1e3:.0f} µs (書き終え {pp[0]/1e3:.0f} / 記録 {pp[1]/1e3:.0f} / 写した所 (12 回目は lock の下) {pp[2]/1e3:.0f} / WAL {pp[3]/1e3:.0f})"
        )
        print(
            f"      書き込み 1 回: 組む間 n={d['in_n']} p99 {d['in_p99_ns']/1e3:.1f} µs 最大 {d['in_max_ns']/1e6:.2f} ms / "
            f"外 n={d['out_n']} p99 {d['out_p99_ns']/1e3:.1f} µs 最大 {d['out_max_ns']/1e6:.2f} ms"
        )
        if "prof_ns" in d:
            pn, c = d["prof_ns"], d["prof_n"]
            print(
                f"      追いつき {d['rounds']} 回: 記録を読む {pn[0]/1e6:.0f} ms / 記録を解いて当てる {pn[1]/1e6:.0f} ms (entry {c[0]}、 当てた {c[2]}) / "
                f"写した所 (12 回目は lock の下) {pn[2]/1e6:.0f} ms / WAL を読む {pn[3]/1e6:.0f} ms / WAL を当てる {pn[4]/1e6:.0f} ms (record {c[1]})"
            )
            if "setup_ns" in d:
                sn = d["setup_ns"]
                print(f"      読む前: 円柱 {sn[0]/1e6:.1f} ms / change_emit を待って土台を開く {sn[1]/1e6:.1f} ms / 種を読む {sn[2]/1e6:.1f} ms / 球を作る {sn[3]/1e6:.1f} ms")
            if "log_prof_ns" in d:
                for lab, pn2, c2 in (("最初の記録", d["log_prof_ns"], d["log_prof_n"]), ("追いつき", pn, c)):
                    print(
                        f"      {lab}: 記録を読む {pn2[0]/1e6:.1f} / 解いて当てる {pn2[1]/1e6:.1f} (entry {c2[0]}) / 控えを当てる {pn2[5]/1e6:.1f} "
                        f"(捨てていた {c2[3]} 回) / 控えを落とす {pn2[6]/1e6:.1f} / 写した所 {pn2[2]/1e6:.1f} / WAL {pn2[3]/1e6:.1f} + {pn2[4]/1e6:.1f} ms"
                    )
            if "skipped" in d:
                print(f"      畳んだ後に飛んだ entry {d['skipped']} / 公開中の前に書き出しを待つ {d['prequiesce_ns']/1e6:.1f} ms / 読み直し {d.get('refolds', '-')}")
            if "log_lock_ns" in d:
                ll = d["log_lock_ns"]
                print(f"      記録の lock: 読みが待つ 最初 {ll[0]/1e6:.1f} / 追いつき {ll[1]/1e6:.1f} ms、 足す側が 1 回分持つ 最大 {ll[2]/1e6:.1f} ms、 組む間に持った和 {ll[3]/1e6:.1f} ms")
            per = lambda t, n: t / max(n, 1)
            print(
                f"      1 件: 記録を読む {per(pn[0], c[0]):.0f} ns / 解いて当てる {per(pn[1], c[0]):.0f} ns / WAL を読む {per(pn[3], c[1]):.0f} ns / "
                f"WAL を当てる {per(pn[4], c[1]):.0f} ns"
            )
    ns = [d["build_ns"] / max(d["records"], 1) for d in by[k]]
    print(f"    組む時間 / 記録の record: 中央値 {st.median(ns):.0f} ns")
if len(sys.argv) > 2:
    for l in open(sys.argv[2]):
        d = json.loads(l)
        print(f"開く ({d['log_mib']} MiB、 交互 {d['k']} 回の中央値): 土台あり {d['open_base_med_ns']/1e6:.2f} ms / なし {d['open_nobase_med_ns']/1e6:.2f} ms")
