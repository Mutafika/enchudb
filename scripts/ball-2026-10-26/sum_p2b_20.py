"""20 回目 (P2b) のまとめ: 版 × 記録ごとに、 持った長さの最大 / その内訳 (組を取る・記録を読む・WAL を読む) / 持ってから読んだ記録の entry / 組む間の書き手の最大の止まり。"""
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1])]
ms = lambda ns: round(ns / 1e6, 1)
for log in (16, 64):
    for v in sorted({r["v"] for r in rows}):
        rs = sorted((r for r in rows if r["v"] == v and r["log_mib"] == log), key=lambda r: r["seed"])
        held = [r["held_max_ns"] for r in rs]
        print(f"記録 {log} MiB {v}: 持った長さ {ms(min(held))}〜{ms(max(held))} ms、 記録を読む {ms(min(r['held_parts_ns'][1] for r in rs))}〜{ms(max(r['held_parts_ns'][1] for r in rs))} ms、"
              f" WAL を読む {ms(min(r['held_parts_ns'][2] for r in rs))}〜{ms(max(r['held_parts_ns'][2] for r in rs))} ms、 持ってから読んだ entry {min(r['held_parts_n'][0] for r in rs)}〜{max(r['held_parts_n'][0] for r in rs)}、"
              f" 書き手の最大の止まり {round(min(r['in_max_ns'] for r in rs)/1e6, 2)}〜{round(max(r['in_max_ns'] for r in rs)/1e6, 2)} ms、 持つ回 {sum(r['held_reads'] for r in rs)} / {len(rs)} run")
