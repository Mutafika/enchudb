"""16 回目 (W): ballb rw (大きい値の列の時点の読み) の版ごとの中央値。 usage: python3 sum_rw16.py <results.jsonl>"""
import json, statistics as st, sys
rows = [json.loads(l) for l in open(sys.argv[1])]
vers = sorted({r['v'] for r in rows})
print('load', min(r['load'] for r in rows), '-', max(r['load'] for r in rows))
for k in ['pull_now', 'pull_at_now', 'pull_at_t5', 'pull_at_t1']:
    for v in vers:
        xs = [r['ns'] for r in rows if r['v'] == v and r['kind'] == k]
        print(f'{k:12} {v:7}: 中央値 {st.median(xs):7.1f} ns  {[round(x) for x in xs]}')
for v in vers:
    o = [r for r in rows if r['v'] == v and r['kind'] == 'ok']
    print(v, '球 MiB', round(st.median(r['ball_bytes'] for r in o) / 2**20), ' RSS の峰 MiB', round(st.median(r['rss_peak_kb'] for r in o) / 1024))
