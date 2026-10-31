"""16 回目 (W): ballb w の結果 (run_w14.sh、 VS = "<前>:0 <前>:1 <後>:0 <後>:1") から、 版ごとの球の分 (同じ seed の球あり − なし、
回の平均) の中央値と、 書き換えの回ごとの伸び (6 回目 / 1 回目) を出す。 usage: python3 sum_w16.py <results.jsonl> <前> <後>"""
import json, statistics as st, sys
rows = [json.loads(l) for l in open(sys.argv[1])]
vers = sys.argv[2:4]
oks = [r for r in rows if r['kind'] == 'ok']
rows = [r for r in rows if 'cycle' in r]
d = {(r['v'], r['kind'], r['seed'], r['cycle']): r['ns'] for r in rows}
seeds = sorted({r['seed'] for r in rows})
def mean_cycles(v, kind, seed):
    xs = [d[(v, kind, seed, c)] for c in range(0, 10) if (v, kind, seed, c) in d]
    return sum(xs) / len(xs)
print('load', min(r['load'] for r in rows), '-', max(r['load'] for r in rows))
for kind in ['w_ins', 'w_upd', 'w_untie', 'w_retie']:
    for ver in vers:
        offs = [mean_cycles(ver + '_off', kind, s) for s in seeds]
        ons = [mean_cycles(ver + '_on', kind, s) for s in seeds]
        diff = [a - b for a, b in zip(ons, offs)]
        print(f'{kind:8} {ver:7}: off {st.median(offs):6.1f}  on {st.median(ons):6.1f}  球の分 中央値 {st.median(diff):+6.1f} ns  seed ごと {[round(x) for x in diff]}')
for ver in [v + s for v in vers for s in ['_on', '_off']]:
    cyc = sorted({c for (v, k, s, c) in d if v == ver and k == 'w_upd'})
    g = [d[(ver, 'w_upd', s, cyc[-1])] / d[(ver, 'w_upd', s, cyc[0])] for s in seeds]
    per = [round(st.median([d[(ver, 'w_upd', s, c)] for s in seeds])) for c in cyc]
    print(f'{ver:11} 書き換え 回ごと {per}  最後 / 最初 中央値 {st.median(g):.3f}')
for ver in [v + '_on' for v in vers]:
    o = [r for r in oks if r['v'] == ver]
    print(ver, '球 MiB', round(st.median(r['ball_bytes'] for r in o) / 2**20), ' RSS の峰 MiB', round(st.median(r['rss_peak_kb'] for r in o) / 1024))
