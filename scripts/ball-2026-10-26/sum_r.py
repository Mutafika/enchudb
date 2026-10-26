import json,statistics,sys
from collections import defaultdict
rows=[json.loads(l) for l in open(sys.argv[1]) if l.strip()]
vs=sys.argv[2].split(",")
d=defaultdict(list)
for r in rows:
    if r["kind"]=="ok": d[(r["v"],"rss_mb")].append(r["rss_peak_kb"]/1024); d[(r["v"],"bytes_mb")].append(r["ball_bytes"]/2**20)
    else: d[(r["v"],r["kind"])].append(r["ns"])
print("loads", sorted({r["load"] for r in rows}))
kinds=[r["kind"] for r in rows if r["kind"]!="ok"]
for k in list(dict.fromkeys(kinds))+["rss_mb","bytes_mb"]:
    line=f"{k:12}"
    for v in vs:
        a=d.get((v,k))
        line+=f"  {v} {statistics.median(a):9.1f} {[round(x,1) for x in a]}" if a else f"  {v} -"
    if all(d.get((v,k)) for v in vs): line+=f"  ratio {statistics.median(d[(vs[-1],k)])/statistics.median(d[(vs[0],k)]):.2f}"
    print(line)
print("ok rows", {v: sum(1 for r in rows if r['kind']=='ok' and r['v']==v) for v in vs})
