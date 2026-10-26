import json,statistics,sys
from collections import defaultdict
def load(p): return [json.loads(l) for l in open(p) if l.strip()]
cols=load(sys.argv[1]); allc=load(sys.argv[2]); old=sys.argv[3]
for name,rows,vs in [("cols",cols,["ab",old+"ab","w",old+"w"]),("3列",allc,["proto_on",old+"_on","proto_off","base2"])]:
    print(name, "runs", len({(r['v'],r['threads'],r['seed']) for r in rows}), "ok", sum(1 for r in rows if r['kind']=='ok'), "loads", min(r['load'] for r in rows), max(r['load'] for r in rows))
    ns=defaultdict(list)
    for r in rows:
        if r["kind"]!="ok": ns[(r["v"],r["threads"],r["kind"])].append(r["ns"])
    med={k:statistics.median(v) for k,v in ns.items()}
    for th in (1,4):
        print(f" 書き手 {th} 本 (base との差 ns): " + " / ".join(vs))
        for s in ["w_ins","w_upd","w_untie","w_retie"]:
            b=med[("base",th,s)]
            print(f"   {s:8} base {b:7.1f}  " + "  ".join(f"{v} {med[(v,th,s)]-b:+7.1f}" for v in vs))
    for v in vs:
        x=[r["rss_peak_kb"]/1024 for r in rows if r["kind"]=="ok" and r["v"]==v]
        if x: print("  RSS", v, round(statistics.median(x)))
