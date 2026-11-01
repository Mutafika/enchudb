import sys, collections
# 行: start_us kind tag dur_us x
ev=[l.split() for l in open(sys.argv[1])]
ev=[(int(s),k,t,int(d),int(x)) for s,k,t,d,x in ev]
agg=collections.defaultdict(list)
for s,k,t,d,x in ev: agg[(k,t)].append((d,x))
print(f"{'kind tag':22} {'n':>5} {'sum ms':>8} {'p50 us':>8} {'max us':>8} {'x sum':>10}")
for (k,t),v in sorted(agg.items(), key=lambda kv:-sum(d for d,_ in kv[1])):
    ds=sorted(d for d,_ in v)
    print(f"{k+' '+t:22} {len(v):5} {sum(ds)/1000:8.1f} {ds[len(ds)//2]:8} {ds[-1]:8} {sum(x for _,x in v):10}")
