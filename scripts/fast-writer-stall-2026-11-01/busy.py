import sys, collections
# consumer の 4 秒の中身: 段ごとの合計 (ms) と 1 件あたり (ns)
tags=['scan','encode','lappend','crc','lwrite','lflush','fsync','body','reset_held','pump_tick','room_mcbf','room','fold_loop']
print(f"{'file':18}" + ''.join(f"{t:>10}" for t in tags) + f"{'recs':>10}")
for p in sys.argv[1:]:
    agg=collections.Counter(); recs=0
    for l in open(p):
        s,k,t,d,x=l.split()
        if k!='c': continue
        agg[t]+=int(d)
        if t=='lappend': recs+=int(x)
    print(f"{p.split('/')[-1][:-4]:18}" + ''.join(f"{agg[t]/1000:10.0f}" for t in tags) + f"{recs:10}")
    print(f"{'  ns/件':18}" + ''.join(f"{agg[t]*1000/max(recs,1):10.1f}" for t in tags))
