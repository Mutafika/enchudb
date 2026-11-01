import json, re, sys
# ab_t7.sh の行を: 版、 負荷、 止めるまでの秒、 組む間の書き込み、 組む時間、 読み直し、 控えで当てた数、 段 (prof_ns: 組を取る / WAL を読む / 控え)
for l in open(sys.argv[1]):
    r = json.loads(l)
    s = r["line"]
    g = lambda k: (m := re.search(k + r": (\d+)", s)) and int(m.group(1))
    during = (m := re.search(r"組む間の書き込み (\d+)", s)) and int(m.group(1))
    prof = (m := re.search(r"prof_ns: \[([\d, ]+)\]", s)) and [int(x) / 1e9 for x in m.group(1).split(",")]
    if not s:
        print(f'{r["v"]:4} r{r["r"]} load {r["load"]:5} t {r["t"]:3}s  組む: 行なし (止めた)')
        continue
    print(f'{r["v"]:4} r{r["r"]} load {r["load"]:5} t {r["t"]:3}s  組む {g("total_ns")/1e9:6.2f}s 書き込み {during/1e4:7.1f}万 読み直し {g("refolds")} 控え {g("side_ops")/1e4:7.1f}万 '
          f'段 組 {prof[2]:.2f} WAL {prof[3]:.2f} 控え {prof[5]:.2f}')
