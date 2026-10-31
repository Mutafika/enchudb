import re, sys, collections
# 書き換えの closure (ROOT を名前に含む frame) の下だけ、 関数ごとの inclusive を数える (同じ関数が入れ子に出ても 1 回)
ROOT = sys.argv[2]
KEYS = sys.argv[3].split(',')
rows = []
for l in open(sys.argv[1]):
    l2 = re.sub(r'\s+\(in [^)]*\).*$', '', l.rstrip('\n'))
    m = re.match(r'^([\s+!:|]*)(\d+) (\S.*)$', l2)
    if m: rows.append((len(m.group(1)), int(m.group(2)), m.group(3)))
tot = collections.Counter(); base = 0
stack = []  # (depth, set of keys active, in_root)
for depth, n, name in rows:
    while stack and stack[-1][0] >= depth: stack.pop()
    inroot = (stack[-1][2] if stack else False)
    active = set(stack[-1][1]) if stack else set()
    if ROOT in name and not inroot:
        base += n; inroot = True
        stack.append((depth, active, True)); continue
    if inroot:
        for k in KEYS:
            if k in name and k not in active:
                tot[k] += n; active = active | {k}
    stack.append((depth, active, inroot))
print('書き換えの closure の下の sample', base)
for k in KEYS:
    print(f'  {k:26} {tot[k]:6} {100*tot[k]/max(base,1):5.1f}%')
