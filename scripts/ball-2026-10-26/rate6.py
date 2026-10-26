"""並行の変異の検出率: 変異ごとに release で組み、 schema の並行の試験 2 本を N 回回して落ちた回数を数える。 対照 (変異なし) も。"""
import json, subprocess, sys
ROOT = "/Users/kubo/Desktop/mutafika/enchudb-ball"
N = int(sys.argv[2]) if len(sys.argv) > 2 else 20
names = ["名簿の鍵の外で帳面に書く (書き換えの速い道)", "名簿の鍵の外で帳面に書く (外す速い道)", "sparse の道で新しい値の鍵を取らない"]
muts = {m["name"]: m for m in json.load(open(sys.argv[1]))}
cmd = ["cargo", "test", "--release", "-q", "-p", "enchudb-schema", "--test", "ball_history", "--", "concurrent"]
def run(label):
    b = subprocess.run(cmd[:-2] + ["--no-run"], cwd=ROOT, capture_output=True, text=True)
    if b.returncode != 0:
        print(label, "BUILD FAILED", b.stderr[-2000:]); return
    fails = 0
    for i in range(N):
        try:
            r = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=120)
            fails += r.returncode != 0
        except subprocess.TimeoutExpired:
            fails += 1
    print(f"{label}: {fails}/{N}", flush=True)
run("対照 (変異なし)")
for n in names:
    m = muts[n]; p = f"{ROOT}/{m['file']}"
    orig = open(p).read()
    assert orig.count(m["find"]) == 1
    try:
        open(p, "w").write(orig.replace(m["find"], m["replace"]))
        run(n)
    finally:
        open(p, "w").write(orig)
