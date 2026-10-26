"""並行の変異の検出率 (9 回目): 変異ごとに release で組み、 schema の並行の試験 (名前に concurrent を含む 3 本、 WAL 付きを含む) を
N 回回して落ちた回数を数える。 対照 (変異なし) も。 usage: python3 rate9.py ball_mut_v9.json N "名前1" "名前2" ..."""
import json, subprocess, sys
ROOT = "/Users/kubo/Desktop/mutafika/enchudb-ball"
N = int(sys.argv[2])
names = sys.argv[3:]
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
    assert orig.count(m["find"]) == 1, n
    try:
        open(p, "w").write(orig.replace(m["find"], m["replace"]))
        run(n)
    finally:
        open(p, "w").write(orig)
