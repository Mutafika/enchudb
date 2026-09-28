#!/usr/bin/env python3
"""変異試験: ソースに 1 か所ずつ変異を入れてテストを回し、 落ちる (= 変異を検出する) かを数える。

    python3 scripts/mutate.py mutations.json -- cargo test --release -q -p enchudb-schema --test live_reach

mutations.json は変異の並び:

    [{"name": "外した row の子を探し直さない",
      "file": "crates/enchudb-schema/src/lib.rs",
      "find": "for y in self.kids(x) {", "replace": "for y in Vec::<u32>::new() {"}]

`find` は file の中にちょうど 1 回だけ在ること (無い / 2 回以上は、 変異を入れずにその変異を飛ばす)。

1 回ごとに時間の上限を置く。 返ってこない変異 (無限ループ) で test のプロセスが置き去りになって、 CPU を使い続けた
ことがある (2026-09-26 に起動した run が 44 時間回っていた)。

- 上限は、 変異を入れない run (baseline) の所要時間の 3 倍 + 30 秒 (`--timeout` で上書き)。 build は先に `--no-run`
  で済ませ、 上限は test の実行だけに掛ける
- 時間切れはプロセスグループごと SIGKILL する (cargo の子の test binary まで)。 時間切れは 「検出した (killed)」 と数える
- この script 自体が落ちても (SIGKILL でも) 孤児が残らないように、 test は見張り付きのシェルの中で走らせる: 見張りは
  同じプロセスグループで上限 + 5 秒眠り、 起きたらグループごと kill する
- 変異を入れた file は、 run が終わるたび / 例外 / SIGINT・SIGTERM・SIGHUP で必ず元に戻す。 SIGKILL では戻せないので、
  元の中身を `target/mutate-backup/` にも置き、 次に起動した時に最初に戻す (その間に file が編集されていたら戻さずに止まる)
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time

# 変異を入れた file の元の中身 (戻すため)
_ORIGINAL: dict[str, str] = {}
# SIGKILL で戻せなかった時の控え (次の起動で戻す)
BACKUP_DIR = os.path.join("target", "mutate-backup")


def backup_path(path: str) -> str:
    return os.path.join(BACKUP_DIR, path.replace(os.sep, "__"))


def keep_original(path: str, text: str, mutated: str) -> None:
    """元の中身を控える。 控えには変異を入れた後の中身も置く (次の起動で、 まだ変異のままかを確かめるため)。"""
    _ORIGINAL[path] = text
    os.makedirs(BACKUP_DIR, exist_ok=True)
    with open(backup_path(path) + ".tmp", "w") as f:
        json.dump({"path": path, "original": text, "mutated": mutated}, f)
    os.replace(backup_path(path) + ".tmp", backup_path(path))


def restore_leftovers() -> bool:
    """前の起動が SIGKILL で死んで戻せなかった file を戻す。

    戻すのは、 今の中身が変異を入れたままの時だけ。 その後に誰かが編集していたら (共有 checkout では他の session も
    同じ file を触る) 上書きせず、 控えを残して False を返す (旧: 無条件に上書きし、 その編集を黙って消した)。"""
    if not os.path.isdir(BACKUP_DIR):
        return True
    ok = True
    for name in os.listdir(BACKUP_DIR):
        if name.endswith(".tmp"):
            continue
        bak = os.path.join(BACKUP_DIR, name)
        b = json.load(open(bak))
        path = b["path"]
        now = open(path).read() if os.path.exists(path) else None
        if now == b["mutated"]:
            with open(path, "w") as f:
                f.write(b["original"])
            print(f"前の run が戻せなかった {path} を元に戻した", file=sys.stderr)
        elif now != b["original"]:
            print(f"{path} は前の run の後に変わっている — 戻さない。 確かめて {bak} を消してから起動し直す",
                  file=sys.stderr)
            ok = False
            continue
        os.remove(bak)
    return ok
# 走っている test のプロセスグループ
_RUNNING: list[int] = []


def restore() -> None:
    for path, text in list(_ORIGINAL.items()):
        with open(path, "w") as f:
            f.write(text)
        del _ORIGINAL[path]
        try:
            os.remove(backup_path(path))
        except FileNotFoundError:
            pass


def kill_group(pgid: int) -> None:
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def on_signal(signum, _frame) -> None:
    for pgid in _RUNNING:
        kill_group(pgid)
    restore()
    sys.exit(128 + signum)


def run(cmd: list[str], timeout: float) -> tuple[str, float]:
    """cmd を自分のプロセスグループで走らせる。 戻りは ("pass" | "fail" | "timeout", 秒)。"""
    # 見張り: 同じグループで上限 + 5 秒眠ってグループごと kill する (この script が死んでも効く)。
    # `exec` で cmd がシェルを置き換えるので、 グループの長 = cmd。
    quoted = " ".join("'" + a.replace("'", "'\\''") + "'" for a in cmd)
    shell = f"(sleep {int(timeout) + 5}; kill -KILL -$$) & exec {quoted}"
    start = time.monotonic()
    p = subprocess.Popen(["/bin/sh", "-c", shell], start_new_session=True,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    _RUNNING.append(p.pid)
    try:
        rc = p.wait(timeout=timeout)
        outcome = "pass" if rc == 0 else "fail"
    except subprocess.TimeoutExpired:
        outcome = "timeout"
    finally:
        # 見張りの sleep と、 test が残した子も片付ける
        kill_group(p.pid)
        p.wait()
        _RUNNING.remove(p.pid)
    return outcome, time.monotonic() - start


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mutations", help="変異の JSON")
    ap.add_argument("--timeout", type=float, help="1 run の上限 (秒)。 既定は baseline の 3 倍 + 30 秒")
    ap.add_argument("--build-timeout", type=float, default=1800, help="build の上限 (秒)")
    # test のコマンドは最初の `--` で自分で切り分ける (argparse の REMAINDER は mutations の後ろの `--timeout` まで
    # コマンドに飲み込み、 `--timeout` を実行して baseline が fail した)。 コマンド側の `--` (test binary への引数) は残る
    argv = sys.argv[1:]
    if "--" not in argv:
        ap.error("-- の後に test のコマンドを置く")
    sep = argv.index("--")
    a, cmd = ap.parse_args(argv[:sep]), argv[sep + 1:]
    if not cmd:
        ap.error("-- の後に test のコマンドを置く")
    muts = json.load(open(a.mutations))
    if not restore_leftovers():
        return 2
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, on_signal)

    is_cargo_test = cmd[:2] == ["cargo", "test"]
    # test binary への引数 (`--` の後) より前に --no-run を置く
    cut = cmd.index("--") if "--" in cmd else len(cmd)
    build = cmd[:cut] + ["--no-run"] + cmd[cut:] if is_cargo_test else None

    def build_ok() -> bool:
        return build is None or run(build, a.build_timeout)[0] == "pass"

    # baseline: 変異なしで通ること + 上限の元
    if not build_ok():
        print("baseline: build が通らない", file=sys.stderr)
        return 2
    outcome, secs = run(cmd, a.timeout or 3600)
    if outcome != "pass":
        print(f"baseline: 変異なしで {outcome} ({secs:.0f} 秒) — 先に直す", file=sys.stderr)
        return 2
    timeout = a.timeout or secs * 3 + 30
    print(f"baseline: pass {secs:.0f} 秒 → 1 run の上限 {timeout:.0f} 秒")

    results = []
    try:
        for m in muts:
            name, path = m.get("name", m["find"][:40]), m["file"]
            text = open(path).read()
            n = text.count(m["find"])
            if n != 1:
                print(f"  skip   {name}: find が {n} 回")
                results.append((name, "skip"))
                continue
            mutated = text.replace(m["find"], m["replace"])
            keep_original(path, text, mutated)
            with open(path, "w") as f:
                f.write(mutated)
            try:
                if not build_ok():
                    verdict = "build-error"
                else:
                    outcome, secs = run(cmd, timeout)
                    verdict = {"pass": "SURVIVED", "fail": "killed", "timeout": "killed(timeout)"}[outcome]
                    verdict += f" {secs:.0f}s"
            finally:
                restore()
            print(f"  {verdict:<22} {name}")
            results.append((name, verdict))
    finally:
        restore()

    survived = [n for n, v in results if v.startswith("SURVIVED")]
    killed = [n for n, v in results if v.startswith("killed")]
    print(f"killed {len(killed)} / survived {len(survived)} / その他 {len(results) - len(killed) - len(survived)}")
    for n in survived:
        print(f"  生き残り: {n}")
    return 1 if survived else 0


if __name__ == "__main__":
    sys.exit(main())
