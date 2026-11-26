#!/usr/bin/env bash
# スピン 案 W の試作の測り: v0.38.0 (worktree) と proto/spin-w を交互に 5 run。
#   BASE=/path/to/v0.38.0-worktree scripts/spin-w/bench.sh [runs]
# 宣言の無い DB の数 (base) は両方、 宣言した列の数 (hist) は proto だけ。 x86 Linux の数として読む。
set -euo pipefail
here=$(cd "$(dirname "$0")/../.." && pwd)
base=${BASE:-/home/user/base}
runs=${1:-5}
work=${WORK:-/tmp/spin_w_bench_work}
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-/home/user/target}
cp "$here/crates/enchudb-engine/examples/spin_w_bench.rs" "$base/crates/enchudb-engine/examples/spin_w_bench.rs"
(cd "$base" && cargo build -q --release -p enchudb-engine --example spin_w_bench)
cp "$CARGO_TARGET_DIR/release/examples/spin_w_bench" "$work.base.bin" 2>/dev/null || { mkdir -p "$(dirname "$work")"; cp "$CARGO_TARGET_DIR/release/examples/spin_w_bench" "$work.base.bin"; }
(cd "$here" && cargo build -q --release -p enchudb-engine --features spin-w-bench --example spin_w_bench)
cp "$CARGO_TARGET_DIR/release/examples/spin_w_bench" "$work.proto.bin"
for i in $(seq 1 "$runs"); do
  echo "run $i v0.38.0 $("$work.base.bin" base "$work.db")"
  echo "run $i proto   $("$work.proto.bin" base "$work.db")"
done
for i in $(seq 1 "$runs"); do
  echo "run $i proto   $("$work.proto.bin" hist "$work.db")"
done
