#!/bin/sh
# 列ごとの球の値段: base / a・b だけ球 (ab) / w だけ球 (w)、 書き手 1 / 4 本 × seed 41〜43 (96 万行、 cycle 3)
S=$(cd "$(dirname "$0")" && pwd)
RES=${RES:-$S/results_ballb_cols.jsonl}
: > "$RES"
export BALL_DIR=$S/balldb
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
quiet() { t=0; while [ "$(load1 | awk '{print ($1 < 8) ? 1 : 0}')" != 1 ]; do sleep 5; t=$((t + 5)); [ $t -ge 1800 ] && break; done; }
for seed in 41 42 43; do for th in 1 4; do for v in ${VERSIONS:-base ab w}; do
  case $v in
    base) B=$S/target-ballb-base/release/ballb; export BALL=0 ;;
    ab) B=$S/target-ballb-proto/release/ballb; export BALL=1 BALL_COLS=t.a,t.b ;;
    w) B=$S/target-ballb-proto/release/ballb; export BALL=1 BALL_COLS=t.w ;;
    v4ab) B=$S/ballb-proto-v4.bin; export BALL=1 BALL_COLS=t.a,t.b ;;
    v4w) B=$S/ballb-proto-v4.bin; export BALL=1 BALL_COLS=t.w ;;
    v5ab) B=$S/ballb-proto-v5.bin; export BALL=1 BALL_COLS=t.a,t.b ;;
    v5w) B=$S/ballb-proto-v5.bin; export BALL=1 BALL_COLS=t.w ;;
    v6ab) B=$S/ballb-proto-v6.bin; export BALL=1 BALL_COLS=t.a,t.b ;;
    v6w) B=$S/ballb-proto-v6.bin; export BALL=1 BALL_COLS=t.w ;;
    v7bab) B=$S/ballb-proto-v7b.bin; export BALL=1 BALL_COLS=t.a,t.b ;;
    v7bw) B=$S/ballb-proto-v7b.bin; export BALL=1 BALL_COLS=t.w ;;
  esac
  quiet; l=$(load1)
  "$B" w $th $seed | sed "s/^{/{\"v\":\"$v\",\"load\":$l,/" >> "$RES"
done; done; done
