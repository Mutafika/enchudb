#!/bin/sh
# 球 (proto/ball-history) の書き込みの値段: master cb827e4 (base、 base2 = 同じ binary をもう 1 回) / 試作の宣言なし (off) /
# 試作で a・b・w を球にした (on) の 4 つを、 書き手 1 / 4 本 × seed 41〜46 で交互に回す (96 万行、 cycle 3)。
# 見る前に決めた基準 (回す前に書いた):
# (A1) 全部の回に ok の行がある (段ごとの件数の照合が通った)
# (A2) 宣言なしの値段: off / base が全部の段で ≤ max(1.10, base2 / base + 0.05) (seed × cycle の中央値)
# (A3) 球の値段: on / base と on - base (ns) を段ごとに出す (合否なし、 位置の印の +40 ns/列 と並べてユーザに見せる)
# (2 回目) lclk_on = 時刻だけ書き手ごとの数え口にした測り専用の版 (時刻の順は壊れる)。 共有の時刻の値段 = on - lclk_on
# (A4) 回の始まりの空きが 2 GiB 未満、 負荷 > 8 の回は判定から外す (印を付けて残す)
# (9 回目) BALL_WAL=1 を付けて回すと WAL 付き (bench の BALL_WAL)。 基準は BALL_PLAN.md の 9 回目
S=$(cd "$(dirname "$0")" && pwd)
RES=${RES:-$S/results_ballb.jsonl}
SEEDS=${SEEDS:-"41 42 43 44 45 46"}
: > "$RES"
export BALL_DIR=$S/balldb
mkdir -p "$BALL_DIR"
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
freeg() { df -g / | awk 'NR==2 {print $4}'; }
quiet() { t=0; while [ "$(load1 | awk '{print ($1 < 8) ? 1 : 0}')" != 1 ]; do sleep 5; t=$((t + 5)); [ $t -ge 1800 ] && break; done; }
for seed in $SEEDS; do
  for th in 1 4; do
    for v in ${VERSIONS:-base proto_off base2 proto_on}; do
      case $v in
        base|base2) B=$S/target-ballb-base/release/ballb; export BALL=0 ;;
        proto_off) B=$S/target-ballb-proto/release/ballb; export BALL=0 ;;
        proto_on) B=$S/target-ballb-proto/release/ballb; export BALL=1 ;;
        lclk_on) B=$S/target-ballb-lclk/release/ballb; export BALL=1 ;;
        v4_on) B=$S/ballb-proto-v4.bin; export BALL=1 ;;
        v5_on) B=$S/ballb-proto-v5.bin; export BALL=1 ;;
        v6_on) B=$S/ballb-proto-v6.bin; export BALL=1 ;;
        v7b_on) B=$S/ballb-proto-v7b.bin; export BALL=1 ;;
        v8w_on) B=$S/ballb-proto-v8w.bin; export BALL=1 ;;
        v*_on) B=$S/ballb-proto-${v%_on}.bin; export BALL=1 ;;
      esac
      quiet; l=$(load1); f=$(freeg); t0=$(date +%s)
      "$B" w $th $seed | sed "s/^{/{\"v\":\"$v\",\"load\":$l,\"free_g\":$f,/" >> "$RES"
      echo "$v th=$th seed=$seed load=$l free=${f}G $(( $(date +%s) - t0 ))s"
    done
  done
done
