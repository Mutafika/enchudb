# #689: 空け作りを試す試験が consumer の畳みに先を越される (2026-11-02)

`engine::fold_copy_tests` の空け作り (`make_wal_room`) を試す 3 本は、 consumer の他の畳み (区画を移った時 / 100 ms ごとの書き出しの後) が先に
空けると前提が崩れて落ちる。 3 本とも consumer の畳みを切って直した (`set_wal_fold_on_switch(false)` / `set_wal_fold_on_tick(false)`)。

| 試験 | 先を越された時 | 落ち方 |
|---|---|---|
| `copying_into_the_change_log_while_making_wal_room_does_not_stop_writers` | consumer が古い区画を先に畳み、 空け作りが後で今の区画をその場で畳む (作りどおり `append_lock` の下で写す) | 写しの最後の 1 回が false |
| `a_fold_numbers_no_record_not_yet_written_out_while_making_wal_room` | consumer が古い区画を先に畳み、 空け作りは (写しの hook が書き出しを止めるので) 何も畳めない | 20 秒後に 「前提: 空け作りで畳んだ」 |
| `an_open_tail_folded_in_place_reaches_the_listener_*` | consumer が空きを見た後、 周期の書き出しの前に書き手が 1/4 を切って止まり、 周期の畳みが空ける (空け作りが一度も走らない) | 20 秒後に 「前提: 空け作りが割り込み点を通った」 |

## 自然に落ちる数 (`results_parallel.txt`)

test binary (`fold_copy_tests` の 8 本) を並べて回した。 負荷は他の session の build で変わる。

- master 47433de、 12 本並べて 3 回 (負荷 14〜16): 36 run のうち 10 run が落ちた (上の 3 本)
- 直し、 12 本並べて 6 回 (負荷 10〜16): 72 run 全部通った
- 負荷 8〜9 の時は master も 12 本 / 24 本並べて全部通った (比べにならない) → 下の変異で窓を必ず開けて確かめた

## 変異 (`scripts/mutate.py`)

`mut_2.log` (`mutations.json`、 `cargo test --no-fail-fast -p enchudb-engine --lib -- fold_copy_tests`):

- A1 / B1: 直しを外して空け作りの前に 300 ms 眠る (consumer に先に畳ませる) → killed。 A2 / B2: 直しを残して同じく眠る → 通る
- A3: 空け作りが古い区画の写しを lock の外でしない → killed (写しの試験は今も本来の誤りを落とす)
- B3: 空け作りが畳む前に lock の外で書き出していない所まで写す → killed (番号の試験も同じ)
- C3 / C4: その場で畳む時に閉じていない末尾を写さない / 移さない → killed (末尾の試験も同じ)
- C1 (生き残り): 末尾の試験で書き手を 1 件 2 ms 遅くした。 眠りが空きの見直しの前にあるので、 空け作りは走り (前提は通る)、 書き手は
  上限の 20 秒まで書き続けただけ。 窓を開けていないので、 下の変異に置き換えた

`mut_tick_{fix,master}.log` (`mutations_tick.json`、 `tail_tests.sh` が `tick_{fix,master}/` に出力を残す): consumer が空きを見た後、
周期の書き出しの前に 150 ms 眠る。 master では 3 回とも末尾の試験 2 本が 「前提: 空け作りが割り込み点を通った」 で落ち (自然に落ちた時と
同じ文言)、 直しでは 3 回とも通る。

`mut_1.log` は最初の直し (写しの試験だけ) の baseline。 番号の試験が負荷 15 の中で落ちて、 隣の 2 本も同じ形だと分かった。
