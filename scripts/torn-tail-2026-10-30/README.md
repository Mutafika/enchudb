# #669 の変異 (2026-10-30)

`mutations.json` を `scripts/mutate.py` で回す (試験は `mutate_tests.sh` = oplog の単体 + 電源断の模擬の 2 本)。 結果は `mutate.log`。
path は測った時の worktree の場所なので、 回す時は書き換える。

- 落ちる 4 本: 止まった区画の head を戻さない / 読めた所でなく閉じた group の終わりまで戻す (再生した閉じていない record を捨てる) /
  古い区画で止まっても新しい区画を畳まない (#665) / header の head を書かない
- 残る 2 本 (守り):
  - checkpoint を戻さない: 再生は checkpoint から読むので、 止まった所は checkpoint より前にならない。 今の区画の checkpoint の欄が
    head より先を指す像 (header の page と record の page が別の時に届いた形) の守り
  - 戻した後に書き出さない: 戻した header が届く前に電源が落ちても、 次に開いた時に同じ所から読んで同じ所で止まり、 また戻す
