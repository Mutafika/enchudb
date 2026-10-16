//! WAL 付きの engine の裏の thread (consumer = 書き出し係) を寝かせて起こす係。
//!
//! 0.33 までは書き込みの queue (`write_queue::WriteQueue`) が持っていた。 0.34 で `*_async` の書き込みも呼んだ thread が
//! その場で書く形になり、 queue を外したので、 起こす係だけをここに残す。

/// #391: consumer thread を寝かせ、 仕事ができた時に起こす (WAL に書いた時、 WAL の空きを待つ書き手が居る時、 閉じる時)。
///
/// consumer は 「寝る」 印を立ててから仕事が無いかをもう一度見て、 無ければ park する。 起こす側は仕事を作った後に印を
/// 見て、 立っていれば unpark する (そのたびのコストは atomic を 1 回読むだけ)。 両側の間に SeqCst の fence を置くので、
/// 「consumer が仕事を見落とす」 と 「起こす側が印を見落とす」 が同時には起きない。 印を見た後の unpark が park より先に
/// 来ても、 park は token で即座に戻る (起こし損ねない)。
///
/// 印は 2 段: 書き出しの tick までの短い眠り (`SHALLOW`、 100 ms 以内に自分で起きる) と、 起こされるまでの深い眠り
/// (`DEEP`)。 WAL への append は書き出しの tick を回すためだけに起こすので、 深い眠りの時だけ起こす
/// (`wake_if_deep`)。 短い眠りの間も毎 append で起こすと、 同期の書き込みのたびに unpark の syscall が走る
/// (#388 の再現で 2 割遅くなった)。
#[derive(Default)]
pub(crate) struct ConsumerWaker {
    state: std::sync::atomic::AtomicU8,
    thread: std::sync::OnceLock<std::thread::Thread>,
}

const AWAKE: u8 = 0;
const SHALLOW: u8 = 1;
const DEEP: u8 = 2;
/// これより長く寝る時は深い眠り (WAL への append でも起こす)。
const SHALLOW_MAX: std::time::Duration = std::time::Duration::from_millis(100);

impl ConsumerWaker {
    /// consumer thread が最初に 1 回呼ぶ。
    pub(crate) fn register_current(&self) {
        let _ = self.thread.set(std::thread::current());
    }

    /// 仕事を作った側が呼ぶ。 consumer が寝ていれば起こす。
    #[inline]
    pub(crate) fn wake(&self) {
        self.wake_from(SHALLOW);
    }

    /// consumer が深く寝ている (起こされるまで寝ている) 時だけ起こす。 WAL への append 用。
    #[inline]
    pub(crate) fn wake_if_deep(&self) {
        self.wake_from(DEEP);
    }

    #[inline]
    fn wake_from(&self, min: u8) {
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
        if self.state.load(std::sync::atomic::Ordering::Relaxed) >= min
            && let Some(t) = self.thread.get()
        {
            t.unpark();
        }
    }

    /// consumer が呼ぶ。 印を立ててから `idle()` で仕事が無いことを確かめ、 無ければ `timeout` まで (None なら起こされる
    /// まで) 寝る。 起こされた / 時間が来た / 仕事があった、 のどれでも戻る (戻った後に仕事を見直すこと)。
    pub(crate) fn sleep(&self, timeout: Option<std::time::Duration>, idle: impl FnOnce() -> bool) {
        use std::sync::atomic::Ordering;
        let depth = if timeout.is_some_and(|d| d <= SHALLOW_MAX) { SHALLOW } else { DEEP };
        self.state.store(depth, Ordering::Relaxed);
        std::sync::atomic::fence(Ordering::SeqCst);
        if idle() {
            match timeout {
                Some(d) => std::thread::park_timeout(d),
                None => std::thread::park(),
            }
        }
        self.state.store(AWAKE, Ordering::Relaxed);
    }
}

