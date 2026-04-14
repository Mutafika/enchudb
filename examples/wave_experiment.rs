/// 量子円柱 v26 実験 — 波形ベース交差
///
/// 断面を波形（振幅）として表現し、掛け算で交差が出るか確認する。

fn main() {
    let n = 8; // entity 0..7

    // age=30 の entity: {1, 3, 5}
    let age30: Vec<f64> = (0..n)
        .map(|i| if [1, 3, 5].contains(&i) { 1.0 } else { 0.0 })
        .collect();

    // dept=5 の entity: {3, 5, 6}
    let dept5: Vec<f64> = (0..n)
        .map(|i| if [3, 5, 6].contains(&i) { 1.0 } else { 0.0 })
        .collect();

    println!("=== 時間領域（ビットマップ相当）===");
    println!("age=30:  {:?}", age30);
    println!("dept=5:  {:?}", dept5);

    // 時間領域での掛け算 = 要素ごとの積
    let product: Vec<f64> = age30.iter().zip(&dept5).map(|(a, b)| a * b).collect();
    println!("積:      {:?}", product);
    println!(
        "交差:    {:?}",
        product
            .iter()
            .enumerate()
            .filter(|(_, v)| **v > 0.0)
            .map(|(i, _)| i)
            .collect::<Vec<_>>()
    );

    println!();
    println!("=== 周波数領域（DFT）===");

    // DFT
    let age30_freq = dft(&age30);
    let dept5_freq = dft(&dept5);

    println!("age=30 DFT: {:?}", fmt_complex(&age30_freq));
    println!("dept=5 DFT: {:?}", fmt_complex(&dept5_freq));

    // 周波数領域での掛け算（要素ごとの複素数積）
    let freq_product: Vec<(f64, f64)> = age30_freq
        .iter()
        .zip(&dept5_freq)
        .map(|((ar, ai), (br, bi))| (ar * br - ai * bi, ar * bi + ai * br))
        .collect();

    println!("積 DFT:     {:?}", fmt_complex(&freq_product));

    // 逆DFTで時間領域に戻す
    let result = idft(&freq_product);
    println!("IDFT結果:   {:?}", fmt_f64(&result));

    println!();
    println!("=== 比較 ===");
    println!("時間領域の積:   {:?}", fmt_f64(&product));
    println!("周波数経由の積: {:?}", fmt_f64(&result));

    // 一致確認
    let match_time: Vec<usize> = product
        .iter()
        .enumerate()
        .filter(|(_, v)| **v > 0.5)
        .map(|(i, _)| i)
        .collect();
    let match_freq: Vec<usize> = result
        .iter()
        .enumerate()
        .filter(|(_, v)| **v > 0.5)
        .map(|(i, _)| i)
        .collect();
    println!();
    println!("時間領域での交差: {:?}", match_time);
    println!("周波数経由の交差: {:?}", match_freq);
    println!("一致: {}", match_time == match_freq);

    println!();
    println!("=== 逆フーリエ保持 ===");
    // ビットマップを IDFT した状態で保持しておく
    // 積を取って DFT で戻す
    let age30_inv = idft_complex(&to_complex(&age30));
    let dept5_inv = idft_complex(&to_complex(&dept5));
    println!("age=30 IDFT: {:?}", fmt_complex(&age30_inv));
    println!("dept=5 IDFT: {:?}", fmt_complex(&dept5_inv));

    // 逆フーリエ領域での積
    let inv_product: Vec<(f64, f64)> = age30_inv
        .iter()
        .zip(&dept5_inv)
        .map(|((ar, ai), (br, bi))| (ar * br - ai * bi, ar * bi + ai * br))
        .collect();
    println!("積:          {:?}", fmt_complex(&inv_product));

    // DFT で時間領域に戻す
    let inv_result = dft_to_real(&inv_product);
    println!("DFT結果:     {:?}", fmt_f64(&inv_result));
    let inv_match: Vec<usize> = inv_result
        .iter()
        .enumerate()
        .filter(|(_, v)| **v > 0.5)
        .map(|(i, _)| i)
        .collect();
    println!("交差:        {:?}", inv_match);
    println!("正解:        {:?}", match_time);

    println!();
    println!("=== 畳み込み vs 積 ===");
    // 周波数領域の積 → 時間領域の畳み込みになるはず
    // 逆に、時間領域の積 → 周波数領域の畳み込み
    // 今回欲しいのは時間領域の積（要素ごとの AND）
    // → 周波数領域で畳み込みして IDFT すれば時間領域の積が得られる
    let freq_conv = circular_conv_freq(&age30_freq, &dept5_freq, n);
    let conv_result = idft(&freq_conv);
    println!("畳み込み経由: {:?}", fmt_f64(&conv_result));
}

/// DFT: 時間領域 → 周波数領域
fn dft(x: &[f64]) -> Vec<(f64, f64)> {
    let n = x.len();
    (0..n)
        .map(|k| {
            let (mut re, mut im) = (0.0, 0.0);
            for (i, xi) in x.iter().enumerate() {
                let angle = -2.0 * std::f64::consts::PI * k as f64 * i as f64 / n as f64;
                re += xi * angle.cos();
                im += xi * angle.sin();
            }
            (re, im)
        })
        .collect()
}

/// IDFT: 周波数領域 → 時間領域
fn idft(x: &[(f64, f64)]) -> Vec<f64> {
    let n = x.len();
    (0..n)
        .map(|i| {
            let mut re = 0.0;
            for (k, (xr, xi)) in x.iter().enumerate() {
                let angle = 2.0 * std::f64::consts::PI * k as f64 * i as f64 / n as f64;
                re += xr * angle.cos() - xi * angle.sin();
            }
            re / n as f64
        })
        .collect()
}

/// 周波数領域での畳み込み（= 時間領域の積を求めるため）
/// 時間領域の積 = IDFT(DFT(a) 畳み込み DFT(b)) ではなく
/// 時間領域の積の DFT = DFT(a) * DFT(b) ではない（これは畳み込み）
///
/// 正しくは:
/// 時間領域の畳み込み ↔ 周波数領域の積
/// 時間領域の積 ↔ 周波数領域の畳み込み / N
fn circular_conv_freq(a: &[(f64, f64)], b: &[(f64, f64)], n: usize) -> Vec<(f64, f64)> {
    // 周波数領域での circular convolution
    let nf = n as f64;
    (0..n)
        .map(|k| {
            let (mut re, mut im) = (0.0, 0.0);
            for m in 0..n {
                let idx = (k + n - m) % n;
                let (ar, ai) = a[m];
                let (br, bi) = b[idx];
                re += ar * br - ai * bi;
                im += ar * bi + ai * br;
            }
            (re / nf, im / nf)
        })
        .collect()
}

fn to_complex(x: &[f64]) -> Vec<(f64, f64)> {
    x.iter().map(|v| (*v, 0.0)).collect()
}

/// IDFT: 複素数入力 → 複素数出力
fn idft_complex(x: &[(f64, f64)]) -> Vec<(f64, f64)> {
    let n = x.len();
    (0..n)
        .map(|i| {
            let (mut re, mut im) = (0.0, 0.0);
            for (k, (xr, xi)) in x.iter().enumerate() {
                let angle = 2.0 * std::f64::consts::PI * k as f64 * i as f64 / n as f64;
                re += xr * angle.cos() - xi * angle.sin();
                im += xr * angle.sin() + xi * angle.cos();
            }
            (re / n as f64, im / n as f64)
        })
        .collect()
}

/// DFT: 複素数入力 → 実数出力
fn dft_to_real(x: &[(f64, f64)]) -> Vec<f64> {
    let n = x.len();
    (0..n)
        .map(|k| {
            let mut re = 0.0;
            for (i, (xr, xi)) in x.iter().enumerate() {
                let angle = -2.0 * std::f64::consts::PI * k as f64 * i as f64 / n as f64;
                re += xr * angle.cos() - xi * angle.sin();
            }
            re
        })
        .collect()
}

fn fmt_f64(v: &[f64]) -> Vec<String> {
    v.iter().map(|x| format!("{:.2}", x)).collect()
}

fn fmt_complex(v: &[(f64, f64)]) -> Vec<String> {
    v.iter()
        .map(|(r, i)| format!("{:.2}{:+.2}i", r, i))
        .collect()
}
