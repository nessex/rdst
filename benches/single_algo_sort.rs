mod bench_utils;

use bench_utils::NumericTest;
use bench_utils::bench_single;
use criterion::{Criterion, criterion_group, criterion_main};
use rdst::RadixSort;
use rdst::tuner::Algorithm;
use rdst::tuner::{Tuner, TuningParams};
use std::hint::black_box;

#[derive(Copy, Clone)]
struct SingleAlgoTuner(Algorithm);

impl<'a> Tuner for SingleAlgoTuner {
    fn pick_algorithm(&self, p: &TuningParams, _counts: &[usize]) -> Algorithm {
        if p.level == p.total_levels - 1 {
            self.0.clone()
        } else {
            // Lsb is used for all other levels as
            // 1. MSB-first algorithms will be recursively tested for each radix, which massively skews the result
            // 2. LSB-first algorithms will handle all levels regardless
            // Lsb is the most stable performance-wise.
            Algorithm::Lsb
        }
    }
}

fn single_algo_sort_set<T>(c: &mut Criterion, suffix: &str, algo: Algorithm, shift: T, count: usize)
where
    T: NumericTest<T>,
{
    let tuner = SingleAlgoTuner(algo);
    let tests: Vec<(&str, Box<dyn Fn(Vec<T>)>)> = vec![
        (
            "rdst",
            Box::new(move |mut input| {
                input.radix_sort_builder().with_tuner(&tuner).sort();
                black_box(input);
            }),
        ),
        (
            "rdst_single_threaded",
            Box::new(move |mut input| {
                input
                    .radix_sort_builder()
                    .with_tuner(&tuner)
                    .with_parallel(false)
                    .sort();

                black_box(input);
            }),
        ),
    ];

    bench_single(
        c,
        &("single_algo_sort_".to_owned() + suffix),
        tests,
        shift,
        count,
    );
}

fn single_algo_sort(c: &mut Criterion) {
    let algorithms = [
        Algorithm::Comparative,
        Algorithm::LrLsb,
        Algorithm::Lsb,
        Algorithm::MtLsb,
        Algorithm::MtOop,
        Algorithm::Recombinating,
        Algorithm::Regions,
        Algorithm::Scanning,
        Algorithm::Ska,
    ];

    for algo in algorithms {
        let name = |suffix: &str| format!("{:?}_{}", algo, suffix).to_ascii_lowercase();
        single_algo_sort_set(c, &name("u8"), algo, 0u8, 10_000_000);
        single_algo_sort_set(c, &name("u16"), algo, 0u16, 10_000_000);
        single_algo_sort_set(c, &name("u32"), algo, 0u32, 10_000_000);
        single_algo_sort_set(c, &name("u64"), algo, 0u64, 10_000_000);
        single_algo_sort_set(c, &name("u128"), algo, 0u128, 10_000_000);
    }
}

criterion_group!(benches, single_algo_sort,);
criterion_main!(benches);
