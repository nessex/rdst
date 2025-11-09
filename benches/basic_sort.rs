mod bench_utils;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use bench_utils::bench_single;
use bench_utils::NumericTest;
use rdst::RadixSort;

fn basic_sort_set<T>(c: &mut Criterion, suffix: &str, shift: T, count: usize)
where
    T: NumericTest<T>,
{
    let tests: Vec<(&str, Box<dyn Fn(Vec<T>)>)> = vec![
        (
            "rdst",
            Box::new(|mut input| {
                input.radix_sort_unstable();
                black_box(input);
            }),
        ),
        (
            "rdst_low_mem",
            Box::new(|mut input| {
                input.radix_sort_builder().with_low_mem_tuner().sort();
                black_box(input);
            }),
        ),
        (
            "rdst_single_threaded",
            Box::new(|mut input| {
                input
                    .radix_sort_builder()
                    .with_single_threaded_tuner()
                    .with_parallel(false)
                    .sort();

                black_box(input);
            }),
        ),
    ];

    bench_single(c, &("basic_sort_".to_owned() + suffix), tests, shift, count);
}

fn basic_sort(c: &mut Criterion) {
    basic_sort_set(c, "u32", 0u32, 10_000_000);
}

criterion_group!(benches, basic_sort,);
criterion_main!(benches);
