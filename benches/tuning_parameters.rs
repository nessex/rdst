use criterion::*;
use rayon::current_num_threads;
use rdst::utils::bench_utils::{bench_common, bench_comparative};
use rdst::utils::test_utils::NumericTest;
use rdst::utils::*;
use std::cmp::max;

fn tune_counts(c: &mut Criterion) {
    let tests: Vec<(&str, Box<dyn Fn(Vec<_>)>)> = vec![
        (
            "get_counts",
            Box::new(|input: Vec<_>| {
                let c = get_counts(&input, 0);
                black_box(c);
            }),
        ),
        (
            "par_get_counts",
            Box::new(|input: Vec<_>| {
                let c = par_get_counts(&input, 0);
                black_box(c);
            }),
        ),
        (
            "get_tile_counts",
            Box::new(|input: Vec<_>| {
                let tile_size = max(30_000, cdiv(input.len(), current_num_threads()));
                let c = get_tile_counts(&input, tile_size, 0);
                black_box(c);
            }),
        ),
        (
            "get_tile_counts_and_aggregate",
            Box::new(|input: Vec<_>| {
                let tile_size = max(30_000, cdiv(input.len(), current_num_threads()));
                let c = get_tile_counts(&input, tile_size, 0);
                let a = aggregate_tile_counts(&c);
                black_box(a);
            }),
        ),
    ];

    bench_common(c, 0u32, "tune_counts", tests);
}

fn tune_sort_common<T>(c: &mut Criterion, shift: T, name_suffix: &str)
where
    T: NumericTest<T>,
{
    let tests: Vec<(&str, Box<dyn Fn(Vec<_>)>)> = vec![
        // (
        //     "regions_sort",
        //     Box::new(|mut input| {
        //         let tuner = DefaultTuner {};
        //         regions_sort_adapter(&tuner, true, &mut input, 3);
        //         black_box(input);
        //     }),
        // ),
        // (
        //     "scanning_sort",
        //     Box::new(|mut input| {
        //         let tuner = DefaultTuner {};
        //         scanning_sort_adapter(&tuner, false, &mut input, 3);
        //         black_box(input);
        //     }),
        // ),
        // (
        //     "lsb_sort",
        //     Box::new(|mut input| {
        //         lsb_sort_adapter(&mut input, 0, 3);
        //         black_box(input);
        //     }),
        // ),
        // (
        //     "ska_sort",
        //     Box::new(|mut input| {
        //         let tuner = DefaultTuner {};
        //         ska_sort_adapter(&tuner, true, &mut input, 3);
        //         black_box(input);
        //     }),
        // ),
        // (
        //     "recombinating_sort",
        //     Box::new(|mut input| {
        //         let tuner = DefaultTuner {};
        //         recombinating_sort_adapter(&tuner, false, &mut input, 3);
        //         black_box(input);
        //     }),
        // ),
    ];

    bench_common(c, shift, &("tune_".to_owned() + name_suffix), tests);
}

fn tune_sort_u32(c: &mut Criterion) {
    tune_sort_common(c, 0u32, "sort_u32");
}

fn tune_sort_u64(c: &mut Criterion) {
    tune_sort_common(c, 0u64, "sort_u64");
}

fn tune_sort_u32_bimodal(c: &mut Criterion) {
    tune_sort_common(c, 16u32, "sort_u32_bimodal");
}

fn tune_sort_u64_bimodal(c: &mut Criterion) {
    tune_sort_common(c, 32u64, "sort_u64_bimodal");
}

fn tune_sort_comparative<T>(c: &mut Criterion, shift: T, name_suffix: &str)
where
    T: NumericTest<T>,
{
    let tests: Vec<(&str, Box<dyn Fn(Vec<_>)>)> = vec![
        // (
        //     "lsb_sort",
        //     Box::new(|mut input| {
        //         lsb_sort_adapter(&mut input, 0, 3);
        //         black_box(input);
        //     }),
        // ),
        // (
        //     "comparative_sort",
        //     Box::new(|mut input| {
        //         comparative_sort(&mut input, 3);
        //         black_box(input);
        //     }),
        // ),
    ];

    bench_comparative(
        c,
        shift,
        &("tune_comparative_".to_owned() + name_suffix),
        tests,
    );
}

fn tune_sort_comparative_u32(c: &mut Criterion) {
    tune_sort_comparative(c, 0u32, "sort_u32");
}

fn tune_sort_comparative_u64(c: &mut Criterion) {
    tune_sort_comparative(c, 0u64, "sort_u64");
}

fn tune_sort_comparative_u32_bimodal(c: &mut Criterion) {
    tune_sort_comparative(c, 16u32, "sort_u32_bimodal");
}

fn tune_sort_comparative_u64_bimodal(c: &mut Criterion) {
    tune_sort_comparative(c, 32u64, "sort_u64_bimodal");
}

criterion_group!(
    tuning_parameters,
    tune_counts,
    tune_sort_u32,
    tune_sort_u64,
    tune_sort_u32_bimodal,
    tune_sort_u64_bimodal,
    tune_sort_comparative_u32,
    tune_sort_comparative_u64,
    tune_sort_comparative_u32_bimodal,
    tune_sort_comparative_u64_bimodal,
);
criterion_main!(tuning_parameters);
