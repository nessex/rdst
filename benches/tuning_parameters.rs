use criterion::*;
use nanorand::{RandomGen, WyRand};
use rdst::bench_utils::bench_common;
use rdst::sorts::lsb_radix_sort::lsb_radix_sort_adapter;
use rdst::sorts::recombinating_sort::recombinating_sort;
use rdst::sorts::scanning_radix_sort::scanning_radix_sort;
use rdst::sorts::ska_sort::ska_sort_adapter;
use rdst::tuning_parameters::TuningParameters;
use rdst::utils::*;
use rdst::RadixKey;
use std::fmt::Debug;
use std::ops::{Shl, Shr};

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
    ];

    bench_common(c, 0u32, "tune_counts", tests);
}

fn tune_sort_common<T>(c: &mut Criterion, shift: T, name_suffix: &str)
where
    T: RadixKey
        + Ord
        + RandomGen<WyRand>
        + Clone
        + Debug
        + Send
        + Sized
        + Copy
        + Sync
        + Shl<Output = T>
        + Shr<Output = T>,
{
    let tests: Vec<(&str, Box<dyn Fn(Vec<_>)>)> = vec![
        (
            "scanning_radix_sort",
            Box::new(|mut input| {
                let tuning = TuningParameters::new(4);
                scanning_radix_sort(&tuning, &mut input, 3, true);
                black_box(input);
            }),
        ),
        (
            "lsb_radix_sort",
            Box::new(|mut input| {
                lsb_radix_sort_adapter(&mut input, 0, 3);
                black_box(input);
            }),
        ),
        (
            "ska_sort",
            Box::new(|mut input| {
                let tuning = TuningParameters::new(4);
                ska_sort_adapter(&tuning, &mut input, 3);
                black_box(input);
            }),
        ),
        (
            "recombinating_sort",
            Box::new(|mut input| {
                let tuning = TuningParameters::new(4);
                recombinating_sort(&tuning, &mut input, 3);
                black_box(input);
            }),
        ),
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

criterion_group!(
    tuning_parameters,
    tune_counts,
    tune_sort_u32,
    tune_sort_u64,
    tune_sort_u32_bimodal,
    tune_sort_u64_bimodal
);
criterion_main!(tuning_parameters);
