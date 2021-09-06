use criterion::{black_box, criterion_group, criterion_main, Criterion};
use nanorand::{RandomGen, WyRand};
use rdst::bench_utils::bench_common;
use rdst::{RadixKey, RadixSort};
use std::fmt::Debug;
use std::ops::{Shl, Shr};
use voracious_radix_sort::{RadixKey as VorKey, RadixSort as Vor, Radixable};

fn full_sort_common<T>(c: &mut Criterion, shift: T, name_suffix: &str)
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
        + Shr<Output = T>
        + Radixable<T>
        + VorKey,
{
    let tests: Vec<(&str, Box<dyn Fn(Vec<_>)>)> = vec![
        (
            "rdst",
            Box::new(|mut input| {
                input.radix_sort_unstable();
                black_box(input);
            }),
        ),
        (
            "voracious",
            Box::new(|mut input| {
                input.voracious_mt_sort(num_cpus::get());
                black_box(input);
            }),
        ),
    ];

    bench_common(c, shift, &("full_sort_".to_owned() + name_suffix), tests);
}

fn full_sort_u32(c: &mut Criterion) {
    full_sort_common(c, 0u32, "u32");
}

fn full_sort_u64(c: &mut Criterion) {
    full_sort_common(c, 0u64, "u64");
}

fn full_sort_u32_bimodal(c: &mut Criterion) {
    full_sort_common(c, 16u32, "u32_bimodal");
}

fn full_sort_u64_bimodal(c: &mut Criterion) {
    full_sort_common(c, 32u64, "u64_bimodal");
}

criterion_group!(
    benches,
    full_sort_u32,
    full_sort_u64,
    full_sort_u32_bimodal,
    full_sort_u64_bimodal,
);
criterion_main!(benches);
