use criterion::*;
use nanorand::{WyRand, RandomGen};
use rdst::utils::*;
use rdst::sorts::ska_sort::ska_sort_adapter;
use rdst::sorts::scanning_radix_sort::scanning_radix_sort;
use rdst::sorts::lsb_radix_sort::lsb_radix_sort_adapter;
use rdst::tuning_parameters::TuningParameters;
use std::time::Duration;
use rdst::RadixKey;
use rdst::test_utils::gen_bench_input_set;
use std::fmt::Debug;
use std::ops::{Shl, Shr};
use rdst::sorts::recombinating_sort::recombinating_sort;

fn bench_common<T>(
    c: &mut Criterion,
    shift: T,
    group: &str,
    tests: Vec<(&str, Box<dyn Fn(Vec<T>)>)>,
)
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
    let input_sets = gen_bench_input_set(shift);

    let mut group = c.benchmark_group(group);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));

    for set in input_sets.iter() {
        let l = set.len();
        group.throughput(Throughput::Elements(l as u64));

        for t in tests.iter() {

            group.bench_with_input(BenchmarkId::new((*t).0.clone(), l), set, |bench, set| {
                bench.iter_batched(|| set.clone(), &*t.1,BatchSize::SmallInput);
            });
        }
    }
    group.finish();
}

fn counts(c: &mut Criterion)
{
    let tests: Vec<(&str, Box<dyn Fn(Vec<_>)>)> = vec![
        ("get_counts", Box::new(|input: Vec<_>| {
            let c = get_counts(&input, 0);
            black_box(c);
        })),
        ("par_get_counts", Box::new(|input: Vec<_>| {
            let c = par_get_counts(&input, 0);
            black_box(c);
        })),
    ];

    bench_common::<>(
        c,
        0u32,
        "counts",
        tests,
    );
}

fn all_sort_common<T>(c: &mut Criterion, shift: T, name_suffix: &str)
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
        ("scanning_radix_sort", Box::new(|mut input| {
            let tuning = TuningParameters::new(4);
            scanning_radix_sort(&tuning, &mut input, 3, false);
            black_box(input);
        })),
        ("lsb_radix_sort", Box::new(|mut input| {
            lsb_radix_sort_adapter(&mut input, 0, 3);
            black_box(input);
        })),
        ("ska_sort", Box::new(|mut input| {
            let tuning = TuningParameters::new(4);
            ska_sort_adapter(&tuning, &mut input, 3);
            black_box(input);
        })),
        ("recombinating_sort", Box::new(|mut input| {
            let tuning = TuningParameters::new(4);
            recombinating_sort(&tuning, &mut input, 3);
            black_box(input);
        })),
    ];

    bench_common::<>(
        c,
        shift,
        &("all_sort_".to_owned() + name_suffix),
        tests,
    );
}

fn all_sort_u32(c: &mut Criterion) {
    all_sort_common(c, 0u32, "u32");
}

fn all_sort_u64(c: &mut Criterion) {
    all_sort_common(c, 0u64, "u64");
}

fn all_sort_u32_bimodal(c: &mut Criterion) {
    all_sort_common(c, 16u32, "u32_bimodal");
}

fn all_sort_u64_bimodal(c: &mut Criterion) {
    all_sort_common(c, 32u64, "u64_bimodal");
}

criterion_group!(tuning_parameters, counts, all_sort_u32, all_sort_u64, all_sort_u32_bimodal, all_sort_u64_bimodal);
criterion_main!(tuning_parameters);
