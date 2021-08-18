use criterion::*;
use nanorand::{Rng, WyRand};
use rdst::{get_counts, lsb_radix_sort_adapter, msb_ska_sort, par_get_counts, scanning_radix_sort, TuningParameters, get_all_counts, par_get_all_counts};
use std::time::Duration;

fn counts(c: &mut Criterion) {
    let n = 500_000_000;
    let mut inputs = Vec::with_capacity(n);
    let mut rng = WyRand::new();

    for _ in 0..n {
        inputs.push(rng.generate::<u32>());
    }

    let input_sets: Vec<Vec<u32>> = vec![
        inputs.clone(),
        inputs[..200_000_000].to_vec(),
        inputs[..100_000_000].to_vec(),
        inputs[..50_000_000].to_vec(),
        inputs[..10_000_000].to_vec(),
        inputs[..5_000_000].to_vec(),
        inputs[..2_000_000].to_vec(),
        inputs[..1_000_000].to_vec(),
        inputs[..500_000].to_vec(),
        inputs[..300_000].to_vec(),
        inputs[..200_000].to_vec(),
        inputs[..100_000].to_vec(),
        inputs[..50_000].to_vec(),
        inputs[..10_000].to_vec(),
        inputs[..5_000].to_vec(),
    ];

    drop(inputs);

    let mut group = c.benchmark_group("counts");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));
    for set in input_sets.iter() {
        let l = set.len();
        group.throughput(Throughput::Elements(l as u64));
        group.bench_with_input(BenchmarkId::new("get_counts", l), set, |bench, set| {
            bench.iter_batched(
                || set.clone(),
                |input| {
                    let c = get_counts(&input, 0);
                    black_box(c);
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("par_get_counts", l), set, |bench, set| {
            bench.iter_batched(
                || set.clone(),
                |input| {
                    let c = par_get_counts(&input, 0);
                    black_box(c);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn all_counts(c: &mut Criterion) {
    let n = 200_000_000;
    let mut inputs = Vec::with_capacity(n);
    let mut rng = WyRand::new();

    for _ in 0..n {
        inputs.push(rng.generate::<u64>());
    }

    let input_sets: Vec<Vec<_>> = vec![
        inputs.clone(),
        inputs[..100_000_000].to_vec(),
        inputs[..50_000_000].to_vec(),
        inputs[..10_000_000].to_vec(),
        inputs[..5_000_000].to_vec(),
        inputs[..2_000_000].to_vec(),
        inputs[..1_000_000].to_vec(),
        inputs[..500_000].to_vec(),
        inputs[..300_000].to_vec(),
        inputs[..200_000].to_vec(),
        inputs[..100_000].to_vec(),
        inputs[..50_000].to_vec(),
        inputs[..10_000].to_vec(),
        inputs[..5_000].to_vec(),
    ];

    drop(inputs);

    let mut group = c.benchmark_group("counts");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));
    for set in input_sets.iter() {
        let l = set.len();
        group.throughput(Throughput::Elements(l as u64));
        group.bench_with_input(BenchmarkId::new("get_all_counts", l), set, |bench, set| {
            bench.iter_batched(
                || set.clone(),
                |input| {
                    let c = get_all_counts(&input, 0, 4);
                    black_box(c);
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("par_get_all_counts", l), set, |bench, set| {
            bench.iter_batched(
                || set.clone(),
                |input| {
                    let c = par_get_all_counts(&input, 0, 4);
                    black_box(c);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn scanning_sort(c: &mut Criterion) {
    let n = 200_000_000;
    let mut inputs = Vec::with_capacity(n);
    let mut rng = WyRand::new();
    let tuning = TuningParameters::new(4);

    for _ in 0..n {
        inputs.push(rng.generate::<u32>());
    }

    let input_sets: Vec<Vec<u32>> = vec![
        inputs.clone(),
        inputs[..100_000_000].to_vec(),
        inputs[..50_000_000].to_vec(),
        inputs[..10_000_000].to_vec(),
        inputs[..5_000_000].to_vec(),
        inputs[..2_000_000].to_vec(),
        inputs[..1_000_000].to_vec(),
        inputs[..500_000].to_vec(),
        inputs[..300_000].to_vec(),
        inputs[..200_000].to_vec(),
        inputs[..100_000].to_vec(),
        inputs[..50_000].to_vec(),
        inputs[..10_000].to_vec(),
        inputs[..5_000].to_vec(),
    ];

    drop(inputs);

    let mut group = c.benchmark_group("scanning_sort_level_4");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));
    for set in input_sets.iter() {
        let l = set.len();
        group.throughput(Throughput::Elements(l as u64));
        group.bench_with_input(
            BenchmarkId::new("scanning_radix_sort", l),
            set,
            |bench, set| {
                bench.iter_batched(
                    || set.clone(),
                    |mut input| {
                        scanning_radix_sort(&tuning, &mut input, 0, true);
                        black_box(input);
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(BenchmarkId::new("lsb_radix_sort", l), set, |bench, set| {
            bench.iter_batched(
                || set.clone(),
                |mut input| {
                    lsb_radix_sort_adapter(&mut input, 3, 0, true);
                    black_box(input);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn ska_sort(c: &mut Criterion) {
    let n = 10_000_000;
    let mut inputs = Vec::with_capacity(n);
    let mut rng = WyRand::new();
    let tuning = TuningParameters::new(8);

    for _ in 0..n {
        inputs.push(rng.generate::<u32>());
    }

    let input_sets: Vec<Vec<u32>> = vec![
        inputs.clone(),
        inputs[..5_000_000].to_vec(),
        inputs[..2_000_000].to_vec(),
        inputs[..1_000_000].to_vec(),
        inputs[..500_000].to_vec(),
        inputs[..300_000].to_vec(),
        inputs[..200_000].to_vec(),
        inputs[..100_000].to_vec(),
        inputs[..50_000].to_vec(),
        inputs[..10_000].to_vec(),
        inputs[..5_000].to_vec(),
    ];

    drop(inputs);

    let mut group = c.benchmark_group("ska_sort_level_4");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));
    for set in input_sets.iter() {
        let l = set.len();
        group.throughput(Throughput::Elements(l as u64));
        group.bench_with_input(BenchmarkId::new("ska_sort", l), set, |bench, set| {
            bench.iter_batched(
                || set.clone(),
                |mut input| {
                    msb_ska_sort(&tuning, &mut input, 0);
                    black_box(input);
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("lsb_radix_sort", l), set, |bench, set| {
            bench.iter_batched(
                || set.clone(),
                |mut input| {
                    lsb_radix_sort_adapter(&mut input, 3, 0, true);
                    black_box(input);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();

    let mut inputs = Vec::with_capacity(n);

    for _ in 0..n {
        inputs.push(rng.generate::<u64>());
    }

    let input_sets: Vec<Vec<u64>> = vec![
        inputs.clone(),
        inputs[..5_000_000].to_vec(),
        inputs[..2_000_000].to_vec(),
        inputs[..1_000_000].to_vec(),
        inputs[..500_000].to_vec(),
        inputs[..300_000].to_vec(),
        inputs[..200_000].to_vec(),
        inputs[..100_000].to_vec(),
        inputs[..50_000].to_vec(),
        inputs[..10_000].to_vec(),
        inputs[..5_000].to_vec(),
    ];

    drop(inputs);

    let mut group = c.benchmark_group("ska_sort_level_8");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));
    for set in input_sets.iter() {
        let l = set.len();
        group.throughput(Throughput::Elements(l as u64));
        group.bench_with_input(BenchmarkId::new("ska_sort", l), set, |bench, set| {
            bench.iter_batched(
                || set.clone(),
                |mut input| {
                    msb_ska_sort(&tuning, &mut input, 0);
                    black_box(input);
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("lsb_radix_sort", l), set, |bench, set| {
            bench.iter_batched(
                || set.clone(),
                |mut input| {
                    lsb_radix_sort_adapter(&mut input, 3, 0, true);
                    black_box(input);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(tuning_parameters, counts, all_counts, scanning_sort, ska_sort);
criterion_main!(tuning_parameters);
