use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use nanorand::{Rng, WyRand};
use rdst::RadixSort;
use std::time::Duration;
use voracious_radix_sort::RadixSort as Vor;

fn full_sort_u32(c: &mut Criterion) {
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
        inputs[..4_000_000].to_vec(),
        inputs[..3_000_000].to_vec(),
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

    let mut group = c.benchmark_group("full_sort_u32");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));
    for set in input_sets.iter() {
        let l = set.len();
        group.throughput(Throughput::Elements(l as u64));
        group.bench_with_input(BenchmarkId::new("rdst", l), set, |bench, set| {
            bench.iter_batched(
                || set.clone(),
                |mut input| {
                    input.radix_sort_unstable();
                    black_box(input);
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("voracious", l), set, |bench, set| {
            bench.iter_batched(
                || set.clone(),
                |mut input| {
                    input.voracious_mt_sort(num_cpus::get());
                    black_box(input);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn full_sort_u64(c: &mut Criterion) {
    let n = 200_000_000;
    let mut inputs = Vec::with_capacity(n);
    let mut rng = WyRand::new();

    for _ in 0..n {
        inputs.push(rng.generate::<u64>());
    }

    let input_sets: Vec<Vec<u64>> = vec![
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

    let mut group = c.benchmark_group("full_sort_u64");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));
    for set in input_sets.iter() {
        let l = set.len();
        group.throughput(Throughput::Elements(l as u64));
        group.bench_with_input(BenchmarkId::new("rdst", l), set, |bench, set| {
            bench.iter_batched(
                || set.clone(),
                |mut input| {
                    input.radix_sort_unstable();
                    black_box(input);
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("voracious", l), set, |bench, set| {
            bench.iter_batched(
                || set.clone(),
                |mut input| {
                    input.voracious_mt_sort(num_cpus::get());
                    black_box(input);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn full_sort_u64_truncated(c: &mut Criterion) {
    let n = 100_000_000;
    let mut inputs = Vec::with_capacity(n);
    let mut rng = WyRand::new();

    for _ in 0..n {
        inputs.push(rng.generate::<u32>() as u64);
    }

    let input_sets: Vec<Vec<u64>> = vec![
        inputs.clone(),
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

    let mut group = c.benchmark_group("full_sort_u64_truncated");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));
    for set in input_sets.iter() {
        let l = set.len();
        group.throughput(Throughput::Elements(l as u64));
        group.bench_with_input(BenchmarkId::new("rdst", l), set, |bench, set| {
            bench.iter_batched(
                || set.clone(),
                |mut input| {
                    input.radix_sort_unstable();
                    black_box(input);
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("voracious", l), set, |bench, set| {
            bench.iter_batched(
                || set.clone(),
                |mut input| {
                    input.voracious_mt_sort(num_cpus::get());
                    black_box(input);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn full_sort_u64_bimodal(c: &mut Criterion) {
    let n = 200_000_000;
    let mut inputs = Vec::with_capacity(n);
    let mut rng = WyRand::new();
    let shift = 32u64;

    for _ in 0..(n/2) {
        inputs.push(rng.generate::<u64>() >> shift);
    }

    for _ in 0..(n/2) {
        inputs.push(rng.generate::<u64>() << shift);
    }

    let input_sets: Vec<Vec<u64>> = vec![
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

    let mut group = c.benchmark_group("full_sort_u64_bimodal");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));
    for set in input_sets.iter() {
        let l = set.len();
        group.throughput(Throughput::Elements(l as u64));
        group.bench_with_input(BenchmarkId::new("rdst", l), set, |bench, set| {
            bench.iter_batched(
                || set.clone(),
                |mut input| {
                    input.radix_sort_unstable();
                    black_box(input);
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("voracious", l), set, |bench, set| {
            bench.iter_batched(
                || set.clone(),
                |mut input| {
                    input.voracious_mt_sort(num_cpus::get());
                    black_box(input);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn full_sort_u64_truncated_shifted(c: &mut Criterion) {
    let n = 200_000_000;
    let mut inputs = Vec::with_capacity(n);
    let mut rng = WyRand::new();

    for _ in 0..n {
        inputs.push((rng.generate::<u32>() as u64) << 16);
    }

    let input_sets: Vec<Vec<u64>> = vec![
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

    let mut group = c.benchmark_group("full_sort_u64_truncated_shifted");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));
    for set in input_sets.iter() {
        let l = set.len();
        group.throughput(Throughput::Elements(l as u64));
        group.bench_with_input(BenchmarkId::new("rdst", l), set, |bench, set| {
            bench.iter_batched(
                || set.clone(),
                |mut input| {
                    input.radix_sort_unstable();
                    black_box(input);
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("voracious", l), set, |bench, set| {
            bench.iter_batched(
                || set.clone(),
                |mut input| {
                    input.voracious_mt_sort(num_cpus::get());
                    black_box(input);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    full_sort_u32,
    full_sort_u64,
    full_sort_u64_bimodal,
    full_sort_u64_truncated,
    full_sort_u64_truncated_shifted,
);
criterion_main!(benches);
