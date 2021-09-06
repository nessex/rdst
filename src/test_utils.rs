use crate::RadixKey;
#[cfg(feature = "bench")]
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use nanorand::{RandomGen, Rng, WyRand};
use std::fmt::Debug;
use std::ops::{Shl, Shr};
use std::time::Duration;
use criterion::{PlotConfiguration, AxisScale};

#[cfg(feature = "bench")]
pub fn bench_common<T>(
    c: &mut Criterion,
    shift: T,
    group: &str,
    tests: Vec<(&str, Box<dyn Fn(Vec<T>)>)>,
) where
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
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));

    for set in input_sets.iter() {
        let l = set.len();
        group.throughput(Throughput::Elements(l as u64));

        for t in tests.iter() {
            group.bench_with_input(BenchmarkId::new((*t).0.clone(), l), set, |bench, set| {
                bench.iter_batched(|| set.clone(), &*t.1, BatchSize::SmallInput);
            });
        }
    }

    group.finish();
}

pub fn gen_inputs<T>(n: usize, shift: T) -> Vec<T>
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
    let mut inputs: Vec<T> = Vec::with_capacity(n);
    let mut rng = WyRand::new();

    for _ in 0..(n / 2) {
        inputs.push(rng.generate::<T>() >> shift);
    }

    for _ in 0..(n / 2) {
        inputs.push(rng.generate::<T>() << shift);
    }

    inputs
}

pub fn gen_input_set<T>(shift: T) -> Vec<Vec<T>>
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
    let inputs = gen_inputs(50_000_000, shift);

    // Middle values are used for the case where shift is provided
    vec![inputs.clone()]
}

pub fn gen_bench_input_set<T>(shift: T) -> Vec<Vec<T>>
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
    let inputs = gen_inputs(200_000_000, shift);

    // Middle values are used for the case where shift is provided
    vec![
        inputs.clone(),
        inputs[50_000_000..150_000_000].to_vec(),
        inputs[75_000_000..125_000_000].to_vec(),
        inputs[95_000_000..105_000_000].to_vec(),
        inputs[97_500_000..102_500_000].to_vec(),
        inputs[99_000_000..101_000_000].to_vec(),
        inputs[99_500_000..100_500_000].to_vec(),
        inputs[99_750_000..100_250_000].to_vec(),
        inputs[99_850_000..100_150_000].to_vec(),
        inputs[99_900_000..100_100_000].to_vec(),
        inputs[99_950_000..100_050_000].to_vec(),
        inputs[99_975_000..100_025_000].to_vec(),
        inputs[99_995_000..100_005_000].to_vec(),
        inputs[99_997_500..100_002_500].to_vec(),
    ]
}

pub fn validate_sort<T, F>(mut inputs: Vec<T>, sort_fn: F)
where
    T: RadixKey + Ord + RandomGen<WyRand> + Clone + Debug + Send + Copy + Sync,
    F: Fn(&mut [T]),
{
    let mut inputs_clone = inputs.clone();

    sort_fn(&mut inputs);
    inputs_clone.sort_unstable();

    assert_eq!(inputs, inputs_clone);
}

pub fn sort_comparison_suite<T, F>(shift: T, sort_fn: F)
where
    F: Fn(&mut [T]),
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
    let input_set = gen_input_set(shift);

    for s in input_set {
        validate_sort(s, &sort_fn);
    }
}
