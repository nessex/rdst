use crate::utils::test_utils::{gen_inputs, NumericTest};
use criterion::{AxisScale, BatchSize, BenchmarkId, Criterion, PlotConfiguration, Throughput};
use std::time::Duration;

pub fn gen_bench_input_set<T>(shift: T) -> Vec<Vec<T>>
where
    T: NumericTest<T>,
{
    let n = 200_000_000;
    let half = n / 2;
    let inputs = gen_inputs(n, shift);

    // Middle values are used for the case where shift is provided
    let mut out = vec![
        inputs[(half - 2_500)..(half + 2_500)].to_vec(),
        inputs[(half - 5_000)..(half + 5_000)].to_vec(),
        inputs[(half - 25_000)..(half + 25_000)].to_vec(),
        inputs[(half - 50_000)..(half + 50_000)].to_vec(),
        inputs[(half - 100_000)..(half + 100_000)].to_vec(),
        inputs[(half - 150_000)..(half + 150_000)].to_vec(),
        inputs[(half - 250_000)..(half + 250_000)].to_vec(),
        inputs[(half - 500_000)..(half + 500_000)].to_vec(),
        inputs[(half - 1_000_000)..(half + 1_000_000)].to_vec(),
        inputs[(half - 2_500_000)..(half + 2_500_000)].to_vec(),
        inputs[(half - 5_000_000)..(half + 5_000_000)].to_vec(),
        inputs[(half - 25_000_000)..(half + 25_000_000)].to_vec(),
        inputs[(half - 50_000_000)..(half + 50_000_000)].to_vec(),
        inputs,
    ];

    out.reverse();

    out
}

pub fn gen_bench_comparative_input_set<T>(shift: T) -> Vec<Vec<T>>
where
    T: NumericTest<T>,
{
    let n = 1_000;
    let half = n / 2;
    let inputs = gen_inputs(n, shift);

    // Middle values are used for the case where shift is provided
    let mut out = vec![
        inputs[(half - 1)..(half + 1)].to_vec(),
        inputs[(half - 10)..(half + 10)].to_vec(),
        inputs[(half - 20)..(half + 20)].to_vec(),
        inputs[(half - 50)..(half + 50)].to_vec(),
        inputs[(half - 75)..(half + 75)].to_vec(),
        inputs[(half - 100)..(half + 100)].to_vec(),
        inputs[(half - 150)..(half + 150)].to_vec(),
        inputs[(half - 200)..(half + 200)].to_vec(),
        inputs[(half - 300)..(half + 300)].to_vec(),
        inputs,
    ];

    out.reverse();

    out
}

pub fn gen_bench_exponential_input_set<T>(shift: T) -> Vec<Vec<T>>
where
    T: NumericTest<T>,
{
    let n = 200_000_000;
    let inputs = gen_inputs(n, shift);
    let mut len = inputs.len();
    let mut out = Vec::new();

    loop {
        let start = (inputs.len() - len) / 2;
        let end = start + len;

        out.push(inputs[start..end].to_vec());

        len = len / 2;
        if len == 0 {
            break;
        }
    }

    out
}

pub fn bench_common<T>(
    c: &mut Criterion,
    shift: T,
    group: &str,
    tests: Vec<(&str, Box<dyn Fn(Vec<T>)>)>,
) where
    T: NumericTest<T>,
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

pub fn bench_medley<T>(
    c: &mut Criterion,
    group: &str,
    tests: Vec<(&str, Box<dyn Fn(Vec<T>)>)>,
    shift: T,
) where
    T: NumericTest<T> + Clone,
{
    let input_sets = gen_bench_exponential_input_set(shift);
    let len: u64 = input_sets.iter().map(|s| s.len() as u64).sum();

    let mut group = c.benchmark_group(group);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.warm_up_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(len));

    for t in tests.iter() {
        group.bench_with_input(
            BenchmarkId::new((*t).0.clone(), len),
            &0u32,
            |bench, _set| {
                bench.iter_batched(
                    || input_sets.clone(),
                    |input| {
                        for set in input {
                            (*t).1(set);
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

pub fn bench_comparative<T>(
    c: &mut Criterion,
    shift: T,
    group: &str,
    tests: Vec<(&str, Box<dyn Fn(Vec<T>)>)>,
) where
    T: NumericTest<T>,
{
    let input_sets = gen_bench_comparative_input_set(shift);

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
