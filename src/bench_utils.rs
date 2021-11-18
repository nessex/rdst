use crate::test_utils::{gen_bench_input_set, gen_inputs, NumericTest};
use criterion::{AxisScale, BatchSize, BenchmarkId, Criterion, PlotConfiguration, Throughput};
use std::time::Duration;

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
