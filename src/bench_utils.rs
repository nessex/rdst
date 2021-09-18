use crate::test_utils::{gen_bench_input_set, NumericTest};
use criterion::{AxisScale, BatchSize, BenchmarkId, Criterion, PlotConfiguration, Throughput};
use std::time::Duration;

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
