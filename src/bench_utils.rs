use crate::test_utils::gen_bench_input_set;
use crate::RadixKey;
#[cfg(feature = "bench")]
use criterion::{AxisScale, BatchSize, BenchmarkId, Criterion, PlotConfiguration, Throughput};
use nanorand::{RandomGen, WyRand};
use std::fmt::Debug;
use std::ops::{Shl, Shr};
use std::time::Duration;

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
