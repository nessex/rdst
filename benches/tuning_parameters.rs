use criterion::*;
use rayon::current_num_threads;
use rdst::utils::bench_utils::bench_common;
use rdst::utils::*;
use std::cmp::max;

fn tune_counts(c: &mut Criterion) {
    let tests: Vec<(&str, Box<dyn Fn(Vec<_>)>)> = vec![
        (
            "get_counts",
            Box::new(|input: Vec<_>| {
                let (c, _) = get_counts(&input, 0);
                black_box(c);
            }),
        ),
        (
            "par_get_counts",
            Box::new(|input: Vec<_>| {
                let (c, _) = par_get_counts(&input, 0);
                black_box(c);
            }),
        ),
        (
            "get_tile_counts",
            Box::new(|input: Vec<_>| {
                let tile_size = max(30_000, cdiv(input.len(), current_num_threads()));
                let (c, _) = get_tile_counts(&input, tile_size, 0);
                black_box(c);
            }),
        ),
        (
            "get_tile_counts_and_aggregate",
            Box::new(|input: Vec<_>| {
                let tile_size = max(30_000, cdiv(input.len(), current_num_threads()));
                let (c, _) = get_tile_counts(&input, tile_size, 0);
                let a = aggregate_tile_counts(&c);
                black_box(a);
            }),
        ),
    ];

    bench_common(c, 0u32, "tune_counts", tests);
}

criterion_group!(tuning_parameters, tune_counts,);
criterion_main!(tuning_parameters);
