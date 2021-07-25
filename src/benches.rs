use crate::{RadixKey, RadixSort};
use rand::{thread_rng, RngCore};
use test::{black_box, Bencher};
use rayon::prelude::*;

#[derive(Debug, Eq, PartialEq, Clone, Copy, PartialOrd, Ord)]
struct BenchLevel4 {
    key: u64,
}

impl RadixKey for BenchLevel4 {
    const LEVELS: usize = 8;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        let b = self.key.to_le_bytes();

        match level {
            0 => b[7],
            1 => b[6],
            2 => b[5],
            3 => b[4],
            4 => b[3],
            5 => b[2],
            6 => b[1],
            _ => b[0],
        }
    }
}

#[bench]
pub fn bench_series_level_4(bench: &mut Bencher) {
    let mut inputs = Vec::new();
    let mut rng = thread_rng();

    for _ in 0..1000000 {
        inputs.push(BenchLevel4 {
            key: rng.next_u64(),
        })
    }

    bench.iter(|| {
        let mut inputs_clone = inputs[..].to_vec();
        RadixSort::sort(&mut inputs_clone);
        black_box(inputs_clone);
    });
}

#[bench]
pub fn bench_base_sort(bench: &mut Bencher) {
    let mut inputs = Vec::new();
    let mut rng = thread_rng();

    for _ in 0..1000000 {
        inputs.push(BenchLevel4 {
            key: rng.next_u64(),
        })
    }

    bench.iter(|| {
        let mut inputs_clone = inputs[..].to_vec();
        inputs_clone.sort_unstable();
        black_box(inputs_clone);
    });
}

#[bench]
pub fn bench_base_par_sort(bench: &mut Bencher) {
    let mut inputs = Vec::new();
    let mut rng = thread_rng();

    for _ in 0..1000000 {
        inputs.push(BenchLevel4 {
            key: rng.next_u64(),
        })
    }

    bench.iter(|| {
        let mut inputs_clone = inputs[..].to_vec();
        inputs_clone.par_sort_unstable();
        black_box(inputs_clone);
    });
}
