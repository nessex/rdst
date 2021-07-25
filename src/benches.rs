use crate::{RadixKey, RadixSort};
use rand::{thread_rng, RngCore};
use test::{black_box, Bencher};
use rayon::prelude::*;
use voracious_radix_sort::{Radixable, RadixSort as VoraciousRadixSort};

#[derive(Debug, Eq, PartialEq, Clone, Copy, PartialOrd, Ord)]
struct BenchLevel8 {
    key: u64,
}

impl RadixKey for BenchLevel8 {
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

impl Radixable<u64> for BenchLevel8 {
    type Key = u64;

    fn key(&self) -> Self::Key {
        self.key
    }
}

fn bench_cmp_base(bench: &mut Bencher, f: fn(&mut Vec<BenchLevel8>)) {
    let mut inputs = Vec::new();
    let mut rng = thread_rng();

    for _ in 0..1000000 {
        inputs.push(BenchLevel8 {
            key: rng.next_u64(),
        })
    }

    bench.iter(|| {
        let mut inputs_clone = inputs[..].to_vec();
        f(&mut inputs_clone);
        black_box(inputs_clone);
    });
}

#[bench]
pub fn bench_base_radix_sort(bench: &mut Bencher) {
    let f = |v: &mut Vec<BenchLevel8>| v.radix_sort_unstable();

    bench_cmp_base(bench, f);
}

#[bench]
pub fn bench_base_sort_unstable(bench: &mut Bencher) {
    let f = |v: &mut Vec<BenchLevel8>| v.sort_unstable();

    bench_cmp_base(bench, f);
}

#[bench]
pub fn bench_base_par_sort_unstable(bench: &mut Bencher) {
    let f = |v: &mut Vec<BenchLevel8>| v.par_sort_unstable();

    bench_cmp_base(bench, f);
}

#[bench]
pub fn bench_base_voracious_mt_sort(bench: &mut Bencher) {
    let f = |v: &mut Vec<BenchLevel8>| v.voracious_mt_sort(num_cpus::get());

    bench_cmp_base(bench, f);
}
