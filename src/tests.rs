use crate::{RadixKey, RadixSort};
use nanorand::{Rng, WyRand};
use std::time::Instant;
use voracious_radix_sort::{Radixable, RadixSort as Vor};

#[derive(Debug, Eq, PartialEq, Clone, Copy, Ord, PartialOrd)]
struct TestLevel1 {
    key: u8,
}

impl RadixKey for TestLevel1 {
    const LEVELS: usize = 1;

    #[inline]
    fn get_level(&self, _level: usize) -> u8 {
        self.key
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Ord, PartialOrd)]
struct TestLevel4 {
    key: u32,
}

impl RadixKey for TestLevel4 {
    const LEVELS: usize = 4;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        (self.key >> ((Self::LEVELS - 1 - level) * 8)) as u8
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Ord, PartialOrd)]
struct TestComplex {
    key: u64,
    value: u128,
}

impl RadixKey for TestComplex {
    const LEVELS: usize = 4;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        (self.key >> ((Self::LEVELS - 1 - level) * 8)) as u8
    }
}

impl Radixable<u64> for TestComplex {
    type Key = u64;

    fn key(&self) -> Self::Key {
        self.key
    }
}

#[test]
pub fn test_1_level() {
    let mut inputs = vec![
        TestLevel1 { key: 5 },
        TestLevel1 { key: 2 },
        TestLevel1 { key: 7 },
        TestLevel1 { key: 3 },
    ];

    inputs.radix_sort_unstable();

    assert_eq!(
        inputs,
        vec![
            TestLevel1 { key: 2 },
            TestLevel1 { key: 3 },
            TestLevel1 { key: 5 },
            TestLevel1 { key: 7 },
        ]
    );
}

#[test]
pub fn test_4_level() {
    let mut inputs = vec![
        TestLevel4 { key: 4294967295 },
        TestLevel4 { key: 4294967294 },
        TestLevel4 { key: 543 },
        TestLevel4 { key: 544 },
        TestLevel4 { key: 0 },
    ];

    inputs.radix_sort_unstable();

    assert_eq!(
        inputs,
        vec![
            TestLevel4 { key: 0 },
            TestLevel4 { key: 543 },
            TestLevel4 { key: 544 },
            TestLevel4 { key: 4294967294 },
            TestLevel4 { key: 4294967295 },
        ]
    );
}

#[test]
pub fn test_random_4_level() {
    let mut inputs = Vec::new();
    let mut rng = WyRand::new();

    for _ in 0..1_000_000 {
        inputs.push(TestLevel4 {
            key: rng.generate::<u32>(),
        })
    }

    let mut inputs_clone = inputs[..].to_vec();

    inputs.radix_sort_unstable();
    inputs_clone.sort_unstable_by_key(|i| i.key);

    assert_eq!(inputs, inputs_clone);
}

#[test]
pub fn test_complex_4_level_solo() {
    let n = 10_000_000;
    let mut inputs = Vec::with_capacity(n);
    let mut rng = WyRand::new();

    for _ in 0..n {
        inputs.push(TestComplex {
            key: rng.generate::<u64>(),
            value: 0,
        });
    }

    inputs.radix_sort_unstable();
}

#[test]
pub fn test_random_4_level_solo() {
    let n = 100_000_000;
    let mut inputs = Vec::with_capacity(n);
    let mut rng = WyRand::new();

    for _ in 0..n {
        inputs.push(rng.generate::<u32>());
    }

    let start = Instant::now();
    inputs.radix_sort_unstable();
    println!(
        "time to sort 200,000,000 random u32 structs: {}ms",
        start.elapsed().as_millis()
    );
}

#[test]
pub fn test_random_4_level_voracious() {
    let n = 100_000_000;
    let mut inputs = Vec::with_capacity(n);
    let mut rng = WyRand::new();

    for _ in 0..n {
        inputs.push(rng.generate::<u32>());
    }

    let start = Instant::now();
    inputs.voracious_mt_sort(num_cpus::get());
    println!(
        "time for voracious to sort 200,000,000 random u32 structs: {}ms",
        start.elapsed().as_millis()
    );
}

#[test]
pub fn test_series_4_level() {
    let mut inputs = Vec::new();

    for i in 0..1_000_000 {
        inputs.push(TestLevel4 { key: i })
    }

    let mut inputs_clone = inputs[..].to_vec();

    inputs.radix_sort_unstable();
    inputs_clone.sort_by_key(|i| i.key);

    assert_eq!(inputs, inputs_clone);
}
