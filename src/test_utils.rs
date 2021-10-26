use crate::RadixKey;
use block_pseudorand::block_rand;
use rayon::prelude::*;
use std::fmt::Debug;
use std::ops::{Shl, ShlAssign, Shr, ShrAssign};

pub trait NumericTest<T>:
    RadixKey
    + Sized
    + Copy
    + Debug
    + PartialEq
    + Ord
    + Send
    + Sync
    + Shl<Output = T>
    + Shr<Output = T>
    + ShrAssign
    + ShlAssign
{
}

impl<T> NumericTest<T> for T where
    T: RadixKey
        + Sized
        + Copy
        + Debug
        + PartialEq
        + Ord
        + Send
        + Sync
        + Shl<Output = T>
        + Shr<Output = T>
        + ShrAssign
        + ShlAssign
{
}

pub fn gen_inputs<T>(n: usize, shift: T) -> Vec<T>
where
    T: NumericTest<T>,
{
    let mut inputs: Vec<T> = block_rand(n);

    inputs[0..(n / 2)].par_iter_mut().for_each(|v| *v >>= shift);
    inputs[(n / 2)..n].par_iter_mut().for_each(|v| *v <<= shift);

    inputs
}

pub fn gen_input_set<T>(shift: T) -> Vec<Vec<T>>
where
    T: NumericTest<T>,
{
    let n = 50_000_000;
    let half = n / 2;
    let inputs = gen_inputs(n, shift);

    // Middle values are used for the case where shift is provided
    let mut out = vec![
        vec![],
        inputs[..1].to_vec(),
        inputs[(half - 5)..(half + 5)].to_vec(),
        inputs[(half - 50)..(half + 50)].to_vec(),
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
        inputs,
    ];

    out.reverse();

    out
}

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

pub fn validate_sort<T, F>(mut inputs: Vec<T>, sort_fn: F)
where
    T: NumericTest<T>,
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
    T: NumericTest<T>,
{
    let input_set = gen_input_set(shift);

    for s in input_set {
        validate_sort(s, &sort_fn);
    }
}
