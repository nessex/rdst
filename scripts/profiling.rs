#!/usr/bin/env -S cargo +nightly -Zscript
---
[package]
edition = "2024"

[dependencies]
block-pseudorand = "0.1.2"
rayon = "1.8"
rdst = { path = "../" }

[profile.dev]
codegen-units = 1
opt-level = 3
debug = false
---

use rayon::prelude::*;
use std::fmt::Debug;
use std::ops::{Shl, ShlAssign, Shr, ShrAssign};
use rdst::tuner::{Algorithm, Tuner, TuningParams};
use rdst::{RadixKey, RadixSort};
use std::thread::sleep;
use std::time::{Duration, Instant};
use block_pseudorand::block_rand;

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

fn gen_inputs<T>(n: usize, shift: T) -> Vec<T>
where
    T: NumericTest<T>,
{
    let mut inputs: Vec<T> = block_rand(n);

    inputs[0..(n / 2)].par_iter_mut().for_each(|v| *v >>= shift);
    inputs[(n / 2)..n].par_iter_mut().for_each(|v| *v <<= shift);

    inputs
}

struct MyTuner {}

impl Tuner for MyTuner {
    fn pick_algorithm(&self, p: &TuningParams, _: &[usize]) -> Algorithm {
        if p.input_len < 128 {
            return Algorithm::Comparative;
        }

        let depth = p.total_levels - p.level - 1;
        match depth {
            0 => Algorithm::MtLsb,
            _ => Algorithm::Lsb,
        }
    }
}

fn main() {
    // Randomly generate an array of
    // 200_000_000 u64's with half shifted >> 32 and half shifted << 32
    let mut inputs = gen_inputs(50_000_000, 0u128);
    let mut inputs_2 = gen_inputs(50_000_000, 0u128);

    // Input generation is multithreaded and hard to differentiate from the actual
    // sorting algorithm, depending on the profiler. This makes it more obvious.
    sleep(Duration::from_millis(300));

    inputs.radix_sort_builder()
        .with_tuner(&MyTuner {})
        .sort();

    // A second run, for comparison
    sleep(Duration::from_millis(300));
    let time = Instant::now();
    inputs_2.radix_sort_builder()
        .with_tuner(&MyTuner {})
        .sort();

    let e = time.elapsed().as_millis();
    println!("Elapsed: {}ms", e);

    // Ensure nothing gets optimized out
    println!("{:?} {:?}", &inputs[0], &inputs_2[0]);
}