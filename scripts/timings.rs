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

//! # timings
//!
//! This is used to run the sorting algorithm across a medley of inputs and output the results
//! as CSV. All numbers are in nanoseconds. It is intended to provide a quick way of getting a
//! performance cross-section of any given change to the algorithms in this crate.
//!
//! ## Usage
//!
//! You may need to tweak the command below for your own machine.
//!
//! ```
//! RUSTFLAGS='-C target-cpu=apple-m1 -C target-feature=+neon' ./timings.rs 1234 "Hello world"
//! ```
//!
//!  - `1234` is where you place the ID for your run. If you are just running a brief test this can be `N/A`, otherwise it should be something like a commit SHA that you can use to find the code for this run again.
//!  - `Hello world` is a description so you can be aware of what you were testing when running this experiment
//!  - `HEADERS=true` is an environment variable you can add to print a header row before the output

#![feature(string_remove_matches)]

use rayon::prelude::*;
use std::fmt::Debug;
use std::ops::{Shl, ShlAssign, Shr, ShrAssign};
use rdst::{RadixKey, RadixSort};
use std::time::Instant;
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

fn gen_exponential_input_set<T>(shift: T) -> Vec<Vec<T>>
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

fn print_row(data: Vec<String>) {
    let mut first = true;

    for mut o in data {
        if !first {
            print!(",");
        } else {
            first = false;
        }

        o.remove_matches("\"");

        if o.contains(" ") {
            print!("\"{}\"", o);
        } else {
            print!("{}", o);
        }
    }

    print!("\n");
}

fn bench<T>(inputs: Vec<Vec<T>>, name: &str, results: &mut Vec<String>, headers: &mut Vec<String>)
where
    T: RadixKey + Clone + Copy + Send + Sync,
{
    for i in inputs {
        if i.len() == 0 {
            continue;
        }
        headers.push(format!("{}_{}", name, i.len()).to_string());
        // Warmup
        let mut warmup = i.clone();
        warmup.radix_sort_unstable();
        drop(warmup);

        let mut scores = Vec::new();
        let n = 5usize;
        for _ in 0..n {
            let mut to_sort = i.clone();
            let time = Instant::now();
            to_sort.radix_sort_unstable();
            let elapsed = time.elapsed().as_nanos();
            let per_sec = ((i.len() as f64 / elapsed as f64) * 1_000_000_000f64) as u64;
            scores.push(per_sec);
            drop(to_sort);
        }

        scores.sort_unstable();
        let items_per_sec = scores[2];

        results.push(items_per_sec.to_string());
    }
}

fn main() {
    let print_headers: bool = std::env::var("HEADERS")
        .unwrap_or_else(|_| "false".to_string())
        .parse()
        .unwrap();
    let mut out: Vec<String> = std::env::args().skip(1).take(2).collect();
    assert_eq!(out.len(), 2);
    let mut headers = vec!["id".to_string(), "description".to_string()];

    let inputs = gen_exponential_input_set(0u32);
    bench(inputs, "u32", &mut out, &mut headers);

    let inputs = gen_exponential_input_set(16u32);
    bench(inputs, "u32_bimodal", &mut out, &mut headers);

    let inputs = gen_exponential_input_set(0u64);
    bench(inputs, "u64", &mut out, &mut headers);

    let inputs = gen_exponential_input_set(32u64);
    bench(inputs, "u64_bimodal", &mut out, &mut headers);

    let inputs = gen_exponential_input_set(0u128);
    bench(inputs, "u128", &mut out, &mut headers);

    let inputs = gen_exponential_input_set(64u128);
    bench(inputs, "u128_bimodal", &mut out, &mut headers);

    if print_headers {
        print_row(headers);
    }

    print_row(out);
}