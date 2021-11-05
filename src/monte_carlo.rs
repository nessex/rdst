//! # monte_carlo
//!
//! monte_carlo is intended for tuning diversions in the sorting algorithm.
//!
//! ## Usage
//!
//! ```
//! # Create a TSV file with data on each sorting algorithm
//! RUSTFLAGS="-C opt-level=3 -C target-cpu=native -C target-feature=+neon" cargo +nightly run --release --features=tuning | tee -a monte-carlo.tsv
//!
//! # Render the plot for analysis
//! gnuplot -p monte-carlo.gnuplot
//! ```
//!
//! Currently the random data does not quickly provide a good sample of all levels. You should manually
//! adjust the input_size range +/- a digit or two to get good coverage quickly. This will eventually be automated.
//!
//! In addition, you may want to manually override the data_type to just be a single data type for cleaner results.
//!
//! ## Results
//!
//! So far this has produced a mediocre tuning, slightly more balanced, but slower overall than the hand-tuning
//! performed previously. I expect this is a problem in how we deal with multi-tasking as ska sort in particular
//! makes a big difference when many operations are ongoing (I'm not sure what specifically causes this), but
//! not so much in isolation.
//!
//! The next step for improving this will be improving the framework upon which tuning results can be applied.
//! By creating a framework that allows tuning based on the total number of levels + current level, as well as
//! the number of threads, length of input, size of type etc. I expect some small wins can be found.

#![feature(int_log)]

use std::any::type_name;
use std::time::{Duration, Instant};
use bit_vec::BitVec;
use nanorand::{RandomGen, Rng, WyRand};
use rayon::prelude::*;

mod director;
mod utils;
mod sorts;
mod sort_manager;
mod tuner;
mod radix_sort;
mod radix_key;
mod radix_key_impl;

use radix_key::RadixKey;
use rdst::tuner::Algorithm;
use crate::sorts::lsb_sort::lsb_sort;
use crate::utils::*;
use crate::sorts::regions_sort::regions_sort;
use crate::sorts::recombinating_sort::recombinating_sort;
use crate::sorts::comparative_sort::comparative_sort;
use crate::sorts::ska_sort::ska_sort;
use crate::sorts::scanning_sort::scanning_sort;

fn get_data<T>(len: usize) -> Vec<T>
where
    T: RadixKey + RandomGen<WyRand> + Send + Sync + Copy
{
    let mut rng = WyRand::new();
    let mut data: Vec<T> = Vec::with_capacity(len);

    for _ in 0..len {
        data.push(rng.generate());
    }

    data
}

// get_seq generates a space covering sequence of integers from 0 to `end`.
// This sequence is intended to slowly over time fill in gaps in the tested integers to get uniform
// and complete coverage.
fn get_seq(end: usize) -> Vec<usize> {
    let mut out: Vec<usize> = Vec::new();
    let final_pow = end.log2();
    let mut inserted = BitVec::from_elem(end + 1, false);

    eprintln!("Generating sequence");

    let _ = out.push(0);
    let _ = out.push(end);

    // Insert in order based upon distance
    for pow in 1..final_pow {
        let denominator = 2_u64 << pow;

        for numerator in 1..denominator {
            let new_val = (end as f64 * (numerator as f64 / denominator as f64)) as usize;

            if inserted.get(new_val).unwrap() {
                continue;
            }

            inserted.set(new_val, true);

            out.push(new_val);
        }
    }

    // Fill in the remaining gaps
    for i in 1..end {
        if !inserted.get(i).unwrap() {
            out.push(i);
        }
    }

    eprintln!("Finished generating sequence");

    out
}

fn sort<T>(algorithm: Algorithm, data: &[T], level: usize, serial: bool) -> Duration
where
    T: RadixKey + RandomGen<WyRand> + Send + Sync + Copy
{
    let algo = |algorithm| {
        let mut data = data.to_vec();
        match algorithm {
            Algorithm::ScanningSort => {
                let counts = par_get_counts(&data, level);
                scanning_sort(&mut data, &counts, level)
            },
            Algorithm::RecombinatingSort => {
                let _ = recombinating_sort(&mut data, level);
            }
            Algorithm::ComparativeSort => comparative_sort(&mut data, level),
            Algorithm::LsbSort => {
                let counts = get_counts(&data, level);
                let mut tmp_bucket = get_tmp_bucket::<T>(data.len());
                lsb_sort(&mut data, &mut tmp_bucket, &counts, level);
            }
            Algorithm::RegionsSort => {
                let _ = regions_sort(&mut data, level);
            }
            Algorithm::SkaSort => {
                let counts = get_counts(&data, level);
                ska_sort(&mut data, &counts, level)
            },
        };
    };

    let start = Instant::now();
    if serial {
        algo(algorithm);
    } else {
        (0..256)
            .into_par_iter()
            .for_each(|_| {
                algo(algorithm);
            })
    }

    start.elapsed()
}


fn main() {
    let input_size: usize = 200_000_000;
    let serial = true;
    let level = 0;
    type DataType = u32;
    let data = get_data::<DataType>(input_size);
    for i in get_seq(input_size) {
        let mut best_time = None;
        let mut best_algo = None;

        for algo in 0..=5usize {
            let slice = &data[0..i];

            let algorithm = match algo {
                0 => Algorithm::ScanningSort,
                1 => Algorithm::RecombinatingSort,
                2 => Algorithm::LsbSort,
                3 => Algorithm::ComparativeSort,
                4 => Algorithm::RegionsSort,
                5 => Algorithm::SkaSort,
                _ => panic!(),
            };

            // Skip these as they are way out of their reasonable ranges
            if (algorithm == Algorithm::ComparativeSort && input_size > 1_000) ||
                (algorithm == Algorithm::LsbSort && input_size > 10_000_000) ||
                (algorithm == Algorithm::SkaSort && input_size > 10_000_000) {
                continue;
            }

            let time = sort::<DataType>(algorithm, &slice, level, serial);

            if best_time.is_none() || time < best_time.unwrap() {
                best_time = Some(time);
                best_algo = Some(algorithm);
            }
        }

        println!("{:?}\t{:?}\t{:?}\t{:?}", i, best_time.unwrap().as_nanos(), best_algo.unwrap(), type_name::<DataType>());
    }
}
