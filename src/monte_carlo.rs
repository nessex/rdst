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

use std::time::{Duration, Instant};
use nanorand::{RandomGen, Rng, WyRand};
use rayon::current_num_threads;
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

#[derive(Debug)]
enum DataType {
    U8, U16, U32, U64, U128,
    I8, I16, I32, I64, I128,
    F32, F64
}

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

fn sort<T>(algorithm: Algorithm, input_size: usize, level: usize, serial: bool) -> Duration
where
    T: RadixKey + RandomGen<WyRand> + Send + Sync + Copy
{

    let algo = |algorithm| {
        let mut data: Vec<T> = get_data(input_size);
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
    let mut rng = WyRand::new();
    loop {
        let input_size: usize = rng.generate_range(2..=10_000_000);
        let serial = true;
        let data_type = match rng.generate_range(0..=11) {
            0 => DataType::U8,
            1 => DataType::U16,
            2 => DataType::U32,
            3 => DataType::U64,
            4 => DataType::U128,
            5 => DataType::I8,
            6 => DataType::I16,
            7 => DataType::I32,
            8 => DataType::I64,
            9 => DataType::I128,
            10 => DataType::F32,
            11 => DataType::F64,
            _ => panic!(),
        };

        let data_type = DataType::U32;

        let level = match data_type {
            DataType::U8 => 0,
            DataType::U16 => rng.generate_range(0..=1),
            DataType::U32 => rng.generate_range(0..=3),
            DataType::U64 => rng.generate_range(0..=7),
            DataType::U128 => rng.generate_range(0..=15),
            DataType::I8 => 0,
            DataType::I16 => rng.generate_range(0..=1),
            DataType::I32 => rng.generate_range(0..=3),
            DataType::I64 => rng.generate_range(0..=7),
            DataType::I128 => rng.generate_range(0..=15),
            DataType::F32 => rng.generate_range(0..=3),
            DataType::F64 => rng.generate_range(0..=7),
        };

        let mut best_time = None;
        let mut best_algo = None;

        for i in 3..=5usize {
            let algorithm = match i {
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

            let time = match data_type {
                DataType::U8 => sort::<u8>(algorithm, input_size, level, serial),
                DataType::U16 => sort::<u16>(algorithm, input_size, level, serial),
                DataType::U32 => sort::<u32>(algorithm, input_size, level, serial),
                DataType::U64 => sort::<u64>(algorithm, input_size, level, serial),
                DataType::U128 => sort::<u128>(algorithm, input_size, level, serial),
                DataType::I8 => sort::<i8>(algorithm, input_size, level, serial),
                DataType::I16 => sort::<i16>(algorithm, input_size, level, serial),
                DataType::I32 => sort::<i32>(algorithm, input_size, level, serial),
                DataType::I64 => sort::<i64>(algorithm, input_size, level, serial),
                DataType::I128 => sort::<i128>(algorithm, input_size, level, serial),
                DataType::F32 => sort::<f32>(algorithm, input_size, level, serial),
                DataType::F64 => sort::<f64>(algorithm, input_size, level, serial),
            };

            if best_time.is_none() || time < best_time.unwrap() {
                best_time = Some(time);
                best_algo = Some(algorithm);
            }
        }

        println!("{:?}\t{:?}\t{:?}\t{:?}", input_size, best_time.unwrap().as_nanos(), best_algo.unwrap(), data_type);
    }
}
