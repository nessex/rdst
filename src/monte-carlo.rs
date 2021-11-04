use std::time::{Duration, Instant};
use nanorand::{RandomGen, Rng, WyRand};

mod director;
mod utils;
mod sorts;
mod sort_manager;
mod tuning_parameters;
mod radix_sort;
mod radix_key;
mod radix_key_impl;

use radix_key::RadixKey;
use crate::sorts::lsb_radix_sort::lsb_radix_sort;
use crate::utils::*;
use crate::sorts::regions_sort::regions_sort;
use crate::sorts::recombinating_sort::recombinating_sort;
use crate::sorts::comparative_sort::comparative_sort;
use crate::sorts::ska_sort::ska_sort;
use crate::sorts::scanning_sort::scanning_sort;
use crate::tuning_parameters::TuningParameters;

#[derive(Debug)]
enum DataType {
    U8, U16, U32, U64, U128,
    I8, I16, I32, I64, I128,
    F32, F64
}

#[derive(Debug, Eq, PartialEq)]
enum Algorithm {
    ScanningSort,
    RecombinatingSort,
    ComparativeSort,
    LsbSort,
    RegionsSort,
    SkaSort,
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

fn sort<T>(algorithm: &Algorithm, input_size: usize, level: usize) -> Duration
where
    T: RadixKey + RandomGen<WyRand> + Send + Sync + Copy
{
    let tuning = TuningParameters::new(0);
    let mut data: Vec<T> = get_data(input_size);

    let start = Instant::now();
    match algorithm {
        Algorithm::ScanningSort => {
            let counts = par_get_counts(&data, level);
            scanning_sort(&tuning, &mut data, &counts, level)
        },
        Algorithm::RecombinatingSort => {
            let _ = recombinating_sort(&tuning, &mut data, level);
        }
        Algorithm::ComparativeSort => comparative_sort(&mut data, level),
        Algorithm::LsbSort => {
            let counts = get_counts(&data, level);
            let mut tmp_bucket = get_tmp_bucket::<T>(data.len());
            lsb_radix_sort(&mut data, &mut tmp_bucket, &counts, level);
        }
        Algorithm::RegionsSort => {
            let _ = regions_sort(&tuning, &mut data, level);
        }
        Algorithm::SkaSort => {
            let counts = get_counts(&data, level);
            ska_sort(&mut data, &counts, level)
        },
    };

    start.elapsed()
}


fn main() {
    let mut rng = WyRand::new();
    loop {
        let input_size: usize = rng.generate_range(2..=100_000);
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

        let data_type = DataType::U64;

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

        for i in 0..=5usize {
            let algorithm = match i {
                0 => Algorithm::ScanningSort,
                1 => Algorithm::RecombinatingSort,
                2 => Algorithm::ComparativeSort,
                3 => Algorithm::LsbSort,
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
                DataType::U8 => sort::<u8>(&algorithm, input_size, level),
                DataType::U16 => sort::<u16>(&algorithm, input_size, level),
                DataType::U32 => sort::<u32>(&algorithm, input_size, level),
                DataType::U64 => sort::<u64>(&algorithm, input_size, level),
                DataType::U128 => sort::<u128>(&algorithm, input_size, level),
                DataType::I8 => sort::<i8>(&algorithm, input_size, level),
                DataType::I16 => sort::<i16>(&algorithm, input_size, level),
                DataType::I32 => sort::<i32>(&algorithm, input_size, level),
                DataType::I64 => sort::<i64>(&algorithm, input_size, level),
                DataType::I128 => sort::<i128>(&algorithm, input_size, level),
                DataType::F32 => sort::<f32>(&algorithm, input_size, level),
                DataType::F64 => sort::<f64>(&algorithm, input_size, level),
            };

            if best_time.is_none() || time < best_time.unwrap() {
                best_time = Some(time);
                best_algo = Some(algorithm);
            }
        }

        println!("{:?}\t{:?}\t{:?}\t{:?}", input_size, best_time.unwrap().as_nanos(), best_algo.unwrap(), data_type);
    }
}