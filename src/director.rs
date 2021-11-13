use crate::sorts::comparative_sort::comparative_sort;
use crate::sorts::lsb_sort::lsb_sort_adapter;
use crate::sorts::recombinating_sort::recombinating_sort_adapter;
use crate::sorts::regions_sort::regions_sort_adapter;
use crate::sorts::scanning_sort::scanning_sort_adapter;
use crate::sorts::ska_sort::ska_sort_adapter;
use crate::tuner::{Algorithm, Tuner, TuningParams};
use crate::RadixKey;
use arbitrary_chunks::ArbitraryChunks;
use rayon::current_num_threads;
use rayon::prelude::*;

pub fn director<T>(
    tuner: &(dyn Tuner + Send + Sync),
    in_place: bool,
    bucket: &mut [T],
    counts: Vec<usize>,
    level: usize,
) where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let depth = T::LEVELS - 1 - level;
    let len = bucket.len();
    let len_limit = ((bucket.len() / current_num_threads()) as f64 * 1.4) as usize;
    let mut long_chunks = Vec::new();
    let mut average_chunks = Vec::with_capacity(256);

    for chunk in bucket.arbitrary_chunks_mut(counts) {
        if chunk.len() > len_limit && depth <= 2 {
            long_chunks.push(chunk);
        } else {
            average_chunks.push(chunk);
        }
    }

    long_chunks.into_iter().for_each(|chunk| {
        let tp = TuningParams {
            threads: current_num_threads(),
            level,
            total_levels: T::LEVELS,
            input_len: chunk.len(),
            parent_len: len,
            in_place,
            serial: true,
        };

        match tuner.pick_algorithm(&tp) {
            Algorithm::ScanningSort => scanning_sort_adapter(tuner, tp.in_place, chunk, tp.level),
            Algorithm::RecombinatingSort => {
                recombinating_sort_adapter(tuner, tp.in_place, chunk, tp.level)
            }
            Algorithm::LsbSort => lsb_sort_adapter(chunk, 0, tp.level),
            Algorithm::SkaSort => ska_sort_adapter(tuner, tp.in_place, chunk, tp.level),
            Algorithm::ComparativeSort => comparative_sort(chunk, tp.level),
            Algorithm::RegionsSort => regions_sort_adapter(tuner, tp.in_place, chunk, tp.level),
        };
    });

    average_chunks.into_par_iter().for_each(|chunk| {
        let tp = TuningParams {
            threads: current_num_threads(),
            level,
            total_levels: T::LEVELS,
            input_len: chunk.len(),
            parent_len: len,
            in_place,
            serial: false,
        };

        match tuner.pick_algorithm(&tp) {
            Algorithm::ScanningSort => scanning_sort_adapter(tuner, tp.in_place, chunk, tp.level),
            Algorithm::RecombinatingSort => {
                recombinating_sort_adapter(tuner, tp.in_place, chunk, tp.level)
            }
            Algorithm::LsbSort => lsb_sort_adapter(chunk, 0, tp.level),
            Algorithm::SkaSort => ska_sort_adapter(tuner, tp.in_place, chunk, tp.level),
            Algorithm::ComparativeSort => comparative_sort(chunk, tp.level),
            Algorithm::RegionsSort => regions_sort_adapter(tuner, tp.in_place, chunk, tp.level),
        };
    });
}
