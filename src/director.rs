use crate::sorts::comparative_sort::comparative_sort;
use crate::sorts::lsb_sort::lsb_sort_adapter;
use crate::sorts::mt_lsb_sort::{mt_lsb_sort_adapter, mt_oop_sort_adapter};
use crate::sorts::recombinating_sort::recombinating_sort_adapter;
use crate::sorts::regions_sort::regions_sort_adapter;
use crate::sorts::scanning_sort::scanning_sort_adapter;
use crate::sorts::ska_sort::ska_sort_adapter;
use crate::tuner::{Algorithm, Tuner, TuningParams};
use crate::utils::*;
use crate::RadixKey;
use arbitrary_chunks::ArbitraryChunks;
use rayon::current_num_threads;
use rayon::prelude::*;
use std::cmp::max;

#[inline]
pub fn run_sort<T>(
    tuner: &(dyn Tuner + Send + Sync),
    level: usize,
    bucket: &mut [T],
    counts: &[usize; 256],
    tile_counts: Option<Vec<[usize; 256]>>,
    tile_size: usize,
    algorithm: Algorithm,
) where
    T: RadixKey + Copy + Sized + Send + Sync,
{
    if let Some(tile_counts) = tile_counts {
        match algorithm {
            Algorithm::ScanningSort => scanning_sort_adapter(tuner, bucket, counts, level),
            Algorithm::RecombinatingSort => {
                recombinating_sort_adapter(tuner, bucket, counts, &tile_counts, tile_size, level)
            }
            Algorithm::LrLsbSort => lsb_sort_adapter(true, bucket, counts, 0, level),
            Algorithm::LsbSort => lsb_sort_adapter(false, bucket, counts, 0, level),
            Algorithm::SkaSort => ska_sort_adapter(tuner, bucket, counts, level),
            Algorithm::ComparativeSort => comparative_sort(bucket, level),
            Algorithm::RegionsSort => {
                regions_sort_adapter(tuner, bucket, counts, &tile_counts, tile_size, level)
            }
            Algorithm::MtOopSort => {
                mt_oop_sort_adapter(tuner, bucket, level, counts, &tile_counts, tile_size)
            }
            Algorithm::MtLsbSort => mt_lsb_sort_adapter(bucket, 0, level, tile_size),
        }
    } else {
        match algorithm {
            Algorithm::ScanningSort => scanning_sort_adapter(tuner, bucket, counts, level),
            Algorithm::LrLsbSort => lsb_sort_adapter(true, bucket, counts, 0, level),
            Algorithm::LsbSort => lsb_sort_adapter(false, bucket, counts, 0, level),
            Algorithm::SkaSort => ska_sort_adapter(tuner, bucket, counts, level),
            Algorithm::ComparativeSort => comparative_sort(bucket, level),
            e => panic!("Bad algorithm: {:?} for len: {}", e, bucket.len()),
        }
    }
}

#[inline]
pub fn top_level_director<T>(
    tuner: &(dyn Tuner + Send + Sync),
    bucket: &mut [T],
    parent_len: usize,
    level: usize,
) where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    if bucket.len() <= 1 {
        return;
    } else if bucket.len() <= 128 {
        comparative_sort(bucket, level);
        return;
    }

    let bucket_len = bucket.len();
    let threads = current_num_threads();
    let tile_size = max(30_000, cdiv(bucket.len(), threads));

    let tp = TuningParams {
        threads,
        level,
        total_levels: T::LEVELS,
        input_len: bucket_len,
        parent_len,
    };

    if bucket.len() <= tile_size {
        let counts = get_counts(bucket, level);
        let homogenous = is_homogenous_bucket(&counts);

        if homogenous {
            if level != 0 {
                director(tuner, bucket, counts.to_vec(), level - 1);
            }

            return;
        }

        let algorithm = tuner.pick_algorithm(&tp, &counts);

        #[cfg(feature = "work_profiles")]
        println!("({}) SOLO: {:?}", level, algorithm);

        run_sort(tuner, level, bucket, &counts, None, tile_size, algorithm);
    } else {
        let tile_counts = get_tile_counts(bucket, tile_size, level);
        let counts = aggregate_tile_counts(&tile_counts);
        let homogenous = is_homogenous_bucket(&counts);

        if homogenous {
            if level != 0 {
                director(tuner, bucket, counts.to_vec(), level - 1);
            }

            return;
        }

        let algorithm = tuner.pick_algorithm(&tp, &counts);

        #[cfg(feature = "work_profiles")]
        println!("({}) SOLO2: {:?}", level, algorithm);

        run_sort(
            tuner,
            level,
            bucket,
            &counts,
            Some(tile_counts),
            tile_size,
            algorithm,
        );
    }
}

#[inline]
pub fn director<T>(
    tuner: &(dyn Tuner + Send + Sync),
    bucket: &mut [T],
    counts: Vec<usize>,
    level: usize,
) where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let parent_len = bucket.len();
    let threads = current_num_threads();

    bucket
        .arbitrary_chunks_mut(counts)
        .par_bridge()
        .for_each(|chunk| {
            if chunk.len() <= 1 {
                return;
            } else if chunk.len() <= 128 {
                comparative_sort(chunk, level);
                return;
            }

            let tile_size = max(30_000, cdiv(chunk.len(), threads));
            let tp = TuningParams {
                threads,
                level,
                total_levels: T::LEVELS,
                input_len: chunk.len(),
                parent_len,
            };

            let tile_counts = if chunk.len() >= 260_000 {
                Some(get_tile_counts(chunk, tile_size, level))
            } else {
                None
            };

            let counts = if let Some(tile_counts) = &tile_counts {
                aggregate_tile_counts(tile_counts)
            } else {
                get_counts(chunk, level)
            };

            if chunk.len() >= 30_000 {
                let homogenous = is_homogenous_bucket(&counts);

                if homogenous {
                    if level != 0 {
                        director(tuner, chunk, counts.to_vec(), level - 1);
                    }

                    return;
                }
            }

            let algorithm = tuner.pick_algorithm(&tp, &counts);

            #[cfg(feature = "work_profiles")]
            println!("({}) PAR: {:?}", level, algorithm);

            run_sort(
                tuner,
                level,
                chunk,
                &counts,
                tile_counts,
                tile_size,
                algorithm,
            );
        });
}
