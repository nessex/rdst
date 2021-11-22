use crate::sorts::comparative_sort::comparative_sort;
use crate::tuner::{Tuner, TuningParams};
use crate::utils::*;
use crate::RadixKey;
use arbitrary_chunks::ArbitraryChunks;
use rayon::current_num_threads;
use rayon::prelude::*;
use std::cmp::max;

#[inline]
pub fn single_director<T>(
    tuner: &(dyn Tuner + Send + Sync),
    in_place: bool,
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
        in_place,
    };

    if bucket.len() <= tile_size {
        let counts = get_counts(bucket, level);
        let homogenous = is_homogenous_bucket(&counts);

        if homogenous {
            if level != 0 {
                director(tuner, in_place, bucket, counts.to_vec(), level - 1);
            }

            return;
        }

        let algorithm = tuner.pick_algorithm(&tp, &counts);

        #[cfg(feature = "work_profiles")]
        println!("({}) SOLO: {:?}", level, algorithm);

        run_sort(
            tuner, in_place, level, bucket, &counts, None, tile_size, algorithm,
        );
    } else {
        let tile_counts = get_tile_counts(bucket, tile_size, level);
        let counts = aggregate_tile_counts(&tile_counts);
        let homogenous = is_homogenous_bucket(&counts);

        if homogenous {
            if level != 0 {
                director(tuner, in_place, bucket, counts.to_vec(), level - 1);
            }

            return;
        }

        let algorithm = tuner.pick_algorithm(&tp, &counts);

        #[cfg(feature = "work_profiles")]
        println!("({}) SOLO2: {:?}", level, algorithm);

        run_sort(
            tuner,
            in_place,
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
    in_place: bool,
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
                in_place,
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
                        director(tuner, in_place, chunk, counts.to_vec(), level - 1);
                    }

                    return;
                }
            }

            let algorithm = tuner.pick_algorithm(&tp, &counts);

            #[cfg(feature = "work_profiles")]
            println!("({}) PAR: {:?}", level, algorithm);

            run_sort(
                tuner,
                in_place,
                level,
                chunk,
                &counts,
                tile_counts,
                tile_size,
                algorithm,
            );
        });
}
