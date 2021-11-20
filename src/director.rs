use crate::sorts::comparative_sort::comparative_sort;
use crate::tuner::{Algorithm, Tuner, TuningParams};
use crate::utils::*;
use crate::RadixKey;
use arbitrary_chunks::ArbitraryChunks;
use rayon::current_num_threads;
use rayon::prelude::*;
use std::cmp::max;

struct Job<'a, T> {
    chunk: &'a mut [T],
    tile_size: usize,
    tile_counts: Option<Vec<[usize; 256]>>,
    counts: Option<[usize; 256]>,
    algorithm: Algorithm,
}

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

        //println!("SOLO L: {} ALG: {:?}", level, algorithm);

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

        //println!("SOLO 2 L: {} ALG: {:?}", level, algorithm);

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
    let mut serials: Vec<Job<T>> = Vec::new();
    let mut parallels: Vec<Job<T>> = Vec::new();

    bucket
        .arbitrary_chunks_mut(counts)
        .into_iter()
        .for_each(|chunk| {
            if chunk.len() <= 1 {
                return;
            } else if chunk.len() <= 128 {
                parallels.push(Job {
                    chunk,
                    tile_size: 0,
                    tile_counts: None,
                    counts: None,
                    algorithm: Algorithm::ComparativeSort,
                });
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

            let job = Job {
                chunk,
                tile_size,
                tile_counts,
                counts: Some(counts),
                algorithm,
            };

            match algorithm {
                Algorithm::SkaSort | Algorithm::ComparativeSort | Algorithm::LsbSort => {
                    parallels.push(job);
                }
                _ => serials.push(job),
            }
        });

    serials.into_iter().for_each(|job| {
        if let Some(counts) = job.counts {
            run_sort(
                tuner,
                in_place,
                level,
                job.chunk,
                &counts,
                job.tile_counts,
                job.tile_size,
                job.algorithm,
            );
        } else {
            comparative_sort(job.chunk, level);
        }
    });

    parallels.into_par_iter().for_each(|job| {
        if let Some(counts) = job.counts {
            run_sort(
                tuner,
                in_place,
                level,
                job.chunk,
                &counts,
                job.tile_counts,
                job.tile_size,
                job.algorithm,
            );
        } else {
            comparative_sort(job.chunk, level);
        }
    });
}
