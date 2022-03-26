use crate::tuner::{Algorithm, Tuner, TuningParams};
use crate::utils::*;
use crate::RadixKey;
use arbitrary_chunks::ArbitraryChunks;
#[cfg(feature = "multi-threaded")]
use rayon::current_num_threads;
#[cfg(feature = "multi-threaded")]
use rayon::prelude::*;
use std::cmp::max;

pub struct Sorter<'a> {
    multi_threaded: bool,
    pub(crate) tuner: &'a (dyn Tuner + Send + Sync),
}

impl<'a> Sorter<'a> {
    pub fn new(multi_threaded: bool, tuner: &'a (dyn Tuner + Send + Sync)) -> Self {
        Self {
            multi_threaded,
            tuner,
        }
    }

    #[inline]
    fn run_sort<T>(
        &self,
        level: usize,
        bucket: &mut [T],
        counts: &[usize; 256],
        tile_counts: Option<Vec<[usize; 256]>>,
        #[allow(unused)]
        tile_size: usize,
        algorithm: Algorithm,
    ) where
        T: RadixKey + Copy + Sized + Send + Sync,
    {
        #[allow(unused)]
        if let Some(tile_counts) = tile_counts {
            match algorithm {
                #[cfg(feature = "multi-threaded")]
                Algorithm::Scanning => self.scanning_sort_adapter(bucket, counts, level),
                #[cfg(feature = "multi-threaded")]
                Algorithm::Recombinating => {
                    self.recombinating_sort_adapter(bucket, counts, &tile_counts, tile_size, level)
                }
                Algorithm::LrLsb => self.lsb_sort_adapter(true, bucket, counts, 0, level),
                Algorithm::Lsb => self.lsb_sort_adapter(false, bucket, counts, 0, level),
                Algorithm::Ska => self.ska_sort_adapter(bucket, counts, level),
                Algorithm::Comparative => self.comparative_sort(bucket, level),
                #[cfg(feature = "multi-threaded")]
                Algorithm::Regions => {
                    self.regions_sort_adapter(bucket, counts, &tile_counts, tile_size, level)
                }
                #[cfg(feature = "multi-threaded")]
                Algorithm::MtOop => {
                    self.mt_oop_sort_adapter(bucket, level, counts, &tile_counts, tile_size)
                }
                #[cfg(feature = "multi-threaded")]
                Algorithm::MtLsb => self.mt_lsb_sort_adapter(bucket, 0, level, tile_size),
            }
        } else {
            match algorithm {
                #[cfg(feature = "multi-threaded")]
                Algorithm::Scanning => self.scanning_sort_adapter(bucket, counts, level),
                Algorithm::LrLsb => self.lsb_sort_adapter(true, bucket, counts, 0, level),
                Algorithm::Lsb => self.lsb_sort_adapter(false, bucket, counts, 0, level),
                Algorithm::Ska => self.ska_sort_adapter(bucket, counts, level),
                Algorithm::Comparative => self.comparative_sort(bucket, level),
                #[cfg(feature = "multi-threaded")]
                e => panic!("Bad algorithm: {:?} for len: {}", e, bucket.len()),
            }
        }
    }

    fn handle_chunk<T>(
        &self,
        chunk: &mut [T],
        level: usize,
        parent_len: Option<usize>,
        threads: usize,
    ) where
        T: RadixKey + Sized + Send + Copy + Sync,
    {
        if chunk.len() <= 1 {
            return;
        } else if chunk.len() <= 128 {
            self.comparative_sort(chunk, level);
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

        let (mut tile_counts, already_sorted) = if
            cfg!(feature = "multi-threaded") &&
            self.multi_threaded &&
            chunk.len() >= 260_000
        {
            let (tile_counts, already_sorted) = get_tile_counts(chunk, tile_size, level);

            (Some(tile_counts), already_sorted)
        } else {
            (None, false)
        };

        let counts = if let Some(tile_counts) = &tile_counts {
            let counts = aggregate_tile_counts(tile_counts);

            if already_sorted {
                if level != 0 {
                    self.director(chunk, &counts, level - 1);
                }

                return;
            }

            counts
        } else {
            let (counts, already_sorted) = get_counts(chunk, level);
            if already_sorted {
                if level != 0 {
                    self.director(chunk, &counts, level - 1);
                }

                return;
            }

            counts
        };

        if chunk.len() >= 30_000 {
            let homogenous = is_homogenous_bucket(&counts);

            if homogenous {
                if level != 0 {
                    self.director(chunk, &counts, level - 1);
                }

                return;
            }
        }

        let algorithm = self.tuner.pick_algorithm(&tp, &counts);

        // Ensure tile_counts is always set when it is required
        if tile_counts.is_none() {
            tile_counts = match algorithm {
                #[cfg(feature = "multi-threaded")]
                Algorithm::MtOop | Algorithm::Recombinating | Algorithm::Regions => {
                    Some(vec![counts.clone()])
                }
                _ => None,
            };
        }

        #[cfg(feature = "work_profiles")]
        println!("({}) PAR: {:?}", level, algorithm);

        self.run_sort(level, chunk, &counts, tile_counts, tile_size, algorithm);
    }

    #[inline]
    pub fn top_level_director<T>(&self, bucket: &mut [T])
    where
        T: RadixKey + Sized + Send + Copy + Sync,
    {
        #[cfg(feature = "multi-threaded")]
        let threads = current_num_threads();

        #[cfg(not(feature = "multi-threaded"))]
        let threads = 1;

        let level = T::LEVELS - 1;

        self.handle_chunk(bucket, level, None, threads);
    }

    #[inline]
    #[cfg(feature = "multi-threaded")]
    pub fn multi_threaded_director<T>(&self, bucket: &mut [T], counts: &[usize; 256], level: usize)
    where
        T: RadixKey + Send + Copy + Sync,
    {
        let parent_len = Some(bucket.len());
        let threads = current_num_threads();

        bucket
            .arbitrary_chunks_mut(counts)
            .par_bridge()
            .for_each(|chunk| self.handle_chunk(chunk, level, parent_len, threads));
    }

    #[inline]
    pub fn single_threaded_director<T>(&self, bucket: &mut [T], counts: &[usize; 256], level: usize)
    where
        T: RadixKey + Send + Sync + Copy,
    {
        let parent_len = Some(bucket.len());
        let threads = 1;

        bucket
            .arbitrary_chunks_mut(counts)
            .for_each(|chunk| self.handle_chunk(chunk, level, parent_len, threads));
    }

    #[inline]
    pub fn director<T>(&self, bucket: &mut [T], counts: &[usize; 256], level: usize)
    where
        T: RadixKey + Send + Sync + Copy,
    {
        if cfg!(feature = "multi-threaded") && self.multi_threaded {
            #[cfg(feature = "multi-threaded")]
            self.multi_threaded_director(bucket, counts, level);
        } else {
            self.single_threaded_director(bucket, counts, level);
        }
    }
}
