use crate::radix_array::RadixArray;
use crate::sort_utils::{aggregate_tile_counts, get_tile_counts};
use crate::sort_value::SortValue;
use crate::tuner::{Algorithm, TunerRef, TuningParams};
#[cfg(feature = "multi-threaded")]
use rayon::current_num_threads;
#[cfg(feature = "multi-threaded")]
use rayon::prelude::*;
use std::cmp::max;

pub struct Sorter<'tuner> {
    multi_threaded: bool,
    pub(crate) tuner: TunerRef<'tuner>,
}

impl<'tuner> Sorter<'tuner> {
    pub fn new(multi_threaded: bool, tuner: TunerRef<'tuner>) -> Self {
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
        counts: &RadixArray<usize>,
        #[allow(unused)] tile_counts: &[RadixArray<usize>],
        #[allow(unused)] tile_size: usize,
        algorithm: Algorithm,
    ) where
        T: SortValue,
    {
        match algorithm {
            #[cfg(feature = "multi-threaded")]
            Algorithm::Scanning => self.scanning_sort_adapter(bucket, counts, level),
            #[cfg(feature = "multi-threaded")]
            Algorithm::Recombinating => {
                self.recombinating_sort_adapter(bucket, counts, tile_counts, tile_size, level)
            }
            Algorithm::LrLsb => self.lsb_sort_adapter(true, bucket, counts, 0, level),
            Algorithm::Lsb => self.lsb_sort_adapter(false, bucket, counts, 0, level),
            Algorithm::Ska => self.ska_sort_adapter(bucket, counts, level),
            Algorithm::Comparative => self.comparative_sort(bucket, level),
            #[cfg(feature = "multi-threaded")]
            Algorithm::Regions => {
                self.regions_sort_adapter(bucket, counts, tile_counts, tile_size, level)
            }
            #[cfg(feature = "multi-threaded")]
            Algorithm::MtOop => {
                self.mt_oop_sort_adapter(bucket, level, counts, tile_counts, tile_size)
            }
            #[cfg(feature = "multi-threaded")]
            Algorithm::MtLsb => self.mt_lsb_sort_adapter(bucket, 0, level, tile_size),
        }
    }

    fn handle_chunk<T>(
        &self,
        chunk: &mut [T],
        level: usize,
        parent_len: Option<usize>,
        threads: usize,
    ) where
        T: SortValue,
    {
        if chunk.len() <= 1 {
            return;
        } else if chunk.len() <= 128 {
            self.comparative_sort(chunk, level);
            return;
        }

        let tp = TuningParams {
            threads,
            level,
            total_levels: T::LEVELS,
            input_len: chunk.len(),
            parent_len,
        };

        #[cfg(feature = "multi-threaded")]
        let use_tiles = self.multi_threaded && chunk.len() >= 260_000;

        #[cfg(not(feature = "multi-threaded"))]
        let use_tiles = false;

        let tile_size = if use_tiles {
            max(30_000, chunk.len().div_ceil(threads))
        } else {
            chunk.len()
        };

        let (tile_counts, already_sorted) = get_tile_counts(chunk, tile_size, level);
        let held_counts;
        let counts = if tile_counts.len() == 1 {
            &tile_counts[0]
        } else {
            held_counts = aggregate_tile_counts(&tile_counts);
            &held_counts
        };

        if already_sorted {
            if level != 0 {
                self.route(chunk, counts, level - 1);
            }

            return;
        }

        let algorithm = self.tuner.pick_algorithm(&tp, counts.inner());

        #[cfg(feature = "work_profiles")]
        println!("({}) PAR: {:?}", level, algorithm);

        self.run_sort(level, chunk, counts, &tile_counts, tile_size, algorithm);
    }

    #[inline]
    pub fn route_top_level<T>(&self, bucket: &mut [T])
    where
        T: SortValue,
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
    pub fn route_multi_threaded<T>(
        &self,
        mut bucket: &mut [T],
        counts: &RadixArray<usize>,
        level: usize,
    ) where
        T: SortValue,
    {
        let parent_len = Some(bucket.len());
        let threads = current_num_threads();

        counts
            .iter()
            .map(|c| bucket.split_off_mut(..c).unwrap())
            .par_bridge()
            .for_each(|chunk| self.handle_chunk(chunk, level, parent_len, threads));
    }

    #[inline]
    pub fn route_single_threaded<T>(
        &self,
        mut bucket: &mut [T],
        counts: &RadixArray<usize>,
        level: usize,
    ) where
        T: SortValue,
    {
        let parent_len = Some(bucket.len());
        let threads = 1;

        counts
            .iter()
            .map(|c| bucket.split_off_mut(..c).unwrap())
            .for_each(|chunk| self.handle_chunk(chunk, level, parent_len, threads));
    }

    #[inline]
    pub fn route<T>(&self, bucket: &mut [T], counts: &RadixArray<usize>, level: usize)
    where
        T: SortValue,
    {
        #[cfg(feature = "multi-threaded")]
        if self.multi_threaded {
            self.route_multi_threaded(bucket, counts, level);
            return;
        }

        self.route_single_threaded(bucket, counts, level);
    }
}
