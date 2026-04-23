//! `mt_lsb_sort` is a multi-threaded Least-Significant Bit first radix sort. Multi-threading
//! is achieved by splitting the data into tiles, counting those tiles independently and
//! using the aggregated prefix sums to generate offsets in the output array for each thread
//! to write to for each radix.
//!
//! The output array is split by tile-counts sorted by radix and those output array chunks are
//! distributed to each thread. As they are distributed in the order in which they originally
//! appeared, the output remains stable just like a typical single-threaded LSB sort.
//!
//! ## Characteristics
//!
//!  * out-of-place
//!  * multi-threaded
//!  * stable
//!  * lsb-first
//!
//! ## Performance
//!
//! While this does not provide the best performance overall, the performance of this algorithm
//! is extremely stable and predictable. It gracefully degrades into a single-threaded LSB radix sort
//! when the tiles are reduced to 1 with only a little overhead.
//!
//! ## Optimizations
//!
//! This shares pretty much all optimizations implemented for LsbSort.
//!
//! ## `mt_oop_sort` Variant
//!
//! This variant uses the same algorithm as `mt_lsb_sort` but uses it in msb-first order.

use crate::radix_array::RadixArray;
use crate::radix_key::RadixKeyChecked;
use crate::sort_utils::*;
use crate::sorter::Sorter;
use rayon::prelude::*;
use std::mem::MaybeUninit;

type MaybeUninitChunk<'a, T> = [&'a mut [MaybeUninit<T>]; 256];

pub fn mt_lsb_sort<T>(
    src_bucket: &[T],
    mut dst_bucket: &mut [MaybeUninit<T>],
    tile_counts: &[RadixArray<usize>],
    tile_size: usize,
    level: usize,
) where
    T: RadixKeyChecked + Sized + Send + Copy + Sync,
{
    let tiles = tile_counts.len();

    let mut chunks: Box<[Option<&mut [MaybeUninit<T>]>]> = (0..=255u8)
        .flat_map(|b| tile_counts.iter().map(move |tc| tc.get(b)))
        .map(|c| dst_bucket.split_off_mut(..c))
        .collect();

    let collated_chunks: Box<[MaybeUninitChunk<T>]> = (0..tiles)
        .map(|tile| {
            std::array::from_fn(|bucket| {
                let idx = bucket * tiles + tile;
                chunks[idx].take().unwrap()
            })
        })
        .collect();

    collated_chunks
        .into_par_iter()
        .zip(src_bucket.par_chunks(tile_size))
        .for_each(|(buckets, bucket): (MaybeUninitChunk<T>, &[T])| {
            if bucket.is_empty() {
                return;
            }

            let mut offsets: RadixArray<usize> = RadixArray::new(0);
            let mut ends: RadixArray<usize> = RadixArray::new(0);

            for (i, b) in buckets.iter().enumerate() {
                if b.is_empty() {
                    continue;
                }

                *ends.get_mut(i as u8) = b.len() - 1;
            }

            let mut left = 0usize;
            let mut right = bucket.len() - 1;
            let pre = bucket.len() % 8;

            for _ in 0..pre {
                let b = bucket[right].get_level_checked(level);

                buckets[b as usize][ends.get(b)] = MaybeUninit::new(bucket[right]);
                *ends.get_mut(b) = ends.get(b).wrapping_sub(1);
                right = right.saturating_sub(1);
            }

            if pre == bucket.len() {
                return;
            }

            let end = (bucket.len() - pre) / 2;

            while left < end {
                let bl_0 = bucket[left].get_level_checked(level);
                let bl_1 = bucket[left + 1].get_level_checked(level);
                let bl_2 = bucket[left + 2].get_level_checked(level);
                let bl_3 = bucket[left + 3].get_level_checked(level);
                let br_0 = bucket[right].get_level_checked(level);
                let br_1 = bucket[right - 1].get_level_checked(level);
                let br_2 = bucket[right - 2].get_level_checked(level);
                let br_3 = bucket[right - 3].get_level_checked(level);

                buckets[bl_0 as usize][offsets.get(bl_0)] = MaybeUninit::new(bucket[left]);
                *offsets.get_mut(bl_0) += 1;
                buckets[br_0 as usize][ends.get(br_0)] = MaybeUninit::new(bucket[right]);
                *ends.get_mut(br_0) = ends.get(br_0).wrapping_sub(1);
                buckets[bl_1 as usize][offsets.get(bl_1)] = MaybeUninit::new(bucket[left + 1]);
                *offsets.get_mut(bl_1) += 1;
                buckets[br_1 as usize][ends.get(br_1)] = MaybeUninit::new(bucket[right - 1]);
                *ends.get_mut(br_1) = ends.get(br_1).wrapping_sub(1);
                buckets[bl_2 as usize][offsets.get(bl_2)] = MaybeUninit::new(bucket[left + 2]);
                *offsets.get_mut(bl_2) += 1;
                buckets[br_2 as usize][ends.get(br_2)] = MaybeUninit::new(bucket[right - 2]);
                *ends.get_mut(br_2) = ends.get(br_2).wrapping_sub(1);
                buckets[bl_3 as usize][offsets.get(bl_3)] = MaybeUninit::new(bucket[left + 3]);
                *offsets.get_mut(bl_3) += 1;
                buckets[br_3 as usize][ends.get(br_3)] = MaybeUninit::new(bucket[right - 3]);
                *ends.get_mut(br_3) = ends.get(br_3).wrapping_sub(1);

                left += 4;
                right = right.wrapping_sub(4);
            }
        });
}

impl<'a> Sorter<'a> {
    pub(crate) fn mt_lsb_sort_adapter<T>(
        &self,
        bucket: &mut [T],
        start_level: usize,
        end_level: usize,
        tile_size: usize,
    ) where
        T: RadixKeyChecked + Sized + Send + Copy + Sync,
    {
        if bucket.len() < 2 {
            return;
        }

        let mut tmp_bucket = Box::new_uninit_slice(bucket.len());
        let mut invert = false;

        for level in start_level..=end_level {
            let src_bucket: &[T];
            let dst_bucket: &mut [MaybeUninit<T>];

            (src_bucket, dst_bucket) = if invert {
                (
                    unsafe {
                        // SAFETY: Invert is only `true`
                        // after the first pass when tmp_bucket
                        // was entirely written
                        assume_init_ref(&tmp_bucket)
                    },
                    bucket_as_uninit_mut(bucket),
                )
            } else {
                (&*bucket, tmp_bucket.as_mut())
            };

            let (tile_counts, already_sorted) = get_tile_counts(src_bucket, tile_size, level);

            if already_sorted {
                continue;
            }

            mt_lsb_sort(src_bucket, dst_bucket, &tile_counts, tile_size, level);

            invert = !invert;
        }

        if invert {
            let tmp_bucket = unsafe {
                // SAFETY: tmp_bucket is guaranteed to have
                // been written if invert is true.
                tmp_bucket.assume_init()
            };

            bucket
                .par_chunks_mut(tile_size)
                .zip(tmp_bucket.par_chunks(tile_size))
                .for_each(|(chunk, tmp_chunk)| {
                    chunk.copy_from_slice(tmp_chunk);
                });
        }
    }

    pub(crate) fn mt_oop_sort_adapter<T>(
        &self,
        bucket: &mut [T],
        level: usize,
        counts: &RadixArray<usize>,
        tile_counts: &[RadixArray<usize>],
        tile_size: usize,
    ) where
        T: RadixKeyChecked + Sized + Send + Copy + Sync,
    {
        if bucket.len() <= 1 {
            return;
        }

        let mut tmp_bucket = Box::new_uninit_slice(bucket.len());
        mt_lsb_sort(bucket, &mut tmp_bucket, tile_counts, tile_size, level);

        let tmp_bucket = unsafe {
            // SAFETY: mt_lsb_sort
            // guarantees tmp_bucket is written
            // at least once.
            tmp_bucket.assume_init()
        };

        bucket
            .par_chunks_mut(tile_size)
            .zip(tmp_bucket.par_chunks(tile_size))
            .for_each(|(chunk, tmp_chunk)| {
                chunk.copy_from_slice(tmp_chunk);
            });

        drop(tmp_bucket);

        if level == 0 {
            return;
        }

        self.director(bucket, counts, level - 1);
    }
}

#[cfg(test)]
mod tests {
    use crate::RadixKey;
    use crate::sort_utils::{aggregate_tile_counts, get_tile_counts};
    use crate::sorter::Sorter;
    use crate::test_utils::{
        NumericTest, SingleAlgoTuner, sort_comparison_suite, sort_single_algorithm,
        validate_u32_patterns,
    };
    use crate::tuner::Algorithm;
    use rayon::current_num_threads;

    fn test_mt_lsb_sort_adapter<T>(shift: T)
    where
        T: NumericTest<T>,
    {
        let tuner = SingleAlgoTuner {
            algo: Algorithm::MtLsb,
        };

        let tuner_oop = SingleAlgoTuner {
            algo: Algorithm::MtOop,
        };

        sort_comparison_suite(shift, |inputs| {
            if inputs.len() == 0 {
                return;
            }

            let tile_size = inputs.len().div_ceil(current_num_threads());
            let sorter = Sorter::new(true, &tuner);

            sorter.mt_lsb_sort_adapter(inputs, 0, T::LEVELS - 1, tile_size);
        });

        sort_comparison_suite(shift, |inputs| {
            if inputs.len() == 0 {
                return;
            }

            let level = T::LEVELS - 1;
            let tile_size = inputs.len().div_ceil(current_num_threads());
            let sorter = Sorter::new(true, &tuner_oop);
            let (tile_counts, _) = get_tile_counts(inputs, tile_size, level);
            let counts = aggregate_tile_counts(&tile_counts);

            sorter.mt_oop_sort_adapter(inputs, T::LEVELS - 1, &counts, &tile_counts, tile_size);
        });
    }

    #[test]
    pub fn test_u8() {
        test_mt_lsb_sort_adapter(0u8);
    }

    #[test]
    pub fn test_u16() {
        test_mt_lsb_sort_adapter(8u16);
    }

    #[test]
    pub fn test_u32() {
        test_mt_lsb_sort_adapter(16u32);
    }

    #[test]
    pub fn test_u64() {
        test_mt_lsb_sort_adapter(32u64);
    }

    #[test]
    pub fn test_u128() {
        test_mt_lsb_sort_adapter(64u128);
    }

    #[test]
    pub fn test_usize() {
        test_mt_lsb_sort_adapter(32usize);
    }

    #[test]
    pub fn test_basic_integration() {
        sort_single_algorithm::<u32>(1_000_000, Algorithm::MtLsb);
    }

    #[test]
    pub fn test_regression_issue_5() {
        // Replicates https://github.com/Nessex/rdst/issues/5
        // MtLsb returns unsorted data when there is only 1 tile
        sort_single_algorithm::<u32>(400, Algorithm::MtLsb);
    }

    #[test]
    pub fn test_u32_patterns() {
        validate_u32_patterns(|inputs| {
            if inputs.len() == 0 {
                return;
            }

            let tuner = SingleAlgoTuner {
                algo: Algorithm::MtLsb,
            };
            let sorter = Sorter::new(true, &tuner);
            let tile_size = inputs.len().div_ceil(current_num_threads());

            sorter.mt_lsb_sort_adapter(inputs, 0, u32::LEVELS - 1, tile_size);
        });
    }
}
