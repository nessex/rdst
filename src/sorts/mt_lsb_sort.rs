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
use arbitrary_chunks::ArbitraryChunks;
use rayon::prelude::*;
use std::mem::MaybeUninit;
use std::mem::transmute;

pub fn mt_lsb_sort<T>(
    src_bucket: &[T],
    dst_bucket: &mut [MaybeUninit<T>],
    tile_counts: &[RadixArray<usize>],
    tile_size: usize,
    level: usize,
) where
    T: RadixKeyChecked + Sized + Send + Copy + Sync,
{
    let tiles = tile_counts.len();
    let mut minor_counts: Box<[MaybeUninit<usize>]> = Box::new_uninit_slice(256 * tiles);

    for b in 0..=255u8 {
        for (i, tile) in tile_counts.iter().enumerate() {
            minor_counts[b as usize * tiles + i] = MaybeUninit::new(tile.get(b));
        }
    }

    let minor_counts = unsafe {
        debug_assert!({
            // XXX: This must exactly mirror the logic above
            // The purpose of this assertion in debug mode is to verify
            // that minor_counts is _entirely_ written before we
            // call assume_init().
            let mut mirror: Vec<bool> = vec![false; 256 * tiles];
            for b in 0..256 {
                for (i, _) in tile_counts.iter().enumerate() {
                    mirror[b * tiles + i] = true;
                }
            }

            let mut all_true = true;
            for v in mirror {
                if !v {
                    all_true = false;
                }
            }
            all_true
        });

        minor_counts.assume_init()
    };

    let mut collated_chunks: Box<[MaybeUninit<[MaybeUninit<&mut [MaybeUninit<T>]>; 256]>]> =
        Box::new_uninit_slice(tiles);

    for chunk in collated_chunks.iter_mut() {
        let arr: [MaybeUninit<&mut [MaybeUninit<T>]>; 256] = MaybeUninit::uninit().into();
        *chunk = MaybeUninit::new(arr);
    }

    let mut collated_chunks: Box<[[MaybeUninit<&mut [MaybeUninit<T>]>; 256]]> = unsafe {
        // SAFETY: All chunk arrays have been written
        // directly above.
        collated_chunks.assume_init()
    };

    let mut chunks = dst_bucket.arbitrary_chunks_mut(&minor_counts);
    for b in 0..256 {
        for coll_chunk in collated_chunks.iter_mut() {
            unsafe {
                // SAFETY:
                // We are initializing values here without reading.
                *coll_chunk[b].assume_init_mut() = chunks.next().unwrap();
            }
        }
    }

    let collated_chunks: Box<[[&mut [T]; 256]]> = unsafe {
        // SAFETY: Box<[[MaybeUninit<&mut [MaybeUninit<T>]>; 256]]> and Box<[[&mut [T]; 256]]>
        // have the same layout. Every value has been written above.
        transmute(collated_chunks)
    };

    collated_chunks
        .into_par_iter()
        .zip(src_bucket.par_chunks(tile_size))
        .for_each(|(buckets, bucket)| {
            if bucket.is_empty() {
                return;
            }

            let mut offsets = [0usize; 256];
            let mut ends = [0usize; 256];

            for (i, b) in buckets.iter().enumerate() {
                if b.is_empty() {
                    continue;
                }

                ends[i] = b.len() - 1;
            }

            let mut left = 0;
            let mut right = bucket.len() - 1;
            let pre = bucket.len() % 8;

            for _ in 0..pre {
                let b = bucket[right].get_level_checked(level) as usize;

                buckets[b][ends[b]] = bucket[right];
                ends[b] = ends[b].wrapping_sub(1);
                right = right.saturating_sub(1);
            }

            if pre == bucket.len() {
                return;
            }

            let end = (bucket.len() - pre) / 2;

            while left < end {
                let bl_0 = bucket[left].get_level_checked(level) as usize;
                let bl_1 = bucket[left + 1].get_level_checked(level) as usize;
                let bl_2 = bucket[left + 2].get_level_checked(level) as usize;
                let bl_3 = bucket[left + 3].get_level_checked(level) as usize;
                let br_0 = bucket[right].get_level_checked(level) as usize;
                let br_1 = bucket[right - 1].get_level_checked(level) as usize;
                let br_2 = bucket[right - 2].get_level_checked(level) as usize;
                let br_3 = bucket[right - 3].get_level_checked(level) as usize;

                buckets[bl_0][offsets[bl_0]] = bucket[left];
                offsets[bl_0] += 1;
                buckets[br_0][ends[br_0]] = bucket[right];
                ends[br_0] = ends[br_0].wrapping_sub(1);
                buckets[bl_1][offsets[bl_1]] = bucket[left + 1];
                offsets[bl_1] += 1;
                buckets[br_1][ends[br_1]] = bucket[right - 1];
                ends[br_1] = ends[br_1].wrapping_sub(1);
                buckets[bl_2][offsets[bl_2]] = bucket[left + 2];
                offsets[bl_2] += 1;
                buckets[br_2][ends[br_2]] = bucket[right - 2];
                ends[br_2] = ends[br_2].wrapping_sub(1);
                buckets[bl_3][offsets[bl_3]] = bucket[left + 3];
                offsets[bl_3] += 1;
                buckets[br_3][ends[br_3]] = bucket[right - 3];
                ends[br_3] = ends[br_3].wrapping_sub(1);

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
            let (src_bucket, dst_bucket): (&[T], &mut [MaybeUninit<T>]) = if invert {
                (
                    unsafe {
                        // SAFETY: Invert is only `true`
                        // after the first pass when tmp_bucket
                        // is entirely written
                        tmp_bucket.assume_init_ref()
                    },
                    unsafe {
                        // SAFETY: We are converting from
                        // &mut [T] to &mut [MaybeUninit<T>]
                        // [T] and [MaybeUninit<T>] have the same
                        // layout.
                        transmute::<&mut [T], &mut [std::mem::MaybeUninit<T>]>(bucket)
                    },
                )
            } else {
                (bucket, &mut tmp_bucket)
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
