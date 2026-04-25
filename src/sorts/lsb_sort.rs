//! `lsb_sort` is a Least-Significant Bit first radix sort.
//!
//! ## Characteristics
//!
//!  * out-of-place
//!  * stable
//!  * single-threaded
//!
//! ## Performance
//!
//! This sort is generally the fastest single-threaded algorithm implemented in this crate.
//!
//! ## Optimizations
//!
//! ### Ping-pong arrays
//!
//! As this is an out-of-place algorithm, we can save some copying by using the temporary array
//! as our input array, and our original array as our output array on odd runs of the algorithm.
//!
//! ### Level skipping
//!
//! When a level has all counts in one bucket (i.e. all values are equal), we can skip the level
//! entirely. This is done by checking counts rather than the actual data.
//!
//! ### Counting while sorting
//!
//! This is implemented in the underlying `out_of_place_sort`. While sorting, we also count the next
//! level to provide a small but significant performance boost. This is not a huge win as it removes
//! some caching benefits etc., but has been benchmarked at roughly 5-15% speedup.

use crate::radix_array::RadixArray;
use crate::sort_utils::{assume_init_ref, bucket_as_uninit_mut, get_counts};
use crate::sort_value::SortValue;
use crate::sorter::Sorter;
use crate::sorts::out_of_place_sort::route_out_of_place_sort;
use std::mem::MaybeUninit;

impl Sorter<'_> {
    pub(crate) fn lsb_sort_adapter<T>(
        &self,
        lr: bool,
        bucket: &mut [T],
        end_counts: &RadixArray<usize>,
        mut start_level: usize,
        end_level: usize,
    ) where
        T: SortValue,
    {
        if bucket.len() < 2 {
            return;
        }

        let mut tmp_bucket: Box<[MaybeUninit<T>]> = Box::new_uninit_slice(bucket.len());
        let mut invert = false;
        let mut next_counts: Option<RadixArray<usize>>;
        let mut level_counts: &RadixArray<usize>;

        // Set the initial level_counts ref
        // this allows us to reuse the already
        // calculated end_counts if we don't
        // find any unsorted levels before that.
        loop {
            if start_level == end_level {
                level_counts = end_counts;
                // Unlike the case below where we count
                // again and can check if the level is already
                // sorted... Here we assume that check has already
                // been done prior to calling this function.
                break;
            } else {
                // If we're not starting at the already counted end_level
                // we need to count again for the current level.
                let (counts, already_sorted) = get_counts(bucket, start_level);

                if !already_sorted {
                    next_counts = Some(counts);
                    level_counts = next_counts.as_ref().unwrap();
                    break;
                }

                start_level += 1;
            }
        }

        for level in start_level..=end_level {
            let (src_bucket, dst_bucket): (&[T], &mut [MaybeUninit<T>]) = if invert {
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
                (bucket, &mut tmp_bucket)
            };

            next_counts = route_out_of_place_sort(
                end_level != 0 && level < (end_level - 1),
                lr,
                src_bucket,
                dst_bucket,
                level_counts,
                level,
            );

            // The next level counts can only be:
            // 1. The counts we just calculated during route_out_of_place_sort
            // 2. The existing counts of the end_level
            level_counts = next_counts.as_ref().unwrap_or(end_counts);

            invert = !invert;
        }

        if invert {
            // We currently have our final output in tmp_bucket not bucket
            // so we need to copy it back across.
            unsafe {
                // SAFETY:
                // All values of tmp_bucket were written in the first iteration
                // of the loop above.
                bucket.copy_from_slice(assume_init_ref(&tmp_bucket));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::RadixKey;
    use crate::sort_utils::get_counts;
    use crate::sorter::Sorter;
    use crate::test_utils::{
        NumericTest, SingleAlgoTuner, sort_comparison_suite, sort_single_algorithm,
        validate_u32_patterns,
    };
    use crate::tuner::Algorithm;

    fn test_lsb_sort_adapter<T>(shift: T)
    where
        T: NumericTest<T>,
    {
        let tuner = SingleAlgoTuner {
            algo: Algorithm::Lsb,
        };

        let tuner_lr = SingleAlgoTuner {
            algo: Algorithm::LrLsb,
        };

        sort_comparison_suite(shift, |inputs| {
            let (counts, _) = get_counts(inputs, T::LEVELS - 1);
            let sorter = Sorter::new(true, &tuner);

            sorter.lsb_sort_adapter(false, inputs, &counts, 0, T::LEVELS - 1)
        });

        sort_comparison_suite(shift, |inputs| {
            let (counts, _) = get_counts(inputs, T::LEVELS - 1);
            let sorter = Sorter::new(true, &tuner_lr);

            sorter.lsb_sort_adapter(true, inputs, &counts, 0, T::LEVELS - 1);
        });
    }

    #[test]
    pub fn test_u8() {
        test_lsb_sort_adapter(0u8);
    }

    #[test]
    pub fn test_u16() {
        test_lsb_sort_adapter(8u16);
    }

    #[test]
    pub fn test_u32() {
        test_lsb_sort_adapter(16u32);
    }

    #[test]
    pub fn test_u64() {
        test_lsb_sort_adapter(32u64);
    }

    #[test]
    pub fn test_u128() {
        test_lsb_sort_adapter(64u128);
    }

    #[test]
    pub fn test_usize() {
        test_lsb_sort_adapter(32usize);
    }

    #[test]
    pub fn test_basic_integration() {
        sort_single_algorithm::<u32>(1_000_000, Algorithm::Lsb);
    }

    #[test]
    pub fn test_basic_integration_lr() {
        sort_single_algorithm::<u32>(1_000_000, Algorithm::LrLsb);
    }

    #[test]
    pub fn test_u32_patterns() {
        validate_u32_patterns(|inputs| {
            let tuner = SingleAlgoTuner {
                algo: Algorithm::Lsb,
            };

            let sorter = Sorter::new(true, &tuner);
            let (counts, _) = get_counts(inputs, u32::LEVELS - 1);

            sorter.lsb_sort_adapter(true, inputs, &counts, 0, u32::LEVELS - 1);
        });
    }
}
