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
use crate::radix_key::RadixKeyChecked;
use crate::sort_utils::*;
use crate::sorter::Sorter;
use crate::sorts::out_of_place_sort::{
    lr_out_of_place_sort, lr_out_of_place_sort_with_counts, out_of_place_sort,
    out_of_place_sort_with_counts,
};
use std::mem::{MaybeUninit, transmute};

impl<'a> Sorter<'a> {
    pub(crate) fn lsb_sort_adapter<T>(
        &self,
        lr: bool,
        bucket: &mut [T],
        last_counts: &RadixArray<usize>,
        start_level: usize,
        end_level: usize,
    ) where
        T: RadixKeyChecked + Sized + Send + Copy + Sync,
    {
        if bucket.len() < 2 {
            return;
        }

        let mut tmp_bucket = Box::new_uninit_slice(bucket.len());
        let mut invert = false;
        let mut next_counts = None;

        'outer: for level in start_level..=end_level {
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
                        transmute(bucket.as_mut())
                    },
                )
            } else {
                (bucket.as_ref(), &mut tmp_bucket)
            };
            let counts = if level == end_level {
                last_counts.clone()
            } else if let Some(next_counts) = next_counts.clone() {
                next_counts
            } else {
                let (counts, already_sorted) = get_counts(src_bucket, level);

                if already_sorted {
                    next_counts = None;
                    continue 'outer;
                }

                counts
            };

            for c in counts.iter() {
                if c == src_bucket.len() {
                    next_counts = None;
                    continue 'outer;
                } else if c > 0 {
                    break;
                }
            }

            let should_count = end_level != 0 && level < (end_level - 1);
            if !should_count {
                next_counts = None;
            }

            match (lr, should_count) {
                (true, true) => {
                    next_counts = Some(lr_out_of_place_sort_with_counts(
                        &src_bucket,
                        dst_bucket,
                        &counts,
                        level,
                    ))
                }
                (true, false) => lr_out_of_place_sort(&src_bucket, dst_bucket, &counts, level),
                (false, true) => {
                    next_counts = Some(out_of_place_sort_with_counts(
                        &src_bucket,
                        dst_bucket,
                        &counts,
                        level,
                    ))
                }
                (false, false) => out_of_place_sort(&src_bucket, dst_bucket, &counts, level),
            };

            invert = !invert;
        }

        if invert {
            unsafe {
                // SAFETY:
                // All values of tmp_bucket were written in the first iteration
                // of the loop above.
                bucket.copy_from_slice(tmp_bucket.assume_init_ref());
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
