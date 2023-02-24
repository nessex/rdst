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

use crate::sorter::Sorter;
use crate::sorts::out_of_place_sort::{
    lr_out_of_place_sort, lr_out_of_place_sort_with_counts, out_of_place_sort,
    out_of_place_sort_with_counts,
};
use crate::utils::*;
use crate::RadixKey;

impl<'a> Sorter<'a> {
    pub(crate) fn lsb_sort_adapter<T>(
        &self,
        lr: bool,
        bucket: &mut [T],
        last_counts: &[usize; 256],
        start_level: usize,
        end_level: usize,
    ) where
        T: RadixKey + Sized + Send + Copy + Sync,
    {
        if bucket.len() < 2 {
            return;
        }

        let mut tmp_bucket = get_tmp_bucket(bucket.len());
        let levels: Vec<usize> = (start_level..=end_level).collect();
        let mut invert = false;
        let mut next_counts = None;

        'outer: for level in levels {
            let counts = if level == end_level {
                *last_counts
            } else if let Some(next_counts) = next_counts {
                next_counts
            } else {
                let (counts, already_sorted) = get_counts(bucket, level);
                if already_sorted {
                    continue 'outer;
                }

                counts
            };

            for c in counts.iter() {
                if *c == bucket.len() {
                    continue 'outer;
                } else if *c > 0 {
                    break;
                }
            }

            let should_count = level < (end_level - 1);
            if !should_count {
                next_counts = None;
            }

            match (lr, invert, should_count) {
                (true, true, true) => {
                    next_counts = Some(lr_out_of_place_sort_with_counts(
                        &tmp_bucket,
                        bucket,
                        &counts,
                        level,
                    ))
                }
                (true, true, false) => lr_out_of_place_sort(&tmp_bucket, bucket, &counts, level),
                (true, false, true) => {
                    next_counts = Some(lr_out_of_place_sort_with_counts(
                        bucket,
                        &mut tmp_bucket,
                        &counts,
                        level,
                    ))
                }
                (true, false, false) => {
                    lr_out_of_place_sort(bucket, &mut tmp_bucket, &counts, level)
                }
                (false, true, true) => {
                    next_counts = Some(out_of_place_sort_with_counts(
                        &tmp_bucket,
                        bucket,
                        &counts,
                        level,
                    ))
                }
                (false, true, false) => out_of_place_sort(&tmp_bucket, bucket, &counts, level),
                (false, false, true) => {
                    next_counts = Some(out_of_place_sort_with_counts(
                        bucket,
                        &mut tmp_bucket,
                        &counts,
                        level,
                    ))
                }
                (false, false, false) => out_of_place_sort(bucket, &mut tmp_bucket, &counts, level),
            };

            invert = !invert;
        }

        if invert {
            bucket.copy_from_slice(&tmp_bucket);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::sorter::Sorter;
    use crate::tuner::Algorithm;
    use crate::tuners::StandardTuner;
    use crate::utils::get_counts;
    use crate::utils::test_utils::{sort_comparison_suite, sort_single_algorithm, NumericTest};

    fn test_lsb_sort_adapter<T>(shift: T)
    where
        T: NumericTest<T>,
    {
        let sorter = Sorter::new(true, &StandardTuner);

        sort_comparison_suite(shift, |inputs| {
            let (counts, _) = get_counts(inputs, T::LEVELS - 1);

            sorter.lsb_sort_adapter(false, inputs, &counts, 0, T::LEVELS - 1)
        });

        sort_comparison_suite(shift, |inputs| {
            let (counts, _) = get_counts(inputs, T::LEVELS - 1);

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
}
