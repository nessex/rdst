//! `ska_sort` is a single-threaded, in-place algorithm described by Malte Skarupke.
//!
//! <https://probablydance.com/2016/12/27/i-wrote-a-faster-sorting-algorithm/>
//!
//! This implementation isn't entirely faithful to the original, however it follows the general
//! principle of skipping over the largest output bucket and simply swapping the remaining buckets
//! until the entire thing is sorted.
//!
//! The in-place nature of this algorithm makes it very efficient memory-wise.
//!
//! ## Characteristics
//!
//!  * in-place
//!  * memory efficient
//!  * unstable
//!  * single-threaded
//!
//! ## Performance
//!
//! This is generally slower than `lsb_sort` for smaller types T or smaller input arrays. For larger
//! types or inputs, the memory efficiency of this algorithm can make it faster than `lsb_sort`.

use crate::radix_array::RadixArray;
use crate::radix_key::RadixKeyChecked;
use crate::sort_utils::*;
use crate::sorter::Sorter;
use partition::partition_index;

pub fn ska_sort<T>(
    bucket: &mut [T],
    prefix_sums: &mut RadixArray<usize>,
    end_offsets: &RadixArray<usize>,
    level: usize,
) where
    T: RadixKeyChecked + Sized + Send + Copy + Sync,
{
    let mut finished = 0usize;
    let mut finished_map = RadixArray::new(false);
    let mut largest = 0usize;
    let mut largest_index: u8 = 0;

    for i in 0..=255 {
        let rem = end_offsets.get(i) - prefix_sums.get(i);
        if rem == 0 {
            *finished_map.get_mut(i) = true;
            finished += 1;
        } else if rem > largest {
            largest = rem;
            largest_index = i;
        }
    }

    if largest == bucket.len() {
        // Already sorted
        return;
    } else if largest > (bucket.len() / 2) {
        // Partition in-place the largest chunk so we don't spend all our time
        // swapping things in and out that are already in the correct place.

        let offs = partition_index(
            &mut bucket[prefix_sums.get(largest_index)..end_offsets.get(largest_index)],
            |v| v.get_level_checked(level) == largest_index,
        );

        *prefix_sums.get_mut(largest_index) += offs;
    }

    if !finished_map.get(largest_index) {
        *finished_map.get_mut(largest_index) = true;
        finished += 1;
    }

    while finished != 256 {
        for b in 0..=255 {
            if finished_map.get(b) {
                continue;
            } else if prefix_sums.get(b) >= end_offsets.get(b) {
                *finished_map.get_mut(b) = true;
                finished += 1;
            }

            for i in prefix_sums.get(b)..end_offsets.get(b) {
                let new_b = bucket[i].get_level_checked(level);
                bucket.swap(prefix_sums.get(new_b), i);
                *prefix_sums.get_mut(new_b) += 1;
            }
        }
    }
}

impl Sorter<'_> {
    pub(crate) fn ska_sort_adapter<T>(
        &self,
        bucket: &mut [T],
        counts: &RadixArray<usize>,
        level: usize,
    ) where
        T: RadixKeyChecked + Sized + Send + Copy + Sync,
    {
        if bucket.len() < 2 {
            return;
        }

        let mut prefix_sums = get_prefix_sums(counts);
        let end_offsets = get_end_offsets(counts, &prefix_sums);

        ska_sort(bucket, &mut prefix_sums, &end_offsets, level);

        if level == 0 {
            return;
        }

        self.route(bucket, counts, level - 1);
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

    fn test_ska_sort_adapter<T>(shift: T)
    where
        T: NumericTest<T>,
    {
        let tuner = SingleAlgoTuner {
            algo: Algorithm::Ska,
        };

        sort_comparison_suite(shift, |inputs| {
            let (counts, _) = get_counts(inputs, T::LEVELS - 1);
            let sorter = Sorter::new(true, &tuner);

            sorter.ska_sort_adapter(inputs, &counts, T::LEVELS - 1);
        });
    }

    #[test]
    pub fn test_u8() {
        test_ska_sort_adapter(0u8);
    }

    #[test]
    pub fn test_u16() {
        test_ska_sort_adapter(8u16);
    }

    #[test]
    pub fn test_u32() {
        test_ska_sort_adapter(16u32);
    }

    #[test]
    pub fn test_u64() {
        test_ska_sort_adapter(32u64);
    }

    #[test]
    pub fn test_u128() {
        test_ska_sort_adapter(64u128);
    }

    #[test]
    pub fn test_usize() {
        test_ska_sort_adapter(32usize);
    }

    #[test]
    pub fn test_basic_integration() {
        sort_single_algorithm::<u32>(1_000_000, Algorithm::Ska);
    }

    #[test]
    pub fn test_u32_patterns() {
        let tuner = SingleAlgoTuner {
            algo: Algorithm::Ska,
        };

        validate_u32_patterns(|inputs| {
            let (counts, _) = get_counts(inputs, u32::LEVELS - 1);
            let sorter = Sorter::new(true, &tuner);

            sorter.ska_sort_adapter(inputs, &counts, u32::LEVELS - 1);
        });
    }
}
