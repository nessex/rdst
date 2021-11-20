use crate::director::director;
use crate::tuner::Tuner;
use crate::utils::*;
use crate::RadixKey;

// Based upon (with modifications):
// https://probablydance.com/2016/12/27/i-wrote-a-faster-sorting-algorithm/
pub fn ska_sort<T>(
    bucket: &mut [T],
    prefix_sums: &mut [usize; 256],
    end_offsets: &[usize; 256],
    level: usize,
) where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let mut finished = 1;
    let mut finished_map = [false; 256];
    let mut largest = 0;
    let mut largest_index = 0;

    for i in 0..256 {
        let rem = end_offsets[i] - prefix_sums[i];
        if rem == 0 {
            finished_map[i] = true;
            finished += 1;
        } else if rem > largest {
            largest = rem;
            largest_index = i;
        }
    }

    if largest == bucket.len() {
        // Already sorted
        return;
    }

    finished_map[largest_index] = true;

    while finished != 256 {
        for b in 0..256 {
            if finished_map[b] {
                continue;
            } else if prefix_sums[b] >= end_offsets[b] {
                finished_map[b] = true;
                finished += 1;
            }

            for i in prefix_sums[b]..end_offsets[b] {
                let new_b = bucket[i].get_level(level) as usize;
                bucket.swap(prefix_sums[new_b], i);
                prefix_sums[new_b] += 1;
            }
        }
    }
}

#[allow(dead_code)]
pub fn ska_sort_adapter<T>(
    tuner: &(dyn Tuner + Send + Sync),
    in_place: bool,
    bucket: &mut [T],
    counts: &[usize; 256],
    level: usize,
) where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let plateaus = detect_plateaus(bucket, level);
    let (mut prefix_sums, end_offsets) = apply_plateaus(bucket, counts, &plateaus);

    ska_sort(bucket, &mut prefix_sums, &end_offsets, level);

    if level == 0 {
        return;
    }

    director(tuner, in_place, bucket, counts.to_vec(), level - 1);
}

#[cfg(test)]
mod tests {
    use crate::sorts::ska_sort::ska_sort_adapter;
    use crate::test_utils::{sort_comparison_suite, NumericTest};
    use crate::tuner::DefaultTuner;
    use crate::utils::get_counts;

    fn test_ska_sort_adapter<T>(shift: T)
    where
        T: NumericTest<T>,
    {
        let tuner = DefaultTuner {};
        sort_comparison_suite(shift, |inputs| {
            let counts = get_counts(inputs, T::LEVELS - 1);
            ska_sort_adapter(&tuner, true, inputs, &counts, T::LEVELS - 1)
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
}
