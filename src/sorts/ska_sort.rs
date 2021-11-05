use crate::director::director;
use crate::utils::*;
use crate::RadixKey;
use crate::tuner::Tuner;

// Based upon (with modifications):
// https://probablydance.com/2016/12/27/i-wrote-a-faster-sorting-algorithm/
pub fn ska_sort<T>(bucket: &mut [T], counts: &[usize], level: usize)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let mut prefix_sums = get_prefix_sums(counts);
    let mut end_offsets = [0usize; 256];

    end_offsets[0..255].copy_from_slice(&prefix_sums[1..256]);
    end_offsets[255] = counts[255] + prefix_sums[255];

    let mut finished = 1;
    let mut finished_map = [false; 256];
    let mut largest = 0;
    let mut largest_index = 0;

    for (i, c) in counts.iter().enumerate() {
        if *c > largest {
            largest = *c;
            largest_index = i;
        }
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
pub fn ska_sort_adapter<T>(tuner: &(dyn Tuner + Send + Sync), in_place: bool, bucket: &mut [T], level: usize)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let (counts, level) = if let Some(s) = get_counts_and_level_descending(bucket, level, 0, false)
    {
        s
    } else {
        return;
    };

    ska_sort(bucket, &counts, level);

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

    fn test_ska_sort_adapter<T>(shift: T)
    where
        T: NumericTest<T>,
    {
        let tuner = DefaultTuner {};
        sort_comparison_suite(shift, |inputs| {
            ska_sort_adapter(&tuner, true, inputs, T::LEVELS - 1)
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
