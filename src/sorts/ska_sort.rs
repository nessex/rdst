use crate::director::director;
use crate::tuning_parameters::TuningParameters;
use crate::utils::*;
use crate::RadixKey;
use arbitrary_chunks::ArbitraryChunks;
use itertools::Itertools;

// Based upon (with modifications):
// https://probablydance.com/2016/12/27/i-wrote-a-faster-sorting-algorithm/
pub fn ska_sort<T>(bucket: &mut [T], counts: &[usize], level: usize)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let mut prefix_sums = get_prefix_sums(&counts);
    let mut end_offsets = prefix_sums.split_at(1).1.to_vec();
    end_offsets.push(end_offsets.last().unwrap() + counts.last().unwrap());

    let mut buckets: Vec<usize> = counts
        .iter()
        .enumerate()
        .sorted_unstable_by_key(|(_, c)| **c)
        .map(|(i, _)| i)
        .collect();

    let mut finished = 1;
    let mut finished_map = [false; 256];
    let largest = buckets.pop().unwrap();
    finished_map[largest] = true;
    buckets.reverse();

    while finished != 256 {
        for b in buckets.iter() {
            if finished_map[*b] {
                continue;
            } else if prefix_sums[*b] >= end_offsets[*b] {
                finished_map[*b] = true;
                finished += 1;
            }

            unsafe {
                for i in prefix_sums[*b]..end_offsets[*b] {
                    let new_b = bucket.get_unchecked(i).get_level(level) as usize;
                    bucket.swap(*prefix_sums.get_unchecked(new_b), i);
                    *prefix_sums.get_unchecked_mut(new_b) += 1;
                }
            }
        }
    }
}

#[allow(dead_code)]
pub fn ska_sort_adapter<T>(tuning: &TuningParameters, bucket: &mut [T], level: usize)
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

    let len = bucket.len();

    bucket
        .arbitrary_chunks_mut(counts.to_vec())
        .for_each(|chunk| director(tuning, chunk, len, level - 1));
}

#[cfg(test)]
mod tests {
    use crate::sorts::ska_sort::ska_sort_adapter;
    use crate::test_utils::sort_comparison_suite;
    use crate::tuning_parameters::TuningParameters;
    use crate::RadixKey;
    use nanorand::{RandomGen, WyRand};
    use std::fmt::Debug;
    use std::ops::{Shl, Shr};

    fn test_ska_sort_adapter<T>(shift: T)
    where
        T: RadixKey
            + Ord
            + RandomGen<WyRand>
            + Clone
            + Debug
            + Send
            + Sized
            + Copy
            + Sync
            + Shl<Output = T>
            + Shr<Output = T>,
    {
        let tuning = TuningParameters::new(T::LEVELS);
        sort_comparison_suite(shift, |inputs| {
            ska_sort_adapter(&tuning, inputs, T::LEVELS - 1)
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
