use crate::lsb_radix_sort::lsb_radix_sort_adapter;
use crate::tuning_parameters::TuningParameters;
use crate::utils::{get_counts, get_prefix_sums};
use crate::RadixKey;
use arbitrary_chunks::ArbitraryChunks;
use itertools::Itertools;

// Based upon (with modifications):
// https://probablydance.com/2016/12/27/i-wrote-a-faster-sorting-algorithm/
pub fn msb_ska_sort<T>(tuning: &TuningParameters, bucket: &mut [T], level: usize)
where
    T: RadixKey + Sized + Ord + Send + Copy + Sync,
{
    if bucket.len() < 2 {
        return;
    } else if bucket.len() < 32 {
        bucket.sort_unstable();
        return;
    }

    let counts = get_counts(bucket, level);
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
    let mut finished_map = vec![false; 256];
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

    drop(prefix_sums);
    drop(end_offsets);
    drop(finished_map);

    if level == T::LEVELS - 1 {
        return;
    }

    let msb_target = 256usize.pow((T::LEVELS - level - 1) as u32) / 4;

    bucket.arbitrary_chunks_mut(counts).for_each(|chunk| {
        if chunk.len() < msb_target {
            msb_ska_sort(tuning, chunk, level + 1);
        } else {
            lsb_radix_sort_adapter(tuning, chunk, T::LEVELS - 1, level + 1);
        }
    });
}
