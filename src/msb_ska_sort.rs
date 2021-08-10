use crate::lsb_radix_sort::lsb_radix_sort_bucket;
use crate::utils::{get_counts, get_prefix_sums};
use crate::RadixKey;
use arbitrary_chunks::ArbitraryChunks;
use itertools::Itertools;

// Based upon (with modifications):
// https://probablydance.com/2016/12/27/i-wrote-a-faster-sorting-algorithm/
pub fn msb_ska_sort<T>(bucket: &mut [T], level: usize)
where
    T: RadixKey + Sized + Send + Ord + Copy + Sync,
{
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

    while finished != 256 {
        for b in buckets.iter() {
            if finished_map[*b] {
                continue;
            } else if prefix_sums[*b] >= end_offsets[*b] {
                finished_map[*b] = true;
                finished += 1;
            }

            let offset = prefix_sums[*b];
            let remaining = end_offsets[*b] - offset;

            for i in 0..remaining {
                let new_b = bucket[offset + i].get_level(level) as usize;
                bucket.swap(prefix_sums[new_b], offset + i);
                prefix_sums[new_b] += 1;
            }
        }
    }

    bucket.arbitrary_chunks_mut(counts).for_each(|chunk| {
        if chunk.len() > 10_000 {
            msb_ska_sort(chunk, level + 1);
        } else {
            lsb_radix_sort_bucket(chunk, T::LEVELS - 1, level + 1);
        }
    });
}
