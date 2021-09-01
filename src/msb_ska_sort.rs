use crate::director::director;
use crate::tuning_parameters::TuningParameters;
use crate::utils::*;
use crate::RadixKey;
use arbitrary_chunks::ArbitraryChunks;
use itertools::Itertools;
use rayon::prelude::*;

// Based upon (with modifications):
// https://probablydance.com/2016/12/27/i-wrote-a-faster-sorting-algorithm/
pub fn msb_ska_sort<T>(tuning: &TuningParameters, bucket: &mut [T], level: usize, parallel: bool)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    if bucket.len() < 2 {
        return;
    }

    let (counts, level) =
        if let Some(s) = get_counts_and_level_descending(bucket, level, 0, parallel) {
            s
        } else {
            return;
        };

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

    drop(prefix_sums);
    drop(end_offsets);
    drop(finished_map);

    if level == 0 {
        return;
    }

    bucket
        .arbitrary_chunks_mut(counts.to_vec())
        .par_bridge()
        .for_each(|chunk| director(tuning, chunk, level - 1, false));
}
