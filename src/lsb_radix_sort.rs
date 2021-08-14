use crate::tuning_parameters::TuningParameters;
use crate::utils::*;
use crate::RadixKey;
use rayon::prelude::*;
use std::ptr::copy_nonoverlapping;

#[inline]
fn lsb_radix_sort<T>(bucket: &mut [T], tmp_bucket: &mut [T], level: usize, parallel_count: bool)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let counts = if parallel_count {
        par_get_counts(bucket, level)
    } else {
        get_counts(bucket, level)
    };
    let mut prefix_sums = get_prefix_sums(&counts);

    let chunks = bucket.chunks_exact(8);
    let rem = chunks.remainder();

    chunks.into_iter().for_each(|chunk| unsafe {
        let a = chunk.get_unchecked(0).get_level(level) as usize;
        let b = chunk.get_unchecked(1).get_level(level) as usize;
        let c = chunk.get_unchecked(2).get_level(level) as usize;
        let d = chunk.get_unchecked(3).get_level(level) as usize;
        let e = chunk.get_unchecked(4).get_level(level) as usize;
        let f = chunk.get_unchecked(5).get_level(level) as usize;
        let g = chunk.get_unchecked(6).get_level(level) as usize;
        let h = chunk.get_unchecked(7).get_level(level) as usize;

        *tmp_bucket.get_unchecked_mut(*prefix_sums.get_unchecked(a)) = *chunk.get_unchecked(0);
        *prefix_sums.get_unchecked_mut(a) += 1;
        *tmp_bucket.get_unchecked_mut(*prefix_sums.get_unchecked(b)) = *chunk.get_unchecked(1);
        *prefix_sums.get_unchecked_mut(b) += 1;
        *tmp_bucket.get_unchecked_mut(*prefix_sums.get_unchecked(c)) = *chunk.get_unchecked(2);
        *prefix_sums.get_unchecked_mut(c) += 1;
        *tmp_bucket.get_unchecked_mut(*prefix_sums.get_unchecked(d)) = *chunk.get_unchecked(3);
        *prefix_sums.get_unchecked_mut(d) += 1;
        *tmp_bucket.get_unchecked_mut(*prefix_sums.get_unchecked(e)) = *chunk.get_unchecked(4);
        *prefix_sums.get_unchecked_mut(e) += 1;
        *tmp_bucket.get_unchecked_mut(*prefix_sums.get_unchecked(f)) = *chunk.get_unchecked(5);
        *prefix_sums.get_unchecked_mut(f) += 1;
        *tmp_bucket.get_unchecked_mut(*prefix_sums.get_unchecked(g)) = *chunk.get_unchecked(6);
        *prefix_sums.get_unchecked_mut(g) += 1;
        *tmp_bucket.get_unchecked_mut(*prefix_sums.get_unchecked(h)) = *chunk.get_unchecked(7);
        *prefix_sums.get_unchecked_mut(h) += 1;
    });

    rem.into_iter().for_each(|val| unsafe {
        let bucket = val.get_level(level) as usize;
        *tmp_bucket.get_unchecked_mut(*prefix_sums.get_unchecked(bucket)) = *val;
        *prefix_sums.get_unchecked_mut(bucket) += 1;
    });

    unsafe {
        copy_nonoverlapping(tmp_bucket.get_unchecked(0), bucket.get_unchecked_mut(0), tmp_bucket.len());
    }
}

pub fn lsb_radix_sort_adapter<T>(
    tuning: &TuningParameters,
    bucket: &mut [T],
    start_level: usize,
    end_level: usize,
) where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    if bucket.len() < 2 {
        return;
    }

    let parallel_count = end_level == 0 && bucket.len() > tuning.par_count_threshold;
    let mut tmp_bucket = get_tmp_bucket(bucket.len());
    let mut levels: Vec<usize> = (end_level..=start_level).into_iter().collect();
    levels.reverse();

    for l in levels {
        lsb_radix_sort(bucket, &mut tmp_bucket, l, parallel_count);
    }
}
