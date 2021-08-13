use crate::tuning_parameters::TuningParameters;
use crate::utils::*;
use crate::RadixKey;
use rayon::prelude::*;

// lsb_radix_sort recursively performs an LSB-first radix sort on a bucket of data.
pub fn lsb_radix_sort<T>(
    tuning: &TuningParameters,
    bucket: &mut [T],
    level: usize,
    min_level: usize,
) where
    T: RadixKey + Sized + Send + Ord + Copy + Sync,
{
    if bucket.len() < 2 {
        return;
    } else if bucket.len() < tuning.comparison_sort_threshold {
        bucket.par_sort_unstable();
        return;
    }

    let mut tmp_bucket = get_tmp_bucket(bucket.len());
    let counts = if level == 0 && bucket.len() > tuning.par_count_threshold {
        par_get_counts(bucket, level)
    } else {
        get_counts(bucket, level)
    };
    let mut prefix_sums = get_prefix_sums(&counts);

    let chunks = bucket.chunks_exact(8);
    let rem = chunks.remainder();

    chunks.into_iter().for_each(|chunk| {
        let a = chunk[0].get_level(level) as usize;
        let b = chunk[1].get_level(level) as usize;
        let c = chunk[2].get_level(level) as usize;
        let d = chunk[3].get_level(level) as usize;
        let e = chunk[4].get_level(level) as usize;
        let f = chunk[5].get_level(level) as usize;
        let g = chunk[6].get_level(level) as usize;
        let h = chunk[7].get_level(level) as usize;

        tmp_bucket[prefix_sums[a]] = chunk[0];
        prefix_sums[a] += 1;
        tmp_bucket[prefix_sums[b]] = chunk[1];
        prefix_sums[b] += 1;
        tmp_bucket[prefix_sums[c]] = chunk[2];
        prefix_sums[c] += 1;
        tmp_bucket[prefix_sums[d]] = chunk[3];
        prefix_sums[d] += 1;
        tmp_bucket[prefix_sums[e]] = chunk[4];
        prefix_sums[e] += 1;
        tmp_bucket[prefix_sums[f]] = chunk[5];
        prefix_sums[f] += 1;
        tmp_bucket[prefix_sums[g]] = chunk[6];
        prefix_sums[g] += 1;
        tmp_bucket[prefix_sums[h]] = chunk[7];
        prefix_sums[h] += 1;
    });

    rem.into_iter().for_each(|val| {
        let bucket = val.get_level(level) as usize;
        tmp_bucket[prefix_sums[bucket]] = *val;
        prefix_sums[bucket] += 1;
    });

    bucket.copy_from_slice(&tmp_bucket);

    if level != min_level {
        // Clean up before recursing to reduce memory usage
        drop(prefix_sums);
        drop(counts);
        drop(tmp_bucket);

        lsb_radix_sort(tuning, bucket, level - 1, min_level);
    }
}
