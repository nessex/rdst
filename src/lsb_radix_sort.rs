use crate::utils::*;
use crate::RadixKey;
use rayon::prelude::*;
use crate::tuning_parameters::TuningParameters;

// lsb_radix_sort recursively performs an LSB-first radix sort on a bucket of data.
pub fn lsb_radix_sort<T>(tuning: &TuningParameters, bucket: &mut [T], level: usize, min_level: usize)
where
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

    bucket.iter().for_each(|val| {
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
