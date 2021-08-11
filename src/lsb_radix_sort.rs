use crate::utils::*;
use crate::RadixKey;
use rayon::prelude::*;

// lsb_radix_sort_bucket recursively performs an LSB radix sort on a bucket of data.
pub fn lsb_radix_sort_bucket<T>(bucket: &mut [T], level: usize, min_level: usize)
where
    T: RadixKey + Sized + Send + Ord + Copy + Sync,
{
    if bucket.len() < 2 {
        return;
    } else if bucket.len() < 32 {
        bucket.par_sort_unstable();
        return;
    }

    let mut tmp_bucket = get_tmp_bucket(bucket.len());
    let counts = if level == 0 && bucket.len() > 220_000 {
        par_get_msb_counts(bucket)
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
    drop(prefix_sums);
    drop(counts);
    drop(tmp_bucket);

    if level == min_level {
        return;
    } else {
        lsb_radix_sort_bucket(bucket, level - 1, min_level);
    }
}
