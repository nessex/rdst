use crate::lsb_radix_sort::lsb_radix_sort_adapter;
use crate::recombinating_sort::recombinating_sort;
use crate::ska_sort::ska_sort_adapter;
use crate::utils::*;
use crate::RadixKey;

pub fn director<T>(bucket: &mut [T], level_total_len: usize, level: usize)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    // len_limit allows for detecting buckets which are significantly larger than
    // the rest of their cohort.
    let len_limit = ((level_total_len / num_cpus::get()) as f64 * 1.4) as usize;

    if bucket.len() > len_limit && bucket.len() >= 350_000 {
        recombinating_sort(bucket, level);
    } else if bucket.len() >= 350_000 {
        ska_sort_adapter(bucket, level);
    } else {
        lsb_radix_sort_adapter(bucket, 0, level);
    }
}
