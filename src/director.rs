use crate::sorts::lsb_radix_sort::lsb_radix_sort_adapter;
use crate::sorts::recombinating_sort::recombinating_sort;
use crate::sorts::ska_sort::ska_sort_adapter;
use crate::RadixKey;
use crate::tuning_parameters::TuningParameters;

pub fn director<T>(tuning: &TuningParameters, bucket: &mut [T], level_total_len: usize, level: usize)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    // len_limit allows for detecting buckets which are significantly larger than
    // the rest of their cohort.
    let len_limit = ((level_total_len / num_cpus::get()) as f64 * 1.4) as usize;

    if bucket.len() > len_limit && bucket.len() >= tuning.recombinating_sort_threshold {
        recombinating_sort(tuning, bucket, level);
    } else if bucket.len() >= tuning.ska_sort_threshold {
        ska_sort_adapter(tuning, bucket, level);
    } else {
        lsb_radix_sort_adapter(bucket, 0, level);
    }
}
