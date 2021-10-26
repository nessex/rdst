use crate::sorts::lsb_radix_sort::lsb_radix_sort_adapter;
use crate::sorts::recombinating_sort::recombinating_sort;
use crate::sorts::ska_sort::ska_sort_adapter;
use crate::tuning_parameters::TuningParameters;
use crate::RadixKey;

pub fn director<T>(
    tuning: &TuningParameters,
    inplace: bool,
    bucket: &mut [T],
    level_total_len: usize,
    level: usize,
) where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    if inplace {
        if bucket.len() < tuning.inplace_sort_lsb_threshold {
            lsb_radix_sort_adapter(bucket, 0, level);
        } else {
            ska_sort_adapter(tuning, inplace, bucket, level);
        }
    } else {
        // len_limit allows for detecting buckets which are significantly larger than
        // the rest of their cohort.
        let len_limit = ((level_total_len / tuning.cpus) as f64 * 1.4) as usize;

        if bucket.len() > len_limit && bucket.len() >= tuning.recombinating_sort_threshold {
            recombinating_sort(tuning, bucket, level);
        } else if bucket.len() > tuning.ska_sort_threshold {
            ska_sort_adapter(tuning, inplace, bucket, level);
        } else {
            lsb_radix_sort_adapter(bucket, 0, level);
        }
    }
}
