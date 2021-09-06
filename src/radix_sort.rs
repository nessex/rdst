use crate::{TuningParameters, RadixKey};
use crate::lsb_radix_sort::lsb_radix_sort_adapter;
use crate::scanning_radix_sort::scanning_radix_sort;

fn radix_sort_bucket_start<T>(tuning: &TuningParameters, bucket: &mut [T])
    where
        T: RadixKey + Sized + Send + Copy + Sync,
{
    if bucket.len() < 2 {
        return;
    }

    let parallel_count = bucket.len() >= tuning.par_count_threshold;

    if bucket.len() >= tuning.scanning_sort_threshold {
        scanning_radix_sort(tuning, bucket, T::LEVELS - 1, parallel_count);
    } else {
        lsb_radix_sort_adapter(bucket, 0, T::LEVELS - 1);
    }
}

fn radix_sort_inner<T>(bucket: &mut [T])
    where
        T: RadixKey + Sized + Send + Copy + Sync,
{
    if T::LEVELS == 0 {
        panic!("RadixKey must have at least 1 level");
    }

    let tuning = TuningParameters::new(T::LEVELS);

    radix_sort_bucket_start(&tuning, bucket);
}

pub trait RadixSort {
    /// radix_sort_unstable runs the actual radix sort based upon the `rdst::RadixKey` implementation
    /// of `T` in your `Vec<T>` or `[T]`.
    fn radix_sort_unstable(&mut self);
}

impl<T> RadixSort for Vec<T>
    where
        T: RadixKey + Sized + Send + Copy + Sync,
{
    fn radix_sort_unstable(&mut self) {
        radix_sort_inner(self);
    }
}

impl<T> RadixSort for [T]
    where
        T: RadixKey + Sized + Send + Copy + Sync,
{
    fn radix_sort_unstable(&mut self) {
        radix_sort_inner(self);
    }
}
