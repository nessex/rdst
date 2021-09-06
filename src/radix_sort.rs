use crate::RadixKey;
use crate::tuning_parameters::TuningParameters;
use crate::sorts::lsb_radix_sort::lsb_radix_sort_adapter;
use crate::sorts::scanning_radix_sort::scanning_radix_sort;
use crate::sort_manager::SortManager;

fn radix_sort_bucket_start<T>(tuning: &TuningParameters, bucket: &mut [T])
    where
        T: RadixKey + Sized + Send + Copy + Sync,
{

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
        let sm = SortManager::new::<T>();
        sm.sort(self);
    }
}

impl<T> RadixSort for [T]
    where
        T: RadixKey + Sized + Send + Copy + Sync,
{
    fn radix_sort_unstable(&mut self) {
        let sm = SortManager::new::<T>();
        sm.sort(self);
    }
}
