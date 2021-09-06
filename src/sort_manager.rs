use crate::sorts::lsb_radix_sort::lsb_radix_sort_adapter;
use crate::sorts::scanning_radix_sort::scanning_radix_sort;
use crate::tuning_parameters::TuningParameters;
use crate::RadixKey;
use crate::sorts::recombinating_sort::recombinating_sort;

pub struct SortManager {
    tuning: TuningParameters,
}

impl SortManager {
    pub fn new<T>() -> Self
    where
        T: RadixKey + Sized + Send + Sync + Copy,
    {
        if T::LEVELS == 0 {
            panic!("RadixKey must have at least 1 level");
        }

        Self {
            tuning: TuningParameters::new(T::LEVELS),
        }
    }

    pub fn sort<T>(&self, bucket: &mut [T])
    where
        T: RadixKey + Sized + Send + Sync + Copy,
    {
        if bucket.len() < 2 {
            return;
        }

        let parallel_count = bucket.len() >= self.tuning.par_count_threshold;

        if bucket.len() >= self.tuning.scanning_sort_threshold {
            scanning_radix_sort(&self.tuning, bucket, T::LEVELS - 1, parallel_count);
        } else if bucket.len() >= self.tuning.recombinating_sort_threshold {
            recombinating_sort(&self.tuning, bucket, T::LEVELS - 1);
        } else {
            lsb_radix_sort_adapter(bucket, 0, T::LEVELS - 1);
        }
    }
}
