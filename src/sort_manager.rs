use crate::sorts::lsb_radix_sort::lsb_radix_sort_adapter;
use crate::sorts::recombinating_sort::recombinating_sort;
use crate::sorts::regions_sort::regions_sort;
use crate::sorts::scanning_radix_sort::scanning_radix_sort;
use crate::tuning_parameters::TuningParameters;
use crate::RadixKey;

pub struct SortManager {
    tuning: TuningParameters,
}

impl SortManager {
    pub fn new<T>() -> Self
    where
        T: RadixKey + Sized + Send + Sync + Copy,
    {
        assert_ne!(T::LEVELS, 0, "RadixKey must have at least 1 level");

        Self {
            tuning: TuningParameters::new(T::LEVELS),
        }
    }

    pub fn sort<T>(&self, bucket: &mut [T])
    where
        T: RadixKey + Sized + Send + Sync + Copy,
    {
        let bucket_len = bucket.len();
        if bucket_len < 2 {
            return;
        }

        let parallel_count = bucket.len() >= self.tuning.par_count_threshold;

        match bucket_len {
            n if n >= self.tuning.regions_sort_threshold => {
                regions_sort(&self.tuning, bucket, T::LEVELS - 1)
            }
            n if n >= self.tuning.scanning_sort_threshold => {
                scanning_radix_sort(&self.tuning, bucket, T::LEVELS - 1, parallel_count)
            }
            n if n >= self.tuning.recombinating_sort_threshold => {
                recombinating_sort(&self.tuning, bucket, T::LEVELS - 1)
            }
            _ => lsb_radix_sort_adapter(bucket, 0, T::LEVELS - 1),
        };
    }
}
