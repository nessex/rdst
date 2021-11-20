use crate::director::single_director;
use crate::tuner::{DefaultTuner, Tuner};
use crate::RadixKey;

pub struct SortManager {
    tuner: Box<dyn Tuner + Send + Sync>,
}

impl SortManager {
    pub fn new<T>() -> Self
    where
        T: RadixKey + Sized + Send + Sync + Copy,
    {
        assert_ne!(T::LEVELS, 0, "RadixKey must have at least 1 level");

        Self {
            tuner: Box::new(DefaultTuner {}),
        }
    }

    #[cfg(feature = "tuning")]
    pub fn new_with_tuning<T>(tuner: Box<dyn Tuner + Send + Sync>) -> Self
    where
        T: RadixKey + Sized + Send + Sync + Copy,
    {
        assert_ne!(T::LEVELS, 0, "RadixKey must have at least 1 level");

        Self { tuner }
    }

    pub fn sort<T>(&self, bucket: &mut [T])
    where
        T: RadixKey + Sized + Send + Sync + Copy,
    {
        if bucket.len() <= 1 {
            return;
        }

        let bucket_len = bucket.len();
        let parent_len = bucket_len;

        single_director(&*self.tuner, false, bucket, parent_len, T::LEVELS - 1);
    }

    pub fn sort_in_place<T>(&self, bucket: &mut [T])
    where
        T: RadixKey + Sized + Send + Sync + Copy,
    {
        if bucket.len() <= 1 {
            return;
        }

        let bucket_len = bucket.len();
        let parent_len = bucket_len;

        single_director(&*self.tuner, true, bucket, parent_len, T::LEVELS - 1);
    }
}
