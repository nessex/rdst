use crate::director::top_level_director;
use crate::tuner::Tuner;
use crate::RadixKey;

pub struct SortManager {
    tuner: Box<dyn Tuner + Send + Sync>,
}

impl SortManager {
    pub fn new<T>(tuner: Box<dyn Tuner + Send + Sync>) -> Self
    where
        T: RadixKey + Sized + Send + Sync + Copy,
    {
        // TODO(nathan): Try to make this a compile-time assert
        // This is an invariant of RadixKey that must be upheld.
        assert_ne!(T::LEVELS, 0, "RadixKey must have at least 1 level");

        Self { tuner }
    }

    pub fn sort<T>(&self, bucket: &mut [T])
    where
        T: RadixKey + Sized + Send + Sync + Copy,
    {
        let len = bucket.len();

        // By definition, this must already be sorted.
        if len <= 1 {
            return;
        }

        top_level_director(&*self.tuner, bucket, len, T::LEVELS - 1);
    }
}
