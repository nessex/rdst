use crate::sorter::Sorter;
use crate::tuner::Tuner;
use crate::tuners::{LowMemoryTuner, StandardTuner};
use crate::RadixKey;

pub struct RadixSortBuilder<'a, T> {
    data: &'a mut [T],
    multi_threaded: bool,
    tuner: &'a (dyn Tuner + Send + Sync),
}

impl<'a, T> RadixSortBuilder<'a, T>
where
    T: RadixKey + Copy + Send + Sync,
{
    pub(crate) fn new(data: &'a mut [T]) -> Self {
        // TODO(nathan): Try to make this a compile-time assert
        // This is an invariant of RadixKey that must be upheld.
        assert_ne!(T::LEVELS, 0, "RadixKey must have at least 1 level");

        Self {
            data,
            multi_threaded: true,
            tuner: &StandardTuner,
        }
    }

    pub fn with_multi_threading(mut self) -> Self {
        self.multi_threaded = true;

        self
    }

    pub fn with_single_threading(mut self) -> Self {
        self.multi_threaded = false;

        self
    }

    pub fn with_low_mem_tuner(mut self) -> Self {
        self.tuner = &LowMemoryTuner;

        self
    }

    pub fn with_tuner(mut self, tuner: &'a (dyn Tuner + Send + Sync)) -> Self {
        self.tuner = tuner;

        self
    }

    pub fn sort(self) {
        // By definition, this is already sorted
        if self.data.len() <= 1 {
            return;
        }

        let sorter = Sorter::new(self.multi_threaded, &*self.tuner);
        sorter.top_level_director(self.data);
    }
}
