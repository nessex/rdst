use crate::RadixKey;
use crate::tuning_parameters::TuningParameters;
use crate::sorts::lsb_radix_sort::lsb_radix_sort_adapter;
use crate::sorts::scanning_radix_sort::scanning_radix_sort;
use crate::sort_manager::SortManager;

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

#[cfg(test)]
mod tests {
    use crate::test_utils::sort_comparison_suite;
    use crate::{RadixKey, RadixSort};
    use nanorand::{RandomGen, WyRand};
    use std::fmt::Debug;
    use std::ops::{Shl, Shr};

    fn test_full_sort<T>(shift: T)
    where
        T: RadixKey
        + Ord
        + RandomGen<WyRand>
        + Clone
        + Debug
        + Send
        + Sized
        + Copy
        + Sync
        + Shl<Output = T>
        + Shr<Output = T>,
    {
        sort_comparison_suite(shift, |inputs| inputs.radix_sort_unstable());
    }

    #[test]
    pub fn test_u8() {
        test_full_sort(0u8);
    }

    #[test]
    pub fn test_u16() {
        test_full_sort(8u16);
    }

    #[test]
    pub fn test_u32() {
        test_full_sort(16u32);
    }

    #[test]
    pub fn test_u64() {
        test_full_sort(32u64);
    }

    #[test]
    pub fn test_u128() {
        test_full_sort(64u128);
    }

    #[test]
    pub fn test_usize() {
        test_full_sort(32usize);
    }
}
