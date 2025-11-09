use crate::radix_sort_builder::RadixSortBuilder;
use crate::RadixKey;

pub trait RadixSort<T> {
    /// radix_sort_unstable runs a radix sort based upon the `rdst::RadixKey` implementation
    /// of `T` in your `Vec<T>` or `[T]`.
    ///
    /// ```
    /// use rdst::RadixSort;
    ///
    /// let mut values = [3, 1, 2];
    /// values.radix_sort_unstable();
    ///
    /// assert_eq!(values, [1, 2, 3]);
    /// ```
    fn radix_sort_unstable(&mut self);

    fn radix_sort_builder(&'_ mut self) -> RadixSortBuilder<'_, T>;
}

impl<T> RadixSort<T> for Vec<T>
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    fn radix_sort_unstable(&mut self) {
        self.radix_sort_builder().sort();
    }

    fn radix_sort_builder(&'_ mut self) -> RadixSortBuilder<'_, T> {
        RadixSortBuilder::new(self)
    }
}

impl<T> RadixSort<T> for [T]
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    fn radix_sort_unstable(&mut self) {
        self.radix_sort_builder().sort();
    }

    fn radix_sort_builder(&'_ mut self) -> RadixSortBuilder<'_, T> {
        RadixSortBuilder::new(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::{sort_comparison_suite, NumericTest, SingleAlgoTuner};
    use crate::tuner::{Algorithm, Tuner, TuningParams};
    use crate::RadixSort;
    use block_pseudorand::block_rand;
    use std::cmp::Ordering;
    use std::fmt::Debug;

    fn test_full_sort<T>(shift: T)
    where
        T: NumericTest<T>,
    {
        sort_comparison_suite(shift, |inputs| inputs.radix_sort_unstable());
    }

    fn test_low_mem_full_sort<T>(shift: T)
    where
        T: NumericTest<T>,
    {
        sort_comparison_suite(shift, |inputs| {
            inputs.radix_sort_builder().with_low_mem_tuner().sort()
        });
    }

    fn test_custom_tuner_full_sort<T>(shift: T)
    where
        T: NumericTest<T>,
    {
        struct CustomTuner {}
        impl Tuner for CustomTuner {
            fn pick_algorithm(&self, _p: &TuningParams, _counts: &[usize]) -> Algorithm {
                Algorithm::Lsb
            }
        }

        sort_comparison_suite(shift, |inputs| {
            inputs
                .radix_sort_builder()
                .with_tuner(&CustomTuner {})
                .sort()
        });
    }

    // This is a generic copy of the
    // nightly total_cmp implementations from the
    // standard library
    trait FpTotalCmp {
        fn fp_total_cmp(&self, other: Self) -> Ordering;
    }

    impl FpTotalCmp for f32 {
        // Ref: https://doc.rust-lang.org/std/primitive.f32.html#method.total_cmp
        fn fp_total_cmp(&self, other: Self) -> Ordering {
            let mut left = self.to_bits() as i32;
            let mut right = other.to_bits() as i32;

            left ^= (((left >> 31) as u32) >> 1) as i32;
            right ^= (((right >> 31) as u32) >> 1) as i32;

            left.cmp(&right)
        }
    }

    impl FpTotalCmp for f64 {
        // Ref: https://doc.rust-lang.org/std/primitive.f64.html#method.total_cmp
        fn fp_total_cmp(&self, other: Self) -> Ordering {
            let mut left = self.to_bits() as i64;
            let mut right = other.to_bits() as i64;

            left ^= (((left >> 63) as u64) >> 1) as i64;
            right ^= (((right >> 63) as u64) >> 1) as i64;

            left.cmp(&right)
        }
    }

    fn test_fp<T: Copy + Debug + PartialEq + PartialOrd + FpTotalCmp>(
        iterations: usize,
        len: usize,
        sort_fn: fn(&mut [T]),
    ) {
        for _ in 0..iterations {
            let mut inputs: Vec<T> = block_rand(len);
            let mut expected = inputs.clone();
            expected.sort_by(|a, b| a.fp_total_cmp(*b));

            sort_fn(&mut inputs);

            let actual = format!("{:?}", inputs);
            let expected = format!("{:?}", expected);

            assert_eq!(actual, expected);
        }
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

    #[test]
    pub fn test_i8() {
        test_full_sort(0i8);
    }

    #[test]
    pub fn test_i16() {
        test_full_sort(8i16);
    }

    #[test]
    pub fn test_i32() {
        test_full_sort(16i32);
    }

    #[test]
    pub fn test_i64() {
        test_full_sort(32i64);
    }

    #[test]
    pub fn test_i128() {
        test_full_sort(64i128);
    }

    #[test]
    pub fn test_isize() {
        test_full_sort(32isize);
    }

    #[test]
    pub fn test_f32() {
        test_fp::<f32>(1_000, 10, |inputs| {
            inputs.radix_sort_unstable();
        });
    }

    #[test]
    pub fn test_f64() {
        test_fp::<f64>(1_000, 10, |inputs| {
            inputs.radix_sort_unstable();
        });
    }

    #[test]
    pub fn test_low_mem_u8() {
        test_low_mem_full_sort(0u8);
    }

    #[test]
    pub fn test_low_mem_u16() {
        test_low_mem_full_sort(8u16);
    }

    #[test]
    pub fn test_low_mem_u32() {
        test_low_mem_full_sort(16u32);
    }

    #[test]
    pub fn test_low_mem_u64() {
        test_low_mem_full_sort(32u64);
    }

    #[test]
    pub fn test_low_mem_u128() {
        test_low_mem_full_sort(64u128);
    }

    #[test]
    pub fn test_low_mem_usize() {
        test_low_mem_full_sort(32usize);
    }

    #[test]
    pub fn test_low_mem_i8() {
        test_low_mem_full_sort(0i8);
    }

    #[test]
    pub fn test_low_mem_i16() {
        test_low_mem_full_sort(8i16);
    }

    #[test]
    pub fn test_low_mem_i32() {
        test_low_mem_full_sort(16i32);
    }

    #[test]
    pub fn test_low_mem_i64() {
        test_low_mem_full_sort(32i64);
    }

    #[test]
    pub fn test_low_mem_i128() {
        test_low_mem_full_sort(64i128);
    }

    #[test]
    pub fn test_low_mem_isize() {
        test_low_mem_full_sort(32isize);
    }

    #[test]
    pub fn test_low_mem_f32() {
        test_fp::<f32>(1_000, 10, |inputs| {
            inputs.radix_sort_builder().with_low_mem_tuner().sort();
        });
    }

    #[test]
    pub fn test_low_mem_f64() {
        test_fp::<f64>(1_000, 10, |inputs| {
            inputs.radix_sort_builder().with_low_mem_tuner().sort();
        });
    }

    #[test]
    pub fn test_custom_tuner_u32() {
        test_custom_tuner_full_sort(16u32);
    }

    #[test]
    pub fn test_custom_tuner_u64() {
        test_custom_tuner_full_sort(32u64);
    }

    #[test]
    pub fn test_f64_parallel_false_only() {
        let mut data = block_rand::<f64>(10_000_000);

        data.radix_sort_builder()
            .with_parallel(false)
            .with_tuner(&SingleAlgoTuner {
                algo: Algorithm::Regions,
            })
            .sort();
    }
}
