use crate::sorter::Sorter;
use crate::RadixKey;
use std::cmp::Ordering;

impl<'a> Sorter<'a> {
    pub(crate) fn comparative_sort<T>(&self, bucket: &mut [T], start_level: usize)
    where
        T: RadixKey + Sized + Send + Copy + Sync,
    {
        bucket.sort_unstable_by(|a, b| -> Ordering {
            let mut level = start_level;
            loop {
                let cmp = a.get_level(level).cmp(&b.get_level(level));

                if level != 0 && cmp == Ordering::Equal {
                    level -= 1;
                    continue;
                }

                return cmp;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::sorter::Sorter;
    use crate::tuners::StandardTuner;
    use crate::utils::test_utils::{sort_comparison_suite, NumericTest};

    fn test_comparative_sort_adapter<T>(shift: T)
    where
        T: NumericTest<T>,
    {
        let sorter = Sorter::new(true, &StandardTuner);

        sort_comparison_suite(shift, |inputs| {
            sorter.comparative_sort(inputs, T::LEVELS - 1);
        });
    }

    #[test]
    pub fn test_u8() {
        test_comparative_sort_adapter(0u8);
    }

    #[test]
    pub fn test_u16() {
        test_comparative_sort_adapter(8u16);
    }

    #[test]
    pub fn test_u32() {
        test_comparative_sort_adapter(16u32);
    }

    #[test]
    pub fn test_u64() {
        test_comparative_sort_adapter(32u64);
    }

    #[test]
    pub fn test_u128() {
        test_comparative_sort_adapter(64u128);
    }

    #[test]
    pub fn test_usize() {
        test_comparative_sort_adapter(32usize);
    }
}
