use std::cmp::Ordering;
use crate::RadixKey;


pub fn comparative_sort_one_level<T>(bucket: &mut [T], level: usize)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    bucket.sort_unstable_by(|a, b| a.get_level(level).cmp(&b.get_level(level)));
}

pub fn comparative_sort<T>(bucket: &mut [T], start_level: usize)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    bucket.sort_unstable_by(|a, b| -> Ordering {
        let mut level = start_level;
        loop {
            let av = a.get_level(level);
            let bv = b.get_level(level);

            match av.cmp(&bv) {
                Ordering::Equal => {
                    if level == 0 {
                        return Ordering::Equal;
                    } else {
                        level -= 1;
                    }
                },
                o => return o,
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use crate::sorts::comparative_sort::comparative_sort;
    use crate::test_utils::{sort_comparison_suite, NumericTest};

    fn test_comparative_sort_adapter<T>(shift: T)
    where
        T: NumericTest<T>,
    {
        sort_comparison_suite(shift, |inputs| {
            comparative_sort(inputs, T::LEVELS - 1)
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

    #[test]
    pub fn test_empty() {
        // This is expected not to panic
        comparative_sort::<usize>(&mut [], 0);
    }
}
