//! `comparative_sort` is a radix-aware comparison sort. It operates on radixes rather than
//! whole numbers to support all the same use-cases as the original radix sort including
//! sorting across multiple keys or partial keys etc.
//!
//! The purpose of this sort is to ensure that the library can provide a simpler interface. Without
//! this sort, users would have to implement both `RadixKey` for the radix sort, _and_ `Ord` for
//! the comparison sort. With this, only `RadixKey` is required.
//!
//! While the performance generally sucks, it is still faster than setting up for a full radix sort
//! in situations where there are very few items.
//!
//! ## Characteristics
//!
//!  * in-place
//!  * unstable
//!  * single-threaded
//!
//! ## Performance
//!
//! This is even slower than a typical comparison sort and so is only used as a fallback for very
//! small inputs. However for those very small inputs it provides a significant speed-up due to
//! having essentially no overhead (from count arrays, buffers etc.) compared to a radix sort.

use crate::sort_value::SortValue;
use crate::sorter::Sorter;
use std::cmp::Ordering;
use std::ops::{BitOrAssign, ShlAssign};

#[inline(always)]
fn cmp_packed<T, PackedRepr, const NUM_LEVELS: usize>(a: &T, b: &T) -> Ordering
where
    T: SortValue,
    PackedRepr: Ord + ShlAssign + BitOrAssign + From<u8>,
{
    let mut acc_a: PackedRepr = 0u8.into();
    let mut acc_b: PackedRepr = 0u8.into();

    // This loop is designed to be trivial for the
    // compiler to unroll for any given NumLevels.
    let mut i = NUM_LEVELS;
    while i > 0 {
        i -= 1;
        acc_a <<= 8.into();
        acc_a |= a.get_level_checked(i).into();
        acc_b <<= 8.into();
        acc_b |= b.get_level_checked(i).into();
    }

    acc_a.cmp(&acc_b)
}

impl Sorter<'_> {
    pub(crate) fn comparative_sort<T>(&self, bucket: &mut [T], start_level: usize)
    where
        T: SortValue,
    {
        if bucket.len() < 2 {
            return;
        }

        match start_level {
            // The conditionals here are to help the compiler
            // shake unnecessary match arms out of the end result.
            // This is heavily inlined & unrolled so all these arms add bloat.
            // But the speedup is substantial, on the order of 10x the naive loop version.
            0 if T::LEVELS >= 1 => bucket.sort_unstable_by(|a, b| cmp_packed::<T, u8, 1>(a, b)),
            1 if T::LEVELS >= 2 => bucket.sort_unstable_by(|a, b| cmp_packed::<T, u16, 2>(a, b)),
            2 if T::LEVELS >= 3 => bucket.sort_unstable_by(|a, b| cmp_packed::<T, u32, 3>(a, b)),
            3 if T::LEVELS >= 4 => bucket.sort_unstable_by(|a, b| cmp_packed::<T, u32, 4>(a, b)),
            4 if T::LEVELS >= 5 => bucket.sort_unstable_by(|a, b| cmp_packed::<T, u64, 5>(a, b)),
            5 if T::LEVELS >= 6 => bucket.sort_unstable_by(|a, b| cmp_packed::<T, u64, 6>(a, b)),
            6 if T::LEVELS >= 7 => bucket.sort_unstable_by(|a, b| cmp_packed::<T, u64, 7>(a, b)),
            7 if T::LEVELS >= 8 => bucket.sort_unstable_by(|a, b| cmp_packed::<T, u64, 8>(a, b)),
            8 if T::LEVELS >= 9 => bucket.sort_unstable_by(|a, b| cmp_packed::<T, u128, 9>(a, b)),
            9 if T::LEVELS >= 10 => bucket.sort_unstable_by(|a, b| cmp_packed::<T, u128, 10>(a, b)),
            10 if T::LEVELS >= 11 => {
                bucket.sort_unstable_by(|a, b| cmp_packed::<T, u128, 11>(a, b))
            }
            11 if T::LEVELS >= 12 => {
                bucket.sort_unstable_by(|a, b| cmp_packed::<T, u128, 12>(a, b))
            }
            12 if T::LEVELS >= 13 => {
                bucket.sort_unstable_by(|a, b| cmp_packed::<T, u128, 13>(a, b))
            }
            13 if T::LEVELS >= 14 => {
                bucket.sort_unstable_by(|a, b| cmp_packed::<T, u128, 14>(a, b))
            }
            14 if T::LEVELS >= 15 => {
                bucket.sort_unstable_by(|a, b| cmp_packed::<T, u128, 15>(a, b))
            }
            15 if T::LEVELS >= 16 => {
                bucket.sort_unstable_by(|a, b| cmp_packed::<T, u128, 16>(a, b))
            }
            _ if T::LEVELS >= 17 => bucket.sort_unstable_by(
                #[inline]
                |a, b| -> Ordering {
                    let mut level = start_level;

                    loop {
                        let al = a.get_level_checked(level);
                        let bl = b.get_level_checked(level);
                        let c = al.cmp(&bl);
                        if c != Ordering::Equal {
                            return c;
                        }

                        if level == 0 {
                            return Ordering::Equal;
                        }

                        level -= 1;
                    }
                },
            ),
            _ => unreachable!(),
        };
    }
}

#[cfg(test)]
mod tests {
    use crate::RadixKey;
    use crate::sorter::Sorter;
    use crate::test_utils::{
        NumericTest, SingleAlgoTuner, sort_comparison_suite, sort_single_algorithm,
        validate_u32_patterns,
    };
    use crate::tuner::Algorithm;

    fn test_comparative_sort_adapter<T>(shift: T)
    where
        T: NumericTest<T>,
    {
        let tuner = SingleAlgoTuner {
            algo: Algorithm::Comparative,
        };

        sort_comparison_suite(shift, |inputs| {
            let sorter = Sorter::new(true, &tuner);
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

    #[test]
    pub fn test_basic_integration() {
        sort_single_algorithm::<u32>(1_000_000, Algorithm::Comparative);
    }

    #[test]
    pub fn test_u32_patterns() {
        let tuner = SingleAlgoTuner {
            algo: Algorithm::Comparative,
        };

        validate_u32_patterns(|inputs| {
            let sorter = Sorter::new(true, &tuner);
            sorter.comparative_sort(inputs, u32::LEVELS - 1);
        });
    }
}
