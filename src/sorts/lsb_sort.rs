use crate::sorts::out_of_place_sort::{out_of_place_sort, out_of_place_sort_with_counts};
use crate::utils::*;
use crate::RadixKey;

pub fn lsb_sort_adapter<T>(bucket: &mut [T], start_level: usize, end_level: usize)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    if bucket.len() < 2 {
        return;
    }

    let mut tmp_bucket = get_tmp_bucket(bucket.len());
    let levels: Vec<usize> = (start_level..=end_level).into_iter().collect();
    let mut invert = false;
    let mut next_counts = None;

    'outer: for level in levels {
        if next_counts.is_none() {
            next_counts = Some(get_counts(bucket, level));
        }

        if let Some(counts) = next_counts {
            // Check for skippable levels
            for c in counts {
                if c == bucket.len() {
                    next_counts = None;
                    continue 'outer;
                }
            }

            match (invert, level == end_level) {
                (true, true) => out_of_place_sort(&mut tmp_bucket, bucket, &counts, level),
                (true, false) => next_counts = Some(out_of_place_sort_with_counts(&mut tmp_bucket, bucket, &counts, level)),
                (false, true) => out_of_place_sort(bucket, &mut tmp_bucket, &counts, level),
                (false, false) => next_counts = Some(out_of_place_sort_with_counts(bucket, &mut tmp_bucket, &counts, level)),
            };

            invert = !invert;
        }
    }

    if invert {
        bucket.copy_from_slice(&tmp_bucket);
    }
}

#[cfg(test)]
mod tests {
    use crate::sorts::lsb_sort::lsb_sort_adapter;
    use crate::test_utils::{sort_comparison_suite, NumericTest};

    fn test_lsb_sort_adapter<T>(shift: T)
    where
        T: NumericTest<T>,
    {
        sort_comparison_suite(shift, |inputs| lsb_sort_adapter(inputs, 0, T::LEVELS - 1));
    }

    #[test]
    pub fn test_u8() {
        test_lsb_sort_adapter(0u8);
    }

    #[test]
    pub fn test_u16() {
        test_lsb_sort_adapter(8u16);
    }

    #[test]
    pub fn test_u32() {
        test_lsb_sort_adapter(16u32);
    }

    #[test]
    pub fn test_u64() {
        test_lsb_sort_adapter(32u64);
    }

    #[test]
    pub fn test_u128() {
        test_lsb_sort_adapter(64u128);
    }

    #[test]
    pub fn test_usize() {
        test_lsb_sort_adapter(32usize);
    }
}
