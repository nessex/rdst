use crate::sorts::out_of_place_sort::out_of_place_sort;
use crate::utils::*;
use crate::RadixKey;
use crate::sorts::ska_sort::ska_sort;

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

    for l in levels {
        let (counts, level) = if let Some(s) = get_counts_and_level_ascending(bucket, l, l, false) {
            s
        } else {
            continue;
        };

        if l == start_level && (end_level - start_level) % 2 == 0 {
            // Use ska sort if the levels in question here will likely require an additional copy
            // at the end.
            let plateaus = detect_plateaus(bucket, l);
            let (mut prefix_sums, end_offsets) = apply_plateaus(bucket, &counts, &plateaus);
            ska_sort(bucket, &mut prefix_sums, &end_offsets, l);
        } else {
            let (src, dst) = if invert {
                (&mut *tmp_bucket.as_mut_slice(), &mut *bucket)
            } else {
                (&mut *bucket, &mut *tmp_bucket.as_mut_slice())
            };
            invert = !invert;

            out_of_place_sort(src, dst, &counts, level);
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
