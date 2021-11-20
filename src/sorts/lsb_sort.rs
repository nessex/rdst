use crate::sorts::out_of_place_sort::{out_of_place_sort, out_of_place_sort_with_counts};
use crate::sorts::ska_sort::ska_sort;
use crate::utils::*;
use crate::RadixKey;

pub fn lsb_sort_adapter<T>(
    bucket: &mut [T],
    last_counts: &[usize; 256],
    start_level: usize,
    end_level: usize,
) where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    if bucket.len() < 2 {
        return;
    }

    let mut tmp_bucket = get_tmp_bucket(bucket.len());
    let levels: Vec<usize> = (start_level..=end_level).into_iter().collect();
    let mut invert = false;
    let mut first = true;
    let mut next_counts = None;

    'outer: for level in levels {
        let counts = if level == end_level {
            *last_counts
        } else if let Some(next_counts) = next_counts {
            next_counts
        } else {
            get_counts(bucket, level)
        };

        for c in counts.iter() {
            if *c == bucket.len() {
                continue 'outer;
            } else if *c > 0 {
                break;
            }
        }

        if first == true && (end_level + 1 - level) % 2 != 0 {
            // Use ska sort if the levels in question here will likely require an additional copy
            // at the end
            let plateaus = detect_plateaus(bucket, level);
            let (mut prefix_sums, end_offsets) = apply_plateaus(bucket, &counts, &plateaus);
            ska_sort(bucket, &mut prefix_sums, &end_offsets, level);
            first = false;
            continue;
        }

        let should_count = level < (end_level - 1);
        if !should_count {
            next_counts = None;
        }

        match (invert, should_count) {
            (true, true) => {
                next_counts = Some(out_of_place_sort_with_counts(
                    &mut tmp_bucket,
                    bucket,
                    &counts,
                    level,
                ))
            }
            (true, false) => out_of_place_sort(&mut tmp_bucket, bucket, &counts, level),
            (false, true) => {
                next_counts = Some(out_of_place_sort_with_counts(
                    bucket,
                    &mut tmp_bucket,
                    &counts,
                    level,
                ))
            }
            (false, false) => out_of_place_sort(bucket, &mut tmp_bucket, &counts, level),
        };

        first = false;
        invert = !invert;
    }

    if invert {
        bucket.copy_from_slice(&tmp_bucket);
    }
}

#[cfg(test)]
mod tests {
    use crate::sorts::lsb_sort::lsb_sort_adapter;
    use crate::test_utils::{sort_comparison_suite, NumericTest};
    use crate::utils::get_counts;

    fn test_lsb_sort_adapter<T>(shift: T)
    where
        T: NumericTest<T>,
    {
        sort_comparison_suite(shift, |inputs| {
            let counts = get_counts(inputs, T::LEVELS - 1);
            lsb_sort_adapter(inputs, &counts, 0, T::LEVELS - 1)
        });
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
