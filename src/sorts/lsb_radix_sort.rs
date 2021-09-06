use crate::utils::*;
use crate::RadixKey;
use std::ptr::copy_nonoverlapping;

#[inline]
fn lsb_radix_sort<T>(bucket: &mut [T], tmp_bucket: &mut [T], counts: &[usize], level: usize)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let mut prefix_sums = get_prefix_sums(&counts);

    let chunks = bucket.chunks_exact(8);
    let rem = chunks.remainder();

    chunks.into_iter().for_each(|chunk| unsafe {
        let a = chunk.get_unchecked(0).get_level(level) as usize;
        let b = chunk.get_unchecked(1).get_level(level) as usize;
        let c = chunk.get_unchecked(2).get_level(level) as usize;
        let d = chunk.get_unchecked(3).get_level(level) as usize;
        let e = chunk.get_unchecked(4).get_level(level) as usize;
        let f = chunk.get_unchecked(5).get_level(level) as usize;
        let g = chunk.get_unchecked(6).get_level(level) as usize;
        let h = chunk.get_unchecked(7).get_level(level) as usize;

        *tmp_bucket.get_unchecked_mut(*prefix_sums.get_unchecked(a)) = *chunk.get_unchecked(0);
        *prefix_sums.get_unchecked_mut(a) += 1;
        *tmp_bucket.get_unchecked_mut(*prefix_sums.get_unchecked(b)) = *chunk.get_unchecked(1);
        *prefix_sums.get_unchecked_mut(b) += 1;
        *tmp_bucket.get_unchecked_mut(*prefix_sums.get_unchecked(c)) = *chunk.get_unchecked(2);
        *prefix_sums.get_unchecked_mut(c) += 1;
        *tmp_bucket.get_unchecked_mut(*prefix_sums.get_unchecked(d)) = *chunk.get_unchecked(3);
        *prefix_sums.get_unchecked_mut(d) += 1;
        *tmp_bucket.get_unchecked_mut(*prefix_sums.get_unchecked(e)) = *chunk.get_unchecked(4);
        *prefix_sums.get_unchecked_mut(e) += 1;
        *tmp_bucket.get_unchecked_mut(*prefix_sums.get_unchecked(f)) = *chunk.get_unchecked(5);
        *prefix_sums.get_unchecked_mut(f) += 1;
        *tmp_bucket.get_unchecked_mut(*prefix_sums.get_unchecked(g)) = *chunk.get_unchecked(6);
        *prefix_sums.get_unchecked_mut(g) += 1;
        *tmp_bucket.get_unchecked_mut(*prefix_sums.get_unchecked(h)) = *chunk.get_unchecked(7);
        *prefix_sums.get_unchecked_mut(h) += 1;
    });

    rem.into_iter().for_each(|val| unsafe {
        let bucket = val.get_level(level) as usize;
        *tmp_bucket.get_unchecked_mut(*prefix_sums.get_unchecked(bucket)) = *val;
        *prefix_sums.get_unchecked_mut(bucket) += 1;
    });

    unsafe {
        copy_nonoverlapping(
            tmp_bucket.get_unchecked(0),
            bucket.get_unchecked_mut(0),
            tmp_bucket.len(),
        );
    }
}

pub fn lsb_radix_sort_adapter<T>(bucket: &mut [T], start_level: usize, end_level: usize)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    if bucket.len() < 2 {
        return;
    }

    let mut tmp_bucket = get_tmp_bucket(bucket.len());
    let levels: Vec<usize> = (start_level..=end_level).into_iter().collect();

    for l in levels {
        let (counts, level) = if let Some(s) = get_counts_and_level_ascending(bucket, l, l, false) {
            s
        } else {
            continue;
        };

        lsb_radix_sort(bucket, &mut tmp_bucket, &counts, level);
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::sort_comparison_suite;
    use crate::{RadixKey, RadixSort};
    use nanorand::{RandomGen, WyRand};
    use std::fmt::Debug;
    use std::ops::{Shl, Shr};
    use crate::sorts::lsb_radix_sort::lsb_radix_sort_adapter;

    fn test_lsb_radix_sort_adapter<T>(shift: T)
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
        sort_comparison_suite(shift, |inputs| lsb_radix_sort_adapter(inputs, 0, T::LEVELS - 1));
    }

    #[test]
    pub fn test_u8() {
        test_lsb_radix_sort_adapter(0u8);
    }

    #[test]
    pub fn test_u16() {
        test_lsb_radix_sort_adapter(8u16);
    }

    #[test]
    pub fn test_u32() {
        test_lsb_radix_sort_adapter(16u32);
    }

    #[test]
    pub fn test_u64() {
        test_lsb_radix_sort_adapter(32u64);
    }

    #[test]
    pub fn test_u128() {
        test_lsb_radix_sort_adapter(64u128);
    }

    #[test]
    pub fn test_usize() {
        test_lsb_radix_sort_adapter(32usize);
    }
}
