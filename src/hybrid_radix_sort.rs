use crate::lsb_radix_sort::lsb_radix_sort_bucket;
use crate::utils::*;
use crate::RadixKey;
use arbitrary_chunks::ArbitraryChunks;
use rayon::prelude::*;

// hybrid_radix_sort_bucket sorts by MSB then for each value of MSB does a LSB-first sort.
// This is the sequential implementation. The parallel equivalent is scanning_radix_sort.
pub fn hybrid_radix_sort_bucket<T>(
    bucket: &mut [T],
    tmp_bucket: &mut [T],
    msb_counts: Vec<usize>,
    lsb_counts: &[usize],
) where
    T: RadixKey + Sized + Send + Ord + Copy + Sync,
{
    let level = 0;
    let mut prefix_sums = get_prefix_sums(&msb_counts);

    bucket.into_iter().for_each(|val| {
        let bucket = val.get_level(level) as usize;
        unsafe {
            // As prefix_sums is always exactly 256 elements long
            // and get_level() returns a byte, this is always valid.
            // This provides a significant speedup.
            let sum = prefix_sums.get_unchecked_mut(bucket);
            tmp_bucket[*sum] = *val;
            *sum += 1;
        }
    });

    drop(prefix_sums);
    bucket.copy_from_slice(tmp_bucket);

    bucket
        .arbitrary_chunks_mut(msb_counts.clone())
        .zip(tmp_bucket.arbitrary_chunks_mut(msb_counts))
        .enumerate()
        .par_bridge()
        .for_each(|(msb, (c, t))| {
            lsb_radix_sort_bucket(c, t, T::LEVELS - 1, msb, lsb_counts);
        });
}
