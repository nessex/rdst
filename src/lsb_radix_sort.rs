use crate::utils::*;
use crate::RadixKey;

// lsb_radix_sort_bucket recursively performs an LSB radix sort on a bucket of data, stopping at level 1.
pub fn lsb_radix_sort_bucket<T>(
    bucket: &mut [T],
    tmp_bucket: &mut [T],
    level: usize,
    msb: usize,
    counts: &[usize],
) where
    T: RadixKey + Sized + Send + Ord + Copy + Sync,
{
    if bucket.len() < 32 {
        bucket.sort_unstable();
        return;
    }

    let mut local_counts = Vec::with_capacity(256);
    let mut prefix_sums = Vec::with_capacity(256);
    let mut running_total = 0;

    for i in 0..256 {
        let count = counts[calculate_position(msb, level - 1, i)];
        local_counts.push(count);
        prefix_sums.push(running_total);
        running_total += count;
    }

    bucket.iter().for_each(|val| {
        let bucket = val.get_level(level) as usize;
        unsafe {
            let write_loc = prefix_sums.get_unchecked_mut(bucket);
            *tmp_bucket.get_unchecked_mut(*write_loc) = *val;
            *write_loc += 1;
        }
    });

    drop(prefix_sums);
    bucket.copy_from_slice(tmp_bucket);

    if level == 1 {
        return;
    } else {
        lsb_radix_sort_bucket(bucket, tmp_bucket, level - 1, msb, counts);
    }
}
