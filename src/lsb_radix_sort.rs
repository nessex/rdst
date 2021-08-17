use crate::utils::*;
use crate::RadixKey;
use std::ptr::copy_nonoverlapping;

#[inline]
fn lsb_radix_sort<T>(bucket: &mut [T], tmp_bucket: &mut [T], level: usize, counts: &[usize])
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let mut prefix_sums = get_prefix_sums(counts);

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

pub fn lsb_radix_sort_adapter<T>(
    bucket: &mut [T],
    start_level: usize,
    end_level: usize,
    parallel_count: bool,
) where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    if bucket.len() < 2 {
        return;
    }

    let mut tmp_bucket = get_tmp_bucket(bucket.len());
    let mut levels: Vec<usize> = (end_level..=start_level).into_iter().collect();
    levels.reverse();

    let all_counts = if parallel_count {
        par_get_all_counts(bucket, end_level, start_level + 1)
    } else {
        get_all_counts(bucket, end_level, start_level + 1)
    };

    for l in levels {
        let counts = all_counts[l];
        let mut non_empty = 0;

        for c in counts {
            if c > 0 {
                non_empty += 1;

                if non_empty >= 2 {
                    lsb_radix_sort(bucket, &mut tmp_bucket, l, &counts);
                    break;
                }
            }
        }
    }
}
