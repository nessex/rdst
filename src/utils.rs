use crate::RadixKey;
use rayon::prelude::*;

#[inline]
pub fn get_prefix_sums(counts: &[usize]) -> [usize; 256] {
    let mut sums = [0usize; 256];

    let mut running_total = 0;
    for (i, c) in counts.iter().enumerate() {
        sums[i] = running_total;
        running_total += c;
    }

    sums
}

#[inline]
pub fn par_get_counts<T>(bucket: &[T], level: usize) -> [usize; 256]
where
    T: RadixKey + Sized + Send + Sync,
{
    let chunk_size = (bucket.len() / num_cpus::get()) + 1;
    let msb_counts = bucket
        .par_chunks(chunk_size)
        .map(|big_chunk| {
            let mut msb_counts = [0usize; 256];
            let chunks = big_chunk.chunks_exact(8);
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

                *msb_counts.get_unchecked_mut(a) += 1;
                *msb_counts.get_unchecked_mut(b) += 1;
                *msb_counts.get_unchecked_mut(c) += 1;
                *msb_counts.get_unchecked_mut(d) += 1;
                *msb_counts.get_unchecked_mut(e) += 1;
                *msb_counts.get_unchecked_mut(f) += 1;
                *msb_counts.get_unchecked_mut(g) += 1;
                *msb_counts.get_unchecked_mut(h) += 1;
            });

            rem.into_iter().for_each(|v| unsafe {
                let a = v.get_level(level) as usize;
                *msb_counts.get_unchecked_mut(a) += 1;
            });

            msb_counts
        })
        .reduce(
            || [0usize; 256],
            |mut msb_counts, msb| {
                for (i, c) in msb.iter().enumerate() {
                    msb_counts[i] += c;
                }

                msb_counts
            },
        );

    msb_counts
}

#[inline]
pub fn get_counts<T>(bucket: &[T], level: usize) -> [usize; 256]
where
    T: RadixKey,
{
    let mut counts = [0usize; 256];
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

        *counts.get_unchecked_mut(a) += 1;
        *counts.get_unchecked_mut(b) += 1;
        *counts.get_unchecked_mut(c) += 1;
        *counts.get_unchecked_mut(d) += 1;
        *counts.get_unchecked_mut(e) += 1;
        *counts.get_unchecked_mut(f) += 1;
        *counts.get_unchecked_mut(g) += 1;
        *counts.get_unchecked_mut(h) += 1;
    });

    rem.into_iter().for_each(|v| unsafe {
        let b = v.get_level(level) as usize;
        *counts.get_unchecked_mut(b) += 1;
    });

    counts
}

#[inline]
pub fn get_tmp_bucket<T>(len: usize) -> Vec<T> {
    let mut tmp_bucket = Vec::with_capacity(len);
    unsafe {
        // This will leave the vec with garbage data
        // however as we account for every value when placing things
        // into tmp_bucket, this is "safe". This is used because it provides a
        // very significant speed improvement over resize, to_vec etc.
        tmp_bucket.set_len(len);
    }

    tmp_bucket
}

#[inline]
pub fn get_counts_and_level_ascending<T>(
    bucket: &[T],
    start_level: usize,
    end_level: usize,
    parallel_count: bool,
) -> Option<([usize; 256], usize)>
where
    T: RadixKey + Sized + Send + Sync,
{
    let counts = if parallel_count {
        par_get_counts
    } else {
        get_counts
    };

    for level in start_level..=end_level {
        let tmp_counts = counts(bucket, level);

        let mut num_buckets = 0;
        for c in tmp_counts {
            if c > 0 {
                if num_buckets == 1 {
                    return Some((tmp_counts, level));
                }

                num_buckets += 1;
            }
        }
    }

    None
}

#[inline]
pub fn get_counts_and_level_descending<T>(
    bucket: &[T],
    start_level: usize,
    end_level: usize,
    parallel_count: bool,
) -> Option<([usize; 256], usize)>
where
    T: RadixKey + Sized + Send + Sync,
{
    let counts = if parallel_count {
        par_get_counts
    } else {
        get_counts
    };

    for level in (end_level..=start_level).into_iter().rev() {
        let tmp_counts = counts(bucket, level);

        let mut num_buckets = 0;
        for c in tmp_counts {
            if c > 0 {
                if num_buckets == 1 {
                    return Some((tmp_counts, level));
                }

                num_buckets += 1;
            }
        }
    }

    None
}
