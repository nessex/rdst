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

pub fn get_counts<T>(bucket: &[T], level: usize) -> [usize; 256]
    where
        T: RadixKey,
{
    let mut counts_1 = [0usize; 256];
    let mut counts_2 = [0usize; 256];
    let mut counts_3 = [0usize; 256];
    let mut counts_4 = [0usize; 256];
    let chunks = bucket.chunks_exact(4);
    let rem = chunks.remainder();

    chunks.into_iter().for_each(|chunk| unsafe {
        let a = chunk.get_unchecked(0).get_level(level) as usize;
        let b = chunk.get_unchecked(1).get_level(level) as usize;
        let c = chunk.get_unchecked(2).get_level(level) as usize;
        let d = chunk.get_unchecked(3).get_level(level) as usize;

        *counts_1.get_unchecked_mut(a) += 1;
        *counts_2.get_unchecked_mut(b) += 1;
        *counts_3.get_unchecked_mut(c) += 1;
        *counts_4.get_unchecked_mut(d) += 1;
    });

    rem.into_iter().for_each(|v| unsafe {
        let b = v.get_level(level) as usize;
        *counts_1.get_unchecked_mut(b) += 1;
    });

    for i in 0..256 {
        counts_1[i] += counts_2[i];
        counts_1[i] += counts_3[i];
        counts_1[i] += counts_4[i];
    }

    counts_1
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

pub fn get_counts_and_level<T>(bucket: &[T], start_level: usize, end_level: usize, parallel_count: bool) -> Option<([usize; 256], usize)>
where
    T: RadixKey + Sized + Send + Sync
{
    let counts;
    let mut level = start_level;
    let ascending = start_level < end_level;

    'outer: loop {
        let tmp_counts = if parallel_count {
            par_get_counts(bucket, level)
        } else {
            get_counts(bucket, level)
        };

        let mut num_buckets = 0;
        for c in tmp_counts {
            if c > 0 {
                if num_buckets == 1 {
                    counts = tmp_counts;
                    break 'outer;
                }

                num_buckets += 1;
            }
        }

        if level == end_level {
            return None;
        }

        if ascending {
            level += 1;
        } else {
            level -= 1;
        }
    }

    Some((counts, level))
}
