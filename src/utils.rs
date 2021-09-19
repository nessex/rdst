use crate::RadixKey;
use rayon::prelude::*;
use std::sync::mpsc::channel;

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
    let threads = rayon::current_num_threads();
    let chunk_divisor = 8;
    let chunk_size = (bucket.len() / threads / chunk_divisor) + 1;
    let chunks = bucket.par_chunks(chunk_size);
    let len = chunks.len();
    let (tx, rx) = channel();
    chunks.for_each_with(tx.clone(), |tx, chunk| {
        let counts = get_counts(chunk, level);
        tx.send(counts).unwrap();
    });

    let mut msb_counts = [0usize; 256];

    for _ in 0..len {
        let counts = rx.recv().unwrap();

        for (i, c) in counts.iter().enumerate() {
            msb_counts[i] += *c;
        }
    }

    msb_counts
}

#[inline]
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

    chunks.into_iter().for_each(|chunk| {
        let a = chunk[0].get_level(level) as usize;
        let b = chunk[1].get_level(level) as usize;
        let c = chunk[2].get_level(level) as usize;
        let d = chunk[3].get_level(level) as usize;

        counts_1[a] += 1;
        counts_2[b] += 1;
        counts_3[c] += 1;
        counts_4[d] += 1;
    });

    rem.into_iter().for_each(|v| {
        let b = v.get_level(level) as usize;
        counts_1[b] += 1;
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
        // Safety: This will leave the vec with potentially uninitialized data
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
