use crate::RadixKey;
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

    let threads = num_cpus::get();
    let chunk_divisor = 8;
    let chunk_size = (bucket.len() / threads / chunk_divisor) + 1;

    rayon::scope(|s| {
        let (tx, rx) = channel();

        bucket
            .chunks(chunk_size)
            .for_each(|big_chunk| {
                let tx = tx.clone();
                s.spawn(move |_| {
                    let mut msb_counts_1 = [0usize; 256];
                    let mut msb_counts_2 = [0usize; 256];
                    let mut msb_counts_3 = [0usize; 256];
                    let mut msb_counts_4 = [0usize; 256];
                    let chunks = big_chunk.chunks_exact(4);
                    let rem = chunks.remainder();

                    chunks.into_iter().for_each(|chunk| unsafe {
                        let a = chunk.get_unchecked(0).get_level(level) as usize;
                        let b = chunk.get_unchecked(1).get_level(level) as usize;
                        let c = chunk.get_unchecked(2).get_level(level) as usize;
                        let d = chunk.get_unchecked(3).get_level(level) as usize;

                        *msb_counts_1.get_unchecked_mut(a) += 1;
                        *msb_counts_2.get_unchecked_mut(b) += 1;
                        *msb_counts_3.get_unchecked_mut(c) += 1;
                        *msb_counts_4.get_unchecked_mut(d) += 1;
                    });

                    rem.into_iter().for_each(|v| unsafe {
                        let a = v.get_level(level) as usize;
                        *msb_counts_1.get_unchecked_mut(a) += 1;
                    });

                    for i in 0..256 {
                        msb_counts_1[i] += msb_counts_2[i];
                        msb_counts_1[i] += msb_counts_3[i];
                        msb_counts_1[i] += msb_counts_4[i];
                    }

                    tx.send(msb_counts_1).unwrap();
                });
            });

        let mut msb_counts = [0usize; 256];

        for _ in 0..(threads * chunk_divisor) {
            let counts = rx.recv().unwrap();

            for (i, c) in counts.iter().enumerate() {
                msb_counts[i] += *c;
            }
        }

        msb_counts
    })
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
