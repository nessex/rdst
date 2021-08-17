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

#[inline]
pub fn calculate_position(level: usize, bucket: usize) -> usize {
    (level << 8) | bucket
}

#[inline]
fn get_count_map<T>() -> Vec<[usize; 256]>
    where
        T: RadixKey,
{
    let mut lsb_counts: Vec<[usize; 256]> = Vec::with_capacity(T::LEVELS);
    for _ in 0..T::LEVELS {
        lsb_counts.push([0usize; 256]);
    }

    lsb_counts
}

#[inline]
pub fn par_get_all_counts<T>(bucket: &[T], start_level: usize, end_level: usize) -> Vec<[usize; 256]>
    where
        T: RadixKey + Sized + Send + Sync,
{
    let chunk_size = (bucket.len() / num_cpus::get()) + 1;
    let lsb_counts = bucket
        .par_chunks(chunk_size)
        .map(|big_chunk| {
            let mut lsb_counts = get_count_map::<T>();
            let sci = big_chunk.chunks_exact(8);
            let rem = sci.remainder();

            sci.for_each(|small_chunk| unsafe {
                for i in start_level..end_level {
                    let a_b = small_chunk.get_unchecked(0).get_level(i) as usize;
                    let b_b = small_chunk.get_unchecked(1).get_level(i) as usize;
                    let c_b = small_chunk.get_unchecked(2).get_level(i) as usize;
                    let d_b = small_chunk.get_unchecked(3).get_level(i) as usize;
                    let e_b = small_chunk.get_unchecked(4).get_level(i) as usize;
                    let f_b = small_chunk.get_unchecked(5).get_level(i) as usize;
                    let g_b = small_chunk.get_unchecked(6).get_level(i) as usize;
                    let h_b = small_chunk.get_unchecked(7).get_level(i) as usize;

                    *lsb_counts.get_unchecked_mut(i).get_unchecked_mut(a_b) += 1;
                    *lsb_counts.get_unchecked_mut(i).get_unchecked_mut(b_b) += 1;
                    *lsb_counts.get_unchecked_mut(i).get_unchecked_mut(c_b) += 1;
                    *lsb_counts.get_unchecked_mut(i).get_unchecked_mut(d_b) += 1;
                    *lsb_counts.get_unchecked_mut(i).get_unchecked_mut(e_b) += 1;
                    *lsb_counts.get_unchecked_mut(i).get_unchecked_mut(f_b) += 1;
                    *lsb_counts.get_unchecked_mut(i).get_unchecked_mut(g_b) += 1;
                    *lsb_counts.get_unchecked_mut(i).get_unchecked_mut(h_b) += 1;
                }
            });

            rem.into_iter().for_each(|v| unsafe {
                for i in start_level..end_level {
                    let a_b = v.get_level(i) as usize;
                    *lsb_counts.get_unchecked_mut(i).get_unchecked_mut(a_b) += 1;
                }
            });

            lsb_counts
        })
        .reduce(
            || get_count_map::<T>(),
            |mut store, lsb| {
                for (i, l) in lsb.iter().enumerate() {
                    for (ii, c) in l.iter().enumerate() {
                        store[i][ii] += c;
                    }
                }

                store
            },
        );

    lsb_counts
}

#[inline]
pub fn get_all_counts<T>(bucket: &[T], start_level: usize, end_level: usize) -> Vec<[usize; 256]>
    where
        T: RadixKey + Sized + Send + Sync,
{
    let mut lsb_counts = get_count_map::<T>();
    let sci = bucket.chunks_exact(8);
    let rem = sci.remainder();

    sci.for_each(|small_chunk| unsafe {
        for i in start_level..end_level {
            let a_b = small_chunk.get_unchecked(0).get_level(i) as usize;
            let b_b = small_chunk.get_unchecked(1).get_level(i) as usize;
            let c_b = small_chunk.get_unchecked(2).get_level(i) as usize;
            let d_b = small_chunk.get_unchecked(3).get_level(i) as usize;
            let e_b = small_chunk.get_unchecked(4).get_level(i) as usize;
            let f_b = small_chunk.get_unchecked(5).get_level(i) as usize;
            let g_b = small_chunk.get_unchecked(6).get_level(i) as usize;
            let h_b = small_chunk.get_unchecked(7).get_level(i) as usize;

            *lsb_counts.get_unchecked_mut(i).get_unchecked_mut(a_b) += 1;
            *lsb_counts.get_unchecked_mut(i).get_unchecked_mut(b_b) += 1;
            *lsb_counts.get_unchecked_mut(i).get_unchecked_mut(c_b) += 1;
            *lsb_counts.get_unchecked_mut(i).get_unchecked_mut(d_b) += 1;
            *lsb_counts.get_unchecked_mut(i).get_unchecked_mut(e_b) += 1;
            *lsb_counts.get_unchecked_mut(i).get_unchecked_mut(f_b) += 1;
            *lsb_counts.get_unchecked_mut(i).get_unchecked_mut(g_b) += 1;
            *lsb_counts.get_unchecked_mut(i).get_unchecked_mut(h_b) += 1;
        }
    });

    rem.into_iter().for_each(|v| unsafe {
        for i in start_level..end_level {
            let a_b = v.get_level(i) as usize;
            *lsb_counts.get_unchecked_mut(i).get_unchecked_mut(a_b) += 1;
        }
    });

    lsb_counts
}
