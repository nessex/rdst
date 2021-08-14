use crate::RadixKey;
use rayon::prelude::*;

#[inline]
pub fn get_prefix_sums(counts: &[usize]) -> Vec<usize> {
    let mut sums = Vec::with_capacity(256);

    let mut running_total = 0;
    for c in counts.iter() {
        sums.push(running_total);
        running_total += c;
    }

    sums
}

#[inline]
pub fn get_double_prefix_sums(counts: &[usize]) -> Vec<usize> {
    let mut sums = Vec::with_capacity(65536);

    let mut running_total = 0;
    for c in counts.iter() {
        sums.push(running_total);
        running_total += c;
    }

    sums
}

pub fn par_get_counts<T>(bucket: &[T], level: usize) -> Vec<usize>
where
    T: RadixKey + Sized + Send + Sync,
{
    let chunk_size = (bucket.len() / num_cpus::get()) + 1;
    let msb_counts = bucket
        .par_chunks(chunk_size)
        .map(|big_chunk| {
            let mut msb_counts = vec![0usize; 256];
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
            || vec![0usize; 256],
            |mut msb_counts, msb| {
                for (i, c) in msb.into_iter().enumerate() {
                    unsafe {
                        *msb_counts.get_unchecked_mut(i) += c;
                    }
                }

                msb_counts
            },
        );

    msb_counts
}

pub fn get_counts<T>(bucket: &[T], level: usize) -> Vec<usize>
where
    T: RadixKey,
{
    let mut counts: Vec<usize> = vec![0usize; 256];
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

pub fn par_get_double_counts<T>(bucket: &[T], level_l: usize, level_r: usize) -> Vec<usize>
    where
        T: RadixKey + Sized + Send + Sync,
{
    let chunk_size = (bucket.len() / num_cpus::get()) + 1;
    let msb_counts = bucket
        .par_chunks(chunk_size)
        .map(|big_chunk| {
            let mut msb_counts = vec![0usize; 65536];
            let chunks = big_chunk.chunks_exact(8);
            let rem = chunks.remainder();

            chunks.into_iter().for_each(|chunk| unsafe {
                let a_l = chunk.get_unchecked(0).get_level(level_l) as usize;
                let b_l = chunk.get_unchecked(1).get_level(level_l) as usize;
                let c_l = chunk.get_unchecked(2).get_level(level_l) as usize;
                let d_l = chunk.get_unchecked(3).get_level(level_l) as usize;
                let e_l = chunk.get_unchecked(4).get_level(level_l) as usize;
                let f_l = chunk.get_unchecked(5).get_level(level_l) as usize;
                let g_l = chunk.get_unchecked(6).get_level(level_l) as usize;
                let h_l = chunk.get_unchecked(7).get_level(level_l) as usize;

                let a_r = chunk.get_unchecked(0).get_level(level_r) as usize;
                let b_r = chunk.get_unchecked(1).get_level(level_r) as usize;
                let c_r = chunk.get_unchecked(2).get_level(level_r) as usize;
                let d_r = chunk.get_unchecked(3).get_level(level_r) as usize;
                let e_r = chunk.get_unchecked(4).get_level(level_r) as usize;
                let f_r = chunk.get_unchecked(5).get_level(level_r) as usize;
                let g_r = chunk.get_unchecked(6).get_level(level_r) as usize;
                let h_r = chunk.get_unchecked(7).get_level(level_r) as usize;

                *msb_counts.get_unchecked_mut(a_l << 8 | a_r) += 1;
                *msb_counts.get_unchecked_mut(b_l << 8 | b_r) += 1;
                *msb_counts.get_unchecked_mut(c_l << 8 | c_r) += 1;
                *msb_counts.get_unchecked_mut(d_l << 8 | d_r) += 1;
                *msb_counts.get_unchecked_mut(e_l << 8 | e_r) += 1;
                *msb_counts.get_unchecked_mut(f_l << 8 | f_r) += 1;
                *msb_counts.get_unchecked_mut(g_l << 8 | g_r) += 1;
                *msb_counts.get_unchecked_mut(h_l << 8 | h_r) += 1;
            });

            rem.into_iter().for_each(|v| unsafe {
                let a_l = v.get_level(level_l) as usize;
                let a_r = v.get_level(level_r) as usize;
                *msb_counts.get_unchecked_mut(a_l << 8 | a_r) += 1;
            });

            msb_counts
        })
        .reduce(
            || vec![0usize; 65536],
            |mut msb_counts, msb| {
                for (i, c) in msb.into_iter().enumerate() {
                    unsafe {
                        *msb_counts.get_unchecked_mut(i) += c;
                    }
                }

                msb_counts
            },
        );

    msb_counts
}

pub fn get_double_counts<T>(bucket: &[T], level_l: usize, level_r: usize) -> Vec<usize>
    where
        T: RadixKey,
{
    let mut counts: Vec<usize> = vec![0usize; 65536];
    let chunks = bucket.chunks_exact(8);
    let rem = chunks.remainder();

    chunks.into_iter().for_each(|chunk| unsafe {
        let a_l = chunk.get_unchecked(0).get_level(level_l) as usize;
        let b_l = chunk.get_unchecked(1).get_level(level_l) as usize;
        let c_l = chunk.get_unchecked(2).get_level(level_l) as usize;
        let d_l = chunk.get_unchecked(3).get_level(level_l) as usize;
        let e_l = chunk.get_unchecked(4).get_level(level_l) as usize;
        let f_l = chunk.get_unchecked(5).get_level(level_l) as usize;
        let g_l = chunk.get_unchecked(6).get_level(level_l) as usize;
        let h_l = chunk.get_unchecked(7).get_level(level_l) as usize;

        let a_r = chunk.get_unchecked(0).get_level(level_r) as usize;
        let b_r = chunk.get_unchecked(1).get_level(level_r) as usize;
        let c_r = chunk.get_unchecked(2).get_level(level_r) as usize;
        let d_r = chunk.get_unchecked(3).get_level(level_r) as usize;
        let e_r = chunk.get_unchecked(4).get_level(level_r) as usize;
        let f_r = chunk.get_unchecked(5).get_level(level_r) as usize;
        let g_r = chunk.get_unchecked(6).get_level(level_r) as usize;
        let h_r = chunk.get_unchecked(7).get_level(level_r) as usize;

        let a = a_l << 8 | a_r;
        let b = b_l << 8 | b_r;
        let c = c_l << 8 | c_r;
        let d = d_l << 8 | d_r;
        let e = e_l << 8 | e_r;
        let f = f_l << 8 | f_r;
        let g = g_l << 8 | g_r;
        let h = h_l << 8 | h_r;

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
        let l = v.get_level(level_l) as usize;
        let r = v.get_level(level_r) as usize;
        let b = l << 8 | r;
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
