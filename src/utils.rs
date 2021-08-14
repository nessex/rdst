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

            rem.into_iter().for_each(|v| {
                let a = v.get_level(level) as usize;
                msb_counts[a] += 1;
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

    rem.into_iter().for_each(|v| {
        let b = v.get_level(level) as usize;
        counts[b] += 1;
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
