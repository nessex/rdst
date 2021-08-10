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

pub fn par_get_msb_counts<T>(bucket: &[T]) -> Vec<usize>
where
    T: RadixKey + Sized + Send + Sync,
{
    let chunk_size = (bucket.len() / num_cpus::get()) + 1;
    let msb_counts = bucket
        .par_chunks(chunk_size)
        .map(|big_chunk| {
            let mut msb_counts = vec![0usize; 256];

            big_chunk.into_iter().for_each(|v| {
                let a = v.get_level(0) as usize;
                msb_counts[a] += 1;
            });

            msb_counts
        })
        .reduce(
            || vec![0usize; 256],
            |mut msb_counts, msb| {
                for (i, c) in msb.into_iter().enumerate() {
                    msb_counts[i] += c;
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

    bucket.iter().for_each(|v| {
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
