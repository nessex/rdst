use crate::RadixKey;
use rayon::prelude::*;

#[inline]
pub fn calculate_position(msb: usize, level: usize, bucket: usize) -> usize {
    let max_msb = 256;
    let max_bucket = 256;

    (max_msb * max_bucket * level) + (max_msb * bucket) + msb
}

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
pub fn get_count_map<T>() -> Vec<usize>
where
    T: RadixKey,
{
    let mut lsb_counts: Vec<usize> = Vec::with_capacity(T::LEVELS * 256 * 256);
    lsb_counts.resize(T::LEVELS * 256 * 256, 0);

    lsb_counts
}

pub fn par_get_all_counts<T>(bucket: &[T]) -> (Vec<usize>, Vec<usize>)
where
    T: RadixKey + Sized + Send + Sync,
{
    let chunk_size = (bucket.len() / num_cpus::get()) + 1;
    let (msb_counts, lsb_counts) = bucket
        .par_chunks(chunk_size)
        .map(|big_chunk| {
            let mut msb_counts = vec![0usize; 256];
            let mut lsb_counts = get_count_map::<T>();
            let sci = big_chunk.chunks_exact(8);
            let rem = sci.remainder();

            sci.for_each(|small_chunk| {
                let a = small_chunk[0].get_level(0) as usize;
                let b = small_chunk[1].get_level(0) as usize;
                let c = small_chunk[2].get_level(0) as usize;
                let d = small_chunk[3].get_level(0) as usize;
                let e = small_chunk[4].get_level(0) as usize;
                let f = small_chunk[5].get_level(0) as usize;
                let g = small_chunk[6].get_level(0) as usize;
                let h = small_chunk[7].get_level(0) as usize;

                msb_counts[a] += 1;
                msb_counts[b] += 1;
                msb_counts[c] += 1;
                msb_counts[d] += 1;
                msb_counts[e] += 1;
                msb_counts[f] += 1;
                msb_counts[g] += 1;
                msb_counts[h] += 1;

                for i in 1..T::LEVELS {
                    let a_b = small_chunk[0].get_level(i) as usize;
                    let b_b = small_chunk[1].get_level(i) as usize;
                    let c_b = small_chunk[2].get_level(i) as usize;
                    let d_b = small_chunk[3].get_level(i) as usize;
                    let e_b = small_chunk[4].get_level(i) as usize;
                    let f_b = small_chunk[5].get_level(i) as usize;
                    let g_b = small_chunk[6].get_level(i) as usize;
                    let h_b = small_chunk[7].get_level(i) as usize;

                    let a_pos = calculate_position(a, i - 1, a_b);
                    let b_pos = calculate_position(b, i - 1, b_b);
                    let c_pos = calculate_position(c, i - 1, c_b);
                    let d_pos = calculate_position(d, i - 1, d_b);
                    let e_pos = calculate_position(e, i - 1, e_b);
                    let f_pos = calculate_position(f, i - 1, f_b);
                    let g_pos = calculate_position(g, i - 1, g_b);
                    let h_pos = calculate_position(h, i - 1, h_b);

                    lsb_counts[a_pos] += 1;
                    lsb_counts[b_pos] += 1;
                    lsb_counts[c_pos] += 1;
                    lsb_counts[d_pos] += 1;
                    lsb_counts[e_pos] += 1;
                    lsb_counts[f_pos] += 1;
                    lsb_counts[g_pos] += 1;
                    lsb_counts[h_pos] += 1;
                }
            });

            rem.into_iter().for_each(|v| {
                let a = v.get_level(0) as usize;
                msb_counts[a] += 1;

                for i in 1..T::LEVELS {
                    let a_b = v.get_level(i) as usize;
                    let a_pos = calculate_position(a, i - 1, a_b);
                    lsb_counts[a_pos] += 1;
                }
            });

            (msb_counts, lsb_counts)
        })
        .reduce(
            || (vec![0usize; 256], get_count_map::<T>()),
            |(mut msb_counts, mut store), (msb, lsb)| {
                for (i, c) in msb.into_iter().enumerate() {
                    msb_counts[i] += c;
                }

                for (i, c) in lsb.into_iter().enumerate() {
                    store[i] += c;
                }

                (msb_counts, store)
            },
        );

    (msb_counts, lsb_counts)
}

pub fn get_all_counts<T>(bucket: &[T]) -> (Vec<usize>, Vec<usize>)
where
    T: RadixKey,
{
    let mut msb_counts: Vec<usize> = vec![0usize; 256];
    let mut lsb_counts: Vec<usize> = get_count_map::<T>();

    bucket.iter().for_each(|v| {
        let msb = v.get_level(0) as usize;
        msb_counts[msb] += 1;

        for i in 1..T::LEVELS {
            let b = v.get_level(i);
            let pos = calculate_position(msb, i - 1, b as usize);
            lsb_counts[pos] += 1;
        }
    });

    (msb_counts, lsb_counts)
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
