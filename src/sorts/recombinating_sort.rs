use crate::director::director;
use crate::sorts::out_of_place_sort::out_of_place_sort;
use crate::tuner::Tuner;
use crate::utils::*;
use crate::RadixKey;
use arbitrary_chunks::ArbitraryChunks;
use rayon::current_num_threads;
use rayon::prelude::*;

#[inline]
pub fn recombinating_sort<T>(bucket: &mut [T], tmp_bucket: &mut [T], level: usize) -> Vec<usize>
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let bucket_len = bucket.len();
    let chunk_size = (bucket_len / current_num_threads()) + 1;

    let locals: Vec<[usize; 256]> = bucket
        .par_chunks(chunk_size)
        .zip(tmp_bucket.par_chunks_mut(chunk_size))
        .map(|(chunk, tmp_chunk)| {
            let counts = get_counts(chunk, level);

            out_of_place_sort(chunk, tmp_chunk, &counts, level);

            counts
        })
        .collect();

    let mut global_counts = vec![0usize; 256];
    let mut local_indexes = Vec::with_capacity(locals.len() * 256);
    let mut local_counts = Vec::with_capacity(locals.len() * 256);

    locals.iter().for_each(|counts| {
        for (i, c) in counts.iter().enumerate() {
            if *c != 0 {
                local_indexes.push(i);
                local_counts.push(*c);
                global_counts[i] += *c;
            }
        }
    });

    let mut tmp_chunks: Vec<(usize, &mut [T])> = local_indexes
        .into_iter()
        .zip(tmp_bucket.arbitrary_chunks_mut(local_counts))
        .collect();

    // NOTE(nathan): This must be a stable sort to preserve the LSB radix sort stable property
    // This sort is used to bring all the partial radix chunks together so they can
    // be mapped to the global count regions.
    tmp_chunks.sort_by_key(|(i, _)| *i);
    let prefixes: Vec<usize> = tmp_chunks.iter().map(|(_, c)| c.len()).collect();

    bucket
        .arbitrary_chunks_mut(prefixes)
        .zip(tmp_chunks.into_iter())
        .par_bridge()
        .for_each(|(chunk, (_, tmp_chunk))| {
            chunk.copy_from_slice(tmp_chunk);
        });

    global_counts
}

pub fn recombinating_sort_adapter<T>(
    tuner: &(dyn Tuner + Send + Sync),
    in_place: bool,
    bucket: &mut [T],
    level: usize,
) where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let mut tmp_bucket = get_tmp_bucket::<T>(bucket.len());
    let global_counts = recombinating_sort(bucket, &mut tmp_bucket, level);

    if level == 0 {
        return;
    }

    director(tuner, in_place, bucket, global_counts, level - 1);
}

pub fn recombinating_sort_lsb_adapter<T>(
    bucket: &mut [T],
    start_level: usize,
    end_level: usize,
) where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    if bucket.len() < 2 {
        return;
    }

    let mut tmp_bucket = get_tmp_bucket(bucket.len());

    for l in start_level..=end_level {
        recombinating_sort(bucket, &mut tmp_bucket, l);
    }
}

#[cfg(test)]
mod tests {
    use crate::sorts::recombinating_sort::{recombinating_sort_adapter, recombinating_sort_lsb_adapter};
    use crate::test_utils::{sort_comparison_suite, NumericTest};
    use crate::tuner::DefaultTuner;

    fn test_recombinating_sort<T>(shift: T)
    where
        T: NumericTest<T>,
    {
        let tuner = DefaultTuner {};
        sort_comparison_suite(shift, |inputs| {
            recombinating_sort_adapter(&tuner, false, inputs, T::LEVELS - 1)
        });

        sort_comparison_suite(shift, |inputs| {
            recombinating_sort_lsb_adapter(inputs, 0, T::LEVELS - 1)
        });
    }

    #[test]
    pub fn test_u8() {
        test_recombinating_sort(0u8);
    }

    #[test]
    pub fn test_u16() {
        test_recombinating_sort(8u16);
    }

    #[test]
    pub fn test_u32() {
        test_recombinating_sort(16u32);
    }

    #[test]
    pub fn test_u64() {
        test_recombinating_sort(32u64);
    }

    #[test]
    pub fn test_u128() {
        test_recombinating_sort(64u128);
    }

    #[test]
    pub fn test_usize() {
        test_recombinating_sort(32usize);
    }
}
