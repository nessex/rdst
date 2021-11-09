use crate::director::director;
use crate::sorts::out_of_place_sort::out_of_place_sort;
use crate::tuner::Tuner;
use crate::utils::*;
use crate::RadixKey;
use arbitrary_chunks::ArbitraryChunks;
use rayon::current_num_threads;
use rayon::prelude::*;

pub fn recombinating_sort<T>(bucket: &mut [T], level: usize) -> Vec<usize>
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let bucket_len = bucket.len();
    let chunk_size = (bucket_len / current_num_threads()) + 1;
    let mut tmp_bucket = get_tmp_bucket::<T>(bucket_len);

    let locals: Vec<([usize; 256], [usize; 256])> = bucket
        .par_chunks(chunk_size)
        .zip(tmp_bucket.par_chunks_mut(chunk_size))
        .map(|(chunk, tmp_chunk)| {
            let counts = get_counts(chunk, level);

            out_of_place_sort(chunk, tmp_chunk, &counts, level);

            let sums = get_prefix_sums(&counts);

            (counts, sums)
        })
        .collect();

    let mut global_counts = vec![0usize; 256];

    locals.iter().for_each(|(counts, _)| {
        for (i, c) in counts.iter().enumerate() {
            global_counts[i] += *c;
        }
    });

    bucket
        .arbitrary_chunks_mut(global_counts.clone())
        .enumerate()
        .par_bridge()
        .for_each(|(index, global_chunk)| {
            let mut read_offset = 0;
            let mut write_offset = 0;

            for (counts, sums) in locals.iter() {
                let read_start = read_offset + sums[index];
                let read_end = read_start + counts[index];
                let read_slice = &tmp_bucket[read_start..read_end];
                let write_end = write_offset + read_slice.len();

                global_chunk[write_offset..write_end].copy_from_slice(read_slice);

                read_offset += chunk_size;
                write_offset = write_end;
            }
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
    let global_counts = recombinating_sort(bucket, level);

    if level == 0 {
        return;
    }

    director(tuner, in_place, bucket, global_counts, level - 1);
}

#[cfg(test)]
mod tests {
    use crate::sorts::recombinating_sort::recombinating_sort_adapter;
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
