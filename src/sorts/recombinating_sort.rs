use crate::director::director;
use crate::sorts::lsb_radix_sort::lsb_radix_sort_adapter;
use crate::sorts::ska_sort::ska_sort;
use crate::utils::*;
use crate::RadixKey;
use arbitrary_chunks::ArbitraryChunks;
use rayon::prelude::*;
use crate::tuning_parameters::TuningParameters;

pub fn recombinating_sort<T>(tuning: &TuningParameters, bucket: &mut [T], level: usize)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let mut tmp_bucket: Vec<T> = get_tmp_bucket::<T>(bucket.len());

    let threads = num_cpus::get();
    let chunk_size = (bucket.len() / threads) + 1;

    let locals: Vec<([usize; 256], [usize; 256])> = bucket
        .par_chunks_mut(chunk_size)
        .map(|chunk| {
            let counts = get_counts(chunk, level);
            ska_sort(chunk, &counts, level);

            let sums = get_prefix_sums(&counts);

            (counts, sums)
        })
        .collect();

    let mut global_counts = [0usize; 256];

    locals.iter().for_each(|(counts, _)| {
        for (i, c) in counts.iter().enumerate() {
            global_counts[i] += *c;
        }
    });

    tmp_bucket
        .arbitrary_chunks_mut(global_counts.to_vec())
        .enumerate()
        .par_bridge()
        .for_each(|(index, global_chunk)| {
            let mut read_offset = 0;
            let mut write_offset = 0;

            for (counts, sums) in locals.iter() {
                let read_start = read_offset + sums[index];
                let read_end = read_start + counts[index];
                let read_slice = &bucket[read_start..read_end];
                let write_end = write_offset + read_slice.len();

                global_chunk[write_offset..write_end].copy_from_slice(&read_slice);

                read_offset += chunk_size;
                write_offset = write_end;
            }
        });

    bucket
        .par_chunks_mut(chunk_size)
        .zip(tmp_bucket.par_chunks(chunk_size))
        .for_each(|(a, b)| {
            a.copy_from_slice(&b[..]);
        });

    drop(tmp_bucket);

    if level == 0 {
        return;
    }

    let len = bucket.len();

    bucket
        .arbitrary_chunks_mut(global_counts.to_vec())
        .par_bridge()
        .for_each(|chunk| director(tuning, chunk, len, level - 1));
}

#[cfg(test)]
mod tests {
    use crate::test_utils::sort_comparison_suite;
    use crate::{RadixKey, RadixSort};
    use nanorand::{RandomGen, WyRand};
    use std::fmt::Debug;
    use std::ops::{Shl, Shr};
    use crate::tuning_parameters::TuningParameters;
    use crate::sorts::recombinating_sort::recombinating_sort;

    fn test_recombinating_sort<T>(shift: T)
    where
        T: RadixKey
        + Ord
        + RandomGen<WyRand>
        + Clone
        + Debug
        + Send
        + Sized
        + Copy
        + Sync
        + Shl<Output = T>
        + Shr<Output = T>,
    {
        let tuning = TuningParameters::new(T::LEVELS);
        sort_comparison_suite(shift, |inputs| recombinating_sort(&tuning, inputs, T::LEVELS - 1));
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
