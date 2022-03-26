//! `recombinating_sort` is a multi-threaded, out-of-place, unstable radix sort unique to rdst. It
//! operates on a set of tiles, which aresub-sections of the original data of roughly the same size.
//!
//! It works by:
//!  1. Sorting each tile out-of-place into a temp array
//!  2. Calculating prefix sums of each tile
//!  3. Splitting the output array based upon the aggregated counts of all tiles
//!  4. Writing out the final data for each global count ("country" in regions sort terminology) in parallel
//!
//! ## Characteristics
//!
//!  * out-of-place
//!  * multi-threaded
//!  * unstable
//!
//! ## Performance
//!
//! This is typically the best performing multi-threaded sorting algorithm until you hit memory
//! constraints. As this is an out-of-place algorithm, you need 2n memory relative to the input for
//! this sort, and eventually the extra allocation and freeing required eats away at the performance.

use crate::sorter::Sorter;
use crate::sorts::out_of_place_sort::out_of_place_sort;
use crate::utils::*;
use crate::RadixKey;
use arbitrary_chunks::ArbitraryChunks;
use rayon::prelude::*;

pub fn recombinating_sort<T>(
    bucket: &mut [T],
    counts: &[usize; 256],
    tile_counts: &[[usize; 256]],
    tile_size: usize,
    level: usize,
) where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let bucket_len = bucket.len();
    let mut tmp_bucket = get_tmp_bucket::<T>(bucket_len);

    let locals: Vec<([usize; 256], [usize; 256])> = bucket
        .par_chunks(tile_size)
        .zip(tmp_bucket.par_chunks_mut(tile_size))
        .zip(tile_counts.into_par_iter())
        .map(|((chunk, tmp_chunk), counts)| {
            out_of_place_sort(chunk, tmp_chunk, counts, level);

            let sums = get_prefix_sums(&*counts);

            (*counts, sums)
        })
        .collect();

    bucket
        .arbitrary_chunks_mut(counts)
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

                read_offset += tile_size;
                write_offset = write_end;
            }
        });
}

impl<'a> Sorter<'a> {
    pub(crate) fn recombinating_sort_adapter<T>(
        &self,
        bucket: &mut [T],
        counts: &[usize; 256],
        tile_counts: &[[usize; 256]],
        tile_size: usize,
        level: usize,
    ) where
        T: RadixKey + Sized + Send + Copy + Sync,
    {
        if bucket.len() < 2 {
            return;
        }

        recombinating_sort(bucket, counts, tile_counts, tile_size, level);

        if level == 0 {
            return;
        }

        self.director(bucket, counts, level - 1);
    }
}

#[cfg(test)]
mod tests {
    use crate::sorter::Sorter;
    use crate::tuners::StandardTuner;
    use crate::utils::test_utils::{sort_comparison_suite, NumericTest};
    use crate::utils::{aggregate_tile_counts, cdiv, get_tile_counts};
    use rayon::current_num_threads;

    fn test_recombinating_sort<T>(shift: T)
    where
        T: NumericTest<T>,
    {
        let sorter = Sorter::new(true, &StandardTuner);

        sort_comparison_suite(shift, |inputs| {
            let level = T::LEVELS - 1;
            let tile_size = cdiv(inputs.len(), current_num_threads());

            if inputs.len() == 0 {
                return;
            }

            let (tile_counts, _) = get_tile_counts(inputs, tile_size, level);
            let counts = aggregate_tile_counts(&tile_counts);

            sorter.recombinating_sort_adapter(
                inputs,
                &counts,
                &tile_counts,
                tile_size,
                T::LEVELS - 1,
            )
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
