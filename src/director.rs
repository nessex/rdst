use arbitrary_chunks::ArbitraryChunks;
use rayon::prelude::*;
use crate::sorts::lsb_radix_sort::lsb_radix_sort_adapter;
use crate::sorts::recombinating_sort::recombinating_sort;
use crate::sorts::ska_sort::ska_sort_adapter;
use crate::tuning_parameters::TuningParameters;
use crate::RadixKey;
use crate::sorts::comparative_sort::comparative_sort;
use crate::sorts::regions_sort::regions_sort;
use crate::sorts::scanning_radix_sort::scanning_radix_sort;

pub fn director<T>(
    tuning: &TuningParameters,
    inplace: bool,
    bucket: &mut [T],
    counts: Vec<usize>,
    level: usize,
) where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let len_limit = ((bucket.len() / tuning.cpus) as f64 * 1.4) as usize;
    let mut long_chunks = Vec::new();
    let mut average_chunks = Vec::with_capacity(256);

    for chunk in bucket.arbitrary_chunks_mut(counts) {
        if chunk.len() > len_limit && chunk.len() >= tuning.recombinating_sort_threshold {
            long_chunks.push(chunk);
        } else {
            average_chunks.push(chunk);
        }
    }

    long_chunks
        .into_iter()
        .for_each(|chunk| {
            if inplace {
                regions_sort(tuning, chunk, level);
            } else if chunk.len() >= tuning.scanning_sort_threshold {
                scanning_radix_sort(tuning, chunk, level, true)
            } else {
                recombinating_sort(tuning, chunk, level);
            }
        });

    average_chunks
        .into_par_iter()
        .for_each(|chunk| {
            if inplace {
                if chunk.len() <= tuning.comparative_sort_threshold {
                    comparative_sort(chunk, level);
                } else if chunk.len() <= tuning.inplace_sort_lsb_threshold {
                    lsb_radix_sort_adapter(chunk, 0, level);
                } else {
                    ska_sort_adapter(tuning, inplace, chunk, level);
                }
            } else {
                if chunk.len() >= tuning.ska_sort_threshold {
                    ska_sort_adapter(tuning, inplace, chunk, level);
                } else if chunk.len() > tuning.comparative_sort_threshold {
                    lsb_radix_sort_adapter(chunk, 0, level);
                } else {
                    comparative_sort(chunk, level);
                }
            }
        });
}
