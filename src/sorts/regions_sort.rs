//! Regions Sort
//!
//! 1. Split into buckets
//! 2. Compute counts for each bucket and sort each bucket in-place
//! 3. Generate global counts
//! 4. Generate Graph & Sort
//!     4.1 List outbound regions for each country
//!     4.2 For each country (C):
//!         4.2.1: List the inbounds for C (filter outbounds for each other country by destination: C)
//!         4.2.2: For each thread:
//!             4.2.2.1: Pop an item off the inbound (country: I) & outbound (country: O) queues for C
//!             4.2.2.2/a: If they are the same size, continue
//!             4.2.2.2/b: If I is bigger than O, keep the remainder of I in the queue and continue
//!             4.2.2.2/c: If O is bigger than I, keep the remainder of O in the queue and continue
//!             4.2.2.2/add.: If a remainder has been kept in the queue
//!             4.2.2.3: Swap items in C destined for O with items in I destined for C

use crate::director::director;
use crate::sorts::ska_sort::ska_sort;
use crate::tuning_parameters::TuningParameters;
use crate::utils::*;
use crate::RadixKey;
use arbitrary_chunks::ArbitraryChunks;
use rayon::prelude::*;
use std::cmp::{min, Ordering};

struct Edge<'bucket, T> {
    /// dst is the destination country index
    dst: usize,
    /// init is the initial country index
    init: usize,
    slice: &'bucket mut [T],
}

/// generate_outbounds generates a Vec for each country containing all the outbound edges
/// for that country.
fn generate_outbounds<'bucket, T>(
    bucket: &'bucket mut [T],
    local_counts: &[[usize; 256]],
    global_counts: &[usize],
) -> Vec<Vec<Edge<'bucket, T>>> {
    let mut outbounds: Vec<Vec<Edge<T>>> = Vec::new();
    outbounds.resize_with(256, Vec::new);

    let mut rem_bucket = bucket;
    let mut local_bucket = 0;
    let mut local_country = 0;
    let mut global_country = 0;
    let mut target_global_dist = global_counts[0];
    let mut target_local_dist = local_counts[0][0];

    while !(global_country == 255 && local_country == 255 && local_bucket == local_counts.len() - 1)
    {
        let step = min(target_global_dist, target_local_dist);
        let dst_global = global_country;
        let dst_local = local_country;

        if step == target_global_dist && global_country < 255 {
            global_country += 1;
            target_global_dist = global_counts[global_country];
        } else {
            target_global_dist -= step;
        }

        if step == target_local_dist
            && !(local_bucket == local_counts.len() - 1 && local_country == 255)
        {
            if local_country < 255 {
                local_country += 1;
            } else {
                local_bucket += 1;
                local_country = 0;
            }

            target_local_dist = local_counts[local_bucket][local_country];
        } else {
            target_local_dist -= step;
        }

        if step != 0 {
            let (slice, rem) = rem_bucket.split_at_mut(step);
            rem_bucket = rem;

            if dst_local != dst_global {
                outbounds[dst_global].push(Edge {
                    dst: dst_local,
                    init: dst_global,
                    slice,
                });
            }
        }
    }

    outbounds
}

/// list_operations takes the lists of outbounds and turns it into a list of swaps to perform
fn list_operations<T>(
    country: usize,
    mut outbounds: Vec<Vec<Edge<T>>>,
) -> (Vec<Vec<Edge<T>>>, Vec<(Edge<T>, Edge<T>)>) {
    let mut inbounds = Vec::new();
    let mut current_outbounds = std::mem::take(&mut outbounds[country]);
    let mut new_outbounds = Vec::new();

    // 1. Calculate inbounds for country
    for country_outbound in outbounds {
        let mut new_outbound_country = Vec::new();

        for edge in country_outbound {
            if edge.dst == country {
                inbounds.push(edge);
            } else {
                new_outbound_country.push(edge);
            }
        }

        new_outbounds.push(new_outbound_country);
    }

    // 2. Sort inbounds & outbounds for country
    // inbounds.sort_unstable_by_key(|v| v.slice.len());
    // current_outbounds.sort_unstable_by_key(|v| v.slice.len());

    // 3. Pair up inbounds & outbounds into an operation, returning unmatched data to the working arrays
    let mut operations = Vec::new();

    while let Some(i) = inbounds.pop() {
        let o = match current_outbounds.pop() {
            Some(o) => o,
            None => break,
        };

        let op = match i.slice.len().cmp(&o.slice.len()) {
            Ordering::Equal => (i, o),
            Ordering::Less => {
                let (sl, rem) = o.slice.split_at_mut(i.slice.len());
                let trunc = Edge {
                    dst: o.dst,
                    init: o.init,
                    slice: rem,
                };
                current_outbounds.push(trunc);

                (
                    i,
                    Edge {
                        dst: o.dst,
                        init: o.init,
                        slice: sl,
                    },
                )
            }
            Ordering::Greater => {
                let (sl, rem) = i.slice.split_at_mut(o.slice.len());
                let trunc = Edge {
                    dst: i.dst,
                    init: i.init,
                    slice: rem,
                };
                inbounds.push(trunc);

                (
                    Edge {
                        dst: i.dst,
                        init: i.init,
                        slice: sl,
                    },
                    o,
                )
            }
        };

        operations.push(op);
    }

    // 4. Return the paired operations
    operations.sort_unstable_by_key(|op| op.0.slice.len());
    (new_outbounds, operations)
}

pub fn regions_sort<T>(tuning: &TuningParameters, bucket: &mut [T], level: usize)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let bucket_len = bucket.len();
    let chunk_size = (bucket_len / tuning.cpus) + 1;

    let local_counts: Vec<[usize; 256]> = bucket
        .par_chunks_mut(chunk_size)
        .map(|chunk| {
            let counts = get_counts(chunk, level);
            ska_sort(chunk, &counts, level);

            counts
        })
        .collect();

    let mut global_counts = vec![0usize; 256];

    local_counts.iter().for_each(|counts| {
        for (i, c) in counts.iter().enumerate() {
            global_counts[i] += *c;
        }
    });

    let mut outbounds = generate_outbounds(bucket, &local_counts, &global_counts);

    for country in 0..256 {
        let (new_outbounds, mut operations) = list_operations(country, outbounds);
        outbounds = new_outbounds;

        operations.par_iter_mut().for_each(|(o, i)| {
            i.slice.swap_with_slice(o.slice);
        });

        // Create new edges for edges that were swapped to the wrong place
        for (i, mut o) in operations {
            if o.dst != i.init {
                o.init = i.init;
                o.slice = i.slice;
                outbounds[i.init].push(o);
            }
        }
    }

    if level == 0 {
        return;
    }

    bucket
        .arbitrary_chunks_mut(global_counts)
        .par_bridge()
        .for_each(|chunk| director(tuning, chunk, bucket_len, level - 1));
}

#[cfg(test)]
mod tests {
    use crate::sorts::regions_sort::regions_sort;
    use crate::test_utils::{sort_comparison_suite, NumericTest};
    use crate::tuning_parameters::TuningParameters;

    fn test_regions_sort<T>(shift: T)
    where
        T: NumericTest<T>,
    {
        let tuning = TuningParameters::new(T::LEVELS);
        sort_comparison_suite(shift, |inputs| regions_sort(&tuning, inputs, T::LEVELS - 1));
    }

    #[test]
    pub fn test_u8() {
        test_regions_sort(0u8);
    }

    #[test]
    pub fn test_u16() {
        test_regions_sort(8u16);
    }

    #[test]
    pub fn test_u32() {
        test_regions_sort(16u32);
    }

    #[test]
    pub fn test_u64() {
        test_regions_sort(32u64);
    }

    #[test]
    pub fn test_u128() {
        test_regions_sort(64u128);
    }

    #[test]
    pub fn test_usize() {
        test_regions_sort(32usize);
    }
}
