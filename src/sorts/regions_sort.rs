//! Regions Sort
//!
//! Based on:
//! Omar Obeya, Endrias Kahssay, Edward Fan, and Julian Shun.
//! Theoretically-Efficient and Practical Parallel In-Place Radix Sorting.
//! In ACM Symposium on Parallelism in Algorithms and Architectures (SPAA), 2019.
//!
//! Summary:
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
//!             4.2.2.3: Swap items in C heading to O, with items in I destined for C (items in C may or may not be destined for O ultimately)

use crate::director::director;
use crate::sorts::ska_sort::ska_sort;
use crate::tuner::Tuner;
use crate::utils::*;
use crate::RadixKey;
use partition::partition_index;
use rayon::current_num_threads;
use rayon::prelude::*;
use std::cmp::{min, Ordering};

/// Operation represents a pair of edges, which have content slices that need to be swapped.
struct Operation<'bucket, T>(Edge<'bucket, T>, Edge<'bucket, T>);

/// Edge represents an outbound bit of data from a "country", an edge in the regions
/// graph. A "country" refers to a space which has been determined to be reserved for a particular
/// byte value in the final array.
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
) -> Vec<Edge<'bucket, T>> {
    let mut outbounds: Vec<Edge<T>> = Vec::new();
    let mut rem_bucket = bucket;
    let mut local_bucket = 0;
    let mut local_country = 0;
    let mut global_country = 0;
    let mut target_global_dist = global_counts[0];
    let mut target_local_dist = local_counts[0][0];

    while !(global_country == 255 && local_country == 255 && local_bucket == local_counts.len() - 1)
    {
        let step = min(target_global_dist, target_local_dist);

        // 1. Add the current step to the outbounds
        if step != 0 {
            let (slice, rem) = rem_bucket.split_at_mut(step);
            rem_bucket = rem;

            if local_country != global_country {
                outbounds.push(Edge {
                    dst: local_country,
                    init: global_country,
                    slice,
                });
            }
        }

        // 2. Update target_global_dist
        if step == target_global_dist && global_country < 255 {
            global_country += 1;
            target_global_dist = global_counts[global_country];
        } else {
            target_global_dist -= step;
        }

        // 3. Update target_local_dist
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
    }

    outbounds
}

/// list_operations takes the lists of outbounds and turns it into a list of swaps to perform
fn list_operations<T>(
    country: usize,
    mut outbounds: Vec<Edge<T>>,
) -> (Vec<Edge<T>>, Vec<Operation<T>>) {
    // 1. Extract current country outbounds from full outbounds list
    // NOTE(nathan): Partitioning a single array benched faster than
    // keeping an array per country (256 arrays total).
    let ob = partition_index(&mut outbounds, |e| e.init != country);
    let mut current_outbounds = outbounds.split_off(ob);

    // 2. Calculate inbounds for country
    let p = partition_index(&mut outbounds, |e| e.dst != country);
    let mut inbounds = outbounds.split_off(p);

    // 3. Pair up inbounds & outbounds into an operation, returning unmatched data to the working arrays
    let mut operations = Vec::new();

    loop {
        let i = match inbounds.pop() {
            Some(i) => i,
            None => {
                outbounds.append(&mut current_outbounds);
                break;
            }
        };

        let o = match current_outbounds.pop() {
            Some(o) => o,
            None => {
                outbounds.push(i);
                outbounds.append(&mut inbounds);
                break;
            }
        };

        let op = match i.slice.len().cmp(&o.slice.len()) {
            Ordering::Equal => Operation(i, o),
            Ordering::Less => {
                let (sl, rem) = o.slice.split_at_mut(i.slice.len());

                current_outbounds.push(Edge {
                    dst: o.dst,
                    init: o.init,
                    slice: rem,
                });

                let o = Edge {
                    dst: o.dst,
                    init: o.init,
                    slice: sl,
                };

                Operation(i, o)
            }
            Ordering::Greater => {
                let (sl, rem) = i.slice.split_at_mut(o.slice.len());

                inbounds.push(Edge {
                    dst: i.dst,
                    init: i.init,
                    slice: rem,
                });

                let i = Edge {
                    dst: i.dst,
                    init: i.init,
                    slice: sl,
                };

                Operation(i, o)
            }
        };

        operations.push(op);
    }

    // 4. Return the paired operations
    (outbounds, operations)
}

pub fn regions_sort<T>(bucket: &mut [T], level: usize) -> Vec<usize>
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    if bucket.len() == 0 {
        return get_counts(bucket, level).to_vec();
    }

    let threads = current_num_threads();
    let bucket_len = bucket.len();
    let chunk_size = (bucket_len / threads) + 1;
    let local_counts: Vec<[usize; 256]> = bucket
        .par_chunks_mut(chunk_size)
        .map(|chunk| {
            let counts = get_counts(chunk, level);
            let plateaus = detect_plateaus(chunk, level);
            let (mut prefix_sums, mut end_offsets) = apply_plateaus(chunk, &counts, &plateaus);
            ska_sort(chunk, &mut prefix_sums, &mut end_offsets, level);

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
    let mut operations = Vec::new();

    // This loop calculates and executes all operations that can be done in parallel, each pass.
    loop {
        if outbounds.is_empty() {
            break;
        }

        // List out all the operations that need to be executed in this pass
        for country in 0..256 {
            let (new_outbounds, mut new_ops) = list_operations(country, outbounds);
            outbounds = new_outbounds;
            operations.append(&mut new_ops);
        }

        if operations.is_empty() {
            break;
        }

        // Execute all operations, swapping the paired slices (inbound/outbound edges)
        let chunk_size = (operations.len() / threads) + 1;
        operations.par_chunks_mut(chunk_size).for_each(|chunk| {
            for Operation(o, i) in chunk {
                i.slice.swap_with_slice(o.slice)
            }
        });

        // Create new edges for edges that were swapped somewhere other than their final destination
        for Operation(i, mut o) in std::mem::take(&mut operations) {
            if o.dst != i.init {
                o.init = i.init;
                o.slice = i.slice;
                outbounds.push(o);
            }
        }
    }

    global_counts
}

pub fn regions_sort_adapter<T>(
    tuner: &(dyn Tuner + Send + Sync),
    in_place: bool,
    bucket: &mut [T],
    level: usize,
) where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    if bucket.len() <= 1 {
        return;
    }

    let global_counts = regions_sort(bucket, level);

    if level == 0 {
        return;
    }

    director(tuner, in_place, bucket, global_counts, level - 1);
}

#[cfg(test)]
mod tests {
    use crate::sorts::regions_sort::regions_sort_adapter;
    use crate::test_utils::{sort_comparison_suite, NumericTest};
    use crate::tuner::DefaultTuner;

    fn test_regions_sort<T>(shift: T)
    where
        T: NumericTest<T>,
    {
        let tuner = DefaultTuner {};
        sort_comparison_suite(shift, |inputs| {
            regions_sort_adapter(&tuner, true, inputs, T::LEVELS - 1)
        });
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
