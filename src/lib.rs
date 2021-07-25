#![feature(test)]
#![feature(async_closure)]
extern crate test;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod benches;
mod radix_key;
mod arbitrary_chunks;

pub use radix_key::RadixKey;
use rayon::prelude::*;
use crate::arbitrary_chunks::*;

fn get_counts<T>(data: &[T], level: usize) -> Vec<usize>
where
    T: RadixKey + Sync
{
    if data.len() > 8192 {
        let chunk_size = (data.len() / num_cpus::get()) + 1;
        data
            .par_chunks(chunk_size)
            .fold(
                || vec![0; 256],
                |mut store, items| {
                    items.iter().for_each(|d| {
                        let val = d.get_level(level) as usize;
                        store[val] += 1;
                    });

                    store
                }
            )
            .reduce(
                || vec![0; 256],
                |mut store, d| {
                    for (i, c) in d.iter().enumerate() {
                        store[i] += c;
                    }

                    store
                }
            )
    } else {
        let mut counts = vec![0; 256];

        data.iter().for_each(|d| {
            let val = d.get_level(level) as usize;
            counts[val] += 1;
        });

        counts
    }
}

#[inline]
fn get_prefix_sums(counts: &Vec<usize>) -> Vec<usize> {
    let mut sums = Vec::with_capacity(256);

    let mut running_total = 0;
    for c in counts.iter() {
        sums.push(running_total);
        running_total += c;
    }

    sums
}

fn radix_sort_bucket<T>(bucket: &mut [T], level: usize, max_level: usize)
where
    T: RadixKey + Sized + Send + Ord + Copy + Sync,
{
    if level >= max_level || bucket.len() < 2 {
        return;
    } else if bucket.len() < 32 {
        bucket.sort_unstable();
    } else {
        let counts = get_counts(bucket, level);
        let initial_offsets = get_prefix_sums(&counts);
        let mut count_offsets = initial_offsets.to_vec();

        for current_loc in 0..bucket.len() {
            loop {
                let val = bucket[current_loc];
                let new_bucket = val.get_level(level) as usize;
                let new_loc = count_offsets[new_bucket];

                if current_loc == count_offsets[new_bucket] {
                    count_offsets[new_bucket] += 1;
                    break;
                } else if current_loc >= initial_offsets[new_bucket] && current_loc < count_offsets[new_bucket] {
                    break;
                } else {
                    let tmp = bucket[new_loc];
                    bucket[new_loc] = val;
                    bucket[current_loc] = tmp;
                    count_offsets[new_bucket] += 1;
                }
            }
        }

        drop(count_offsets);

        bucket
            .arbitrary_chunks_mut(counts)
            .par_bridge()
            .for_each(|s| radix_sort_bucket(s, level + 1, max_level));
    }
}

fn radix_sort_inner<T>(bucket: &mut [T])
where
    T: RadixKey + Sized + Send + Ord + Copy + Sync
{
    if T::LEVELS == 0 {
        panic!("RadixKey must have at least 1 level");
    }

    radix_sort_bucket(bucket, 0, T::LEVELS);
}

pub trait RadixSort {
    fn radix_sort_unstable(&mut self);
}

impl<T> RadixSort for Vec<T>
where
    T: RadixKey + Sized + Send + Ord + Copy + Sync,
{
    fn radix_sort_unstable(&mut self) {
        radix_sort_inner(self);
    }
}

impl<T> RadixSort for [T]
where
    T: RadixKey + Sized + Send + Ord + Copy + Sync,
{
    fn radix_sort_unstable(&mut self) {
        radix_sort_inner(self);
    }
}
