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

#[inline]
fn get_count_vec() -> Vec<usize> {
    let mut v: Vec<usize> = Vec::with_capacity(256);
    v.resize(256, 0);

    v
}

#[inline]
fn get_counts<T>(data: &[T], level: usize) -> Vec<usize>
where
    T: RadixKey + Sync
{
    if level == 0 && data.len() > 100000 {
        data
            .par_chunks(16384)
            .fold_with(
                get_count_vec(),
                |mut store, items| {

                    items.iter().for_each(|d| {
                        let val = d.get_level(level) as usize;
                        store[val] += 1;
                    });

                    store
                }
            )
            .reduce(
                || get_count_vec(),
                |mut store, d| {
                    for (i, c) in d.iter().enumerate() {
                        store[i] += c;
                    }

                    store
                }
            )
    } else {
        let mut counts = get_count_vec();

        data.iter().for_each(|d| {
            let val = d.get_level(level) as usize;
            counts[val] += 1;
        });

        counts
    }
}

#[inline]
fn get_count_offsets(counts: &Vec<usize>) -> Vec<usize> {
    let mut count_offsets = Vec::with_capacity(256);

    let mut running_total = 0;
    for c in counts.iter() {
        count_offsets.push(running_total);
        running_total += c;
    }

    count_offsets
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
        let mut new_bucket = Vec::with_capacity(bucket.len());
        unsafe {
            // This will leave the vec with garbage data
            // however as we account for every value when placing things
            // into new_bucket, this is "safe". This is used because it provides a
            // very significant speed improvement over resize, to_vec etc.
            new_bucket.set_len(bucket.len());
        }
        let counts = get_counts(bucket, level);
        let mut count_offsets = get_count_offsets(&counts);

        for val in bucket.iter() {
            let bucket = val.get_level(level) as usize;
            new_bucket[count_offsets[bucket]] = *val;
            count_offsets[bucket] += 1;
        }

        drop(count_offsets);

        bucket.copy_from_slice(new_bucket.as_slice());

        drop(new_bucket);

        bucket
            .arbitrary_chunks_mut(counts)
            .par_bridge()
            .for_each(|s| radix_sort_bucket(s, level + 1, max_level));
    }
}

pub struct RadixSort {}

impl RadixSort {
    pub fn sort<T>(data: &mut Vec<T>)
    where
        T: RadixKey + Sized + Send + Ord + Copy + Sync,
    {
        if T::LEVELS == 0 {
            panic!("RadixKey must have at least 1 level");
        }

        radix_sort_bucket(data, 0, T::LEVELS);
    }
}
