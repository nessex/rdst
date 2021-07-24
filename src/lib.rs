#![feature(test)]
#![feature(async_closure)]
extern crate test;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod benches;
mod radix_key;
// mod arbitrary_chunks;

pub use radix_key::RadixKey;
use rayon::prelude::*;

fn radix_sort_bucket<T>(bucket: &mut [T], level: usize, max_level: usize)
where
    T: RadixKey + Sized + Send + PartialOrd + Ord + Copy + Clone + Sync,
{
    if level >= max_level || bucket.len() < 2 {
        return;
    } else if bucket.len() < 256 {
        bucket.sort_unstable();
    } else {
        let mut new_bucket = bucket.to_vec();
        let mut counts = Vec::with_capacity(256);
        counts.resize(256, 0);

        bucket.iter().for_each(|d| {
            let val = d.get_level(level) as usize;
            counts[val] += 1;
        });

        let mut count_offsets = Vec::with_capacity(256);

        let mut running_total = 0;
        for c in counts.iter() {
            count_offsets.push(running_total);
            running_total += c;
        }

        for val in bucket.iter() {
            let bucket = val.get_level(level) as usize;
            new_bucket[count_offsets[bucket]] = *val;
            count_offsets[bucket] += 1;
        }

        bucket.copy_from_slice(&new_bucket[..]);

        let mut rem = bucket;

        for c in counts {
            let (chunk, r) = rem.split_at_mut(c);
            rem = r;
            radix_sort_bucket(chunk, level + 1, max_level);
        }
    }
}

pub struct RadixSort {}

impl RadixSort {
    pub fn sort<T>(data: &mut Vec<T>)
    where
        T: RadixKey + Sized + Send + Copy + PartialOrd + Ord + Clone + Sync,
    {
        if T::LEVELS == 0 {
            panic!("RadixKey must have at least 1 level");
        }

        radix_sort_bucket(data, 0, T::LEVELS);
    }
}
