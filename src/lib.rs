#![feature(test)]
extern crate test;

use rayon::prelude::*;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod benches;

pub trait RadixKey {
    const LEVELS: usize;

    fn get_level(&self, level: usize) -> u8;
}

fn radix_sort_bucket<T>(bucket: Vec<T>, level: usize, max_level: usize) -> Vec<T>
where
    T: RadixKey + Sized + Send,
{
    if level >= max_level || bucket.len() < 2 {
        bucket
    } else {
        let mut tmp_buckets: Vec<Vec<T>> = Vec::with_capacity(256);
        tmp_buckets.resize_with(256, || Vec::new());

        bucket.into_iter().for_each(|d| {
            let val = d.get_level(level) as usize;
            tmp_buckets[val].push(d);
        });

        tmp_buckets
            .into_iter()
            .flat_map(|bucket| radix_sort_bucket(bucket, level + 1, max_level))
            .collect()
    }
}

pub struct RadixSort {}

impl RadixSort {
    pub fn sort<T>(data: &mut Vec<T>)
    where
        T: RadixKey + Sized + Send + Copy,
    {
        if T::LEVELS == 0 {
            panic!("RadixKey must have at least 1 level");
        }

        let mut buckets: Vec<Vec<T>> = Vec::with_capacity(256);
        buckets.resize_with(256, || Vec::new());

        data.iter().for_each(|d| {
            let val = d.get_level(0) as usize;
            buckets[val].push(*d);
        });

        *data = buckets
            .into_par_iter()
            .flat_map(|bucket| radix_sort_bucket(bucket, 1, T::LEVELS))
            .collect()
    }
}
