//! # rdst
//!
//! rdst is a flexible native Rust implementation of unstable radix sort.
//!
//! ## Usage
//!
//! In the simplest case, you can use this sort by simply calling `my_vec.radix_sort_unstable()`. If you have a custom type to sort, you may need to implement `RadixKey` for that type.
//!
//! ## Default Implementations
//!
//! `RadixKey` is implemented for `Vec` of the following types out-of-the-box:
//!
//!  * `u8`
//!  * `u16`
//!  * `u32`
//!  * `u64`
//!  * `u128`
//!  * `usize`
//!  * `[u8; N]`
//!
//! ### Implementing `RadixKey`
//!
//! To be able to sort custom types, implement `RadixKey` as below.
//!
//!  * `LEVELS` should be set to the total number of bytes you will consider for each item being sorted
//!  * `get_level` should return the corresponding bytes in the order you would like them to be sorted. This library is intended to be used starting from the MSB (most significant bits).
//!
//! Note that this also allows you to implement radix keys that span multiple values.
//!
//! ```ignore
//! use rdst::RadixKey;
//!
//! impl RadixKey for u16 {
//!     const LEVELS: usize = 2;
//!
//!     #[inline]
//!     fn get_level(&self, level: usize) -> u8 {
//!         let b = self.to_le_bytes();
//!
//!         match level {
//!             0 => b[1],
//!             _ => b[0],
//!         }
//!     }
//! }
//! ```
//!
//! ## License
//!
//! Licensed under either of
//!
//! * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
//! * MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
//!
//! at your option.
//!
//! ### Contribution
//!
//! Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

// XXX: Required by benches
// uncomment to run `cargo bench`
// #![feature(test)]

#[cfg(all(test, feature = "bench"))]
extern crate test;

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "bench"))]
mod benches;
mod radix_key;

use arbitrary_chunks::ArbitraryChunks;
pub use radix_key::RadixKey;
use rayon::prelude::*;

#[inline]
fn calculate_position(msb: usize, level: usize, bucket: usize) -> usize {
    let max_msb = 256;
    let max_bucket = 256;

    (max_msb * max_bucket * level) + (max_msb * bucket) + msb
}

#[inline]
fn get_prefix_sums(counts: &[usize]) -> Vec<usize> {
    let mut sums = Vec::with_capacity(256);

    let mut running_total = 0;
    for c in counts.iter() {
        sums.push(running_total);
        running_total += c;
    }

    sums
}

#[inline]
fn get_count_map<T>() -> Vec<usize>
where
    T: RadixKey,
{
    let mut lsb_counts: Vec<usize> = Vec::with_capacity(T::LEVELS * 256 * 256);
    lsb_counts.resize(T::LEVELS * 256 * 256, 0);

    lsb_counts
}

fn par_get_all_counts<T>(bucket: &[T]) -> (Vec<usize>, Vec<usize>)
where
    T: RadixKey + Sized + Send + Sync,
{
    let chunk_size = (bucket.len() / num_cpus::get()) + 1;
    let (msb_counts, lsb_counts) = bucket
        .par_chunks(chunk_size)
        .map(|big_chunk| {
            let mut msb_counts = vec![0usize; 256];
            let mut lsb_counts = get_count_map::<T>();
            let sci = big_chunk.chunks_exact(8);
            let rem = sci.remainder();

            sci.for_each(|small_chunk| {
                let a = small_chunk[0].get_level(0) as usize;
                let b = small_chunk[1].get_level(0) as usize;
                let c = small_chunk[2].get_level(0) as usize;
                let d = small_chunk[3].get_level(0) as usize;
                let e = small_chunk[4].get_level(0) as usize;
                let f = small_chunk[5].get_level(0) as usize;
                let g = small_chunk[6].get_level(0) as usize;
                let h = small_chunk[7].get_level(0) as usize;

                msb_counts[a] += 1;
                msb_counts[b] += 1;
                msb_counts[c] += 1;
                msb_counts[d] += 1;
                msb_counts[e] += 1;
                msb_counts[f] += 1;
                msb_counts[g] += 1;
                msb_counts[h] += 1;

                for i in 1..T::LEVELS {
                    let a_b = small_chunk[0].get_level(i) as usize;
                    let b_b = small_chunk[1].get_level(i) as usize;
                    let c_b = small_chunk[2].get_level(i) as usize;
                    let d_b = small_chunk[3].get_level(i) as usize;
                    let e_b = small_chunk[4].get_level(i) as usize;
                    let f_b = small_chunk[5].get_level(i) as usize;
                    let g_b = small_chunk[6].get_level(i) as usize;
                    let h_b = small_chunk[7].get_level(i) as usize;

                    let a_pos = calculate_position(a, i - 1, a_b);
                    let b_pos = calculate_position(b, i - 1, b_b);
                    let c_pos = calculate_position(c, i - 1, c_b);
                    let d_pos = calculate_position(d, i - 1, d_b);
                    let e_pos = calculate_position(e, i - 1, e_b);
                    let f_pos = calculate_position(f, i - 1, f_b);
                    let g_pos = calculate_position(g, i - 1, g_b);
                    let h_pos = calculate_position(h, i - 1, h_b);

                    lsb_counts[a_pos] += 1;
                    lsb_counts[b_pos] += 1;
                    lsb_counts[c_pos] += 1;
                    lsb_counts[d_pos] += 1;
                    lsb_counts[e_pos] += 1;
                    lsb_counts[f_pos] += 1;
                    lsb_counts[g_pos] += 1;
                    lsb_counts[h_pos] += 1;
                }
            });

            rem.into_iter().for_each(|v| {
                let a = v.get_level(0) as usize;
                msb_counts[a] += 1;

                for i in 1..T::LEVELS {
                    let a_b = v.get_level(i) as usize;
                    let a_pos = calculate_position(a, i - 1, a_b);
                    lsb_counts[a_pos] += 1;
                }
            });

            (msb_counts, lsb_counts)
        })
        .reduce(
            || (vec![0usize; 256], get_count_map::<T>()),
            |(mut msb_counts, mut store), (msb, lsb)| {
                for (i, c) in msb.into_iter().enumerate() {
                    msb_counts[i] += c;
                }

                for (i, c) in lsb.into_iter().enumerate() {
                    store[i] += c;
                }

                (msb_counts, store)
            },
        );

    (msb_counts, lsb_counts)
}

fn get_all_counts<T>(bucket: &[T]) -> (Vec<usize>, Vec<usize>)
where
    T: RadixKey,
{
    let mut msb_counts: Vec<usize> = vec![0usize; 256];
    let mut lsb_counts: Vec<usize> = get_count_map::<T>();

    bucket.iter().for_each(|v| {
        let msb = v.get_level(0) as usize;
        msb_counts[msb] += 1;

        for i in 1..T::LEVELS {
            let b = v.get_level(i);
            let pos = calculate_position(msb, i - 1, b as usize);
            lsb_counts[pos] += 1;
        }
    });

    (msb_counts, lsb_counts)
}

#[inline]
fn get_tmp_bucket<T>(len: usize) -> Vec<T> {
    let mut tmp_bucket = Vec::with_capacity(len);
    unsafe {
        // This will leave the vec with garbage data
        // however as we account for every value when placing things
        // into tmp_bucket, this is "safe". This is used because it provides a
        // very significant speed improvement over resize, to_vec etc.
        tmp_bucket.set_len(len);
    }

    tmp_bucket
}

fn radix_sort_bucket_start<T>(bucket: &mut [T])
where
    T: RadixKey + Sized + Send + Ord + Copy + Sync,
{
    if bucket.len() < 32 {
        bucket.sort_unstable();
        return;
    }

    let mut tmp_bucket = get_tmp_bucket(bucket.len());
    let (msb_counts, lsb_counts) = if bucket.len() > 1_000_000 {
        par_get_all_counts(bucket)
    } else {
        get_all_counts(bucket)
    };
    radix_sort_bucket(bucket, &mut tmp_bucket, msb_counts, &lsb_counts);
}

fn radix_sort_bucket<T>(
    bucket: &mut [T],
    tmp_bucket: &mut [T],
    msb_counts: Vec<usize>,
    lsb_counts: &[usize],
) where
    T: RadixKey + Sized + Send + Ord + Copy + Sync,
{
    let level = 0;
    let mut prefix_sums = get_prefix_sums(&msb_counts);

    bucket.into_iter().for_each(|val| {
        let bucket = val.get_level(level) as usize;
        unsafe {
            // As prefix_sums is always exactly 256 elements long
            // and get_level() returns a byte, this is always valid.
            // This provides a significant speedup.
            let sum = prefix_sums.get_unchecked_mut(bucket);
            tmp_bucket[*sum] = *val;
            *sum += 1;
        }
    });

    drop(prefix_sums);
    bucket.copy_from_slice(tmp_bucket);

    bucket
        .arbitrary_chunks_mut(msb_counts.clone())
        .zip(tmp_bucket.arbitrary_chunks_mut(msb_counts))
        .enumerate()
        .par_bridge()
        .for_each(|(msb, (c, t))| {
            lsb_radix_sort_bucket(c, t, T::LEVELS - 1, msb, lsb_counts);
        });
}

fn lsb_radix_sort_bucket<T>(
    bucket: &mut [T],
    tmp_bucket: &mut [T],
    level: usize,
    msb: usize,
    counts: &[usize],
) where
    T: RadixKey + Sized + Send + Ord + Copy + Sync,
{
    if bucket.len() < 32 {
        bucket.sort_unstable();
        return;
    }

    let mut local_counts = Vec::with_capacity(256);
    let mut prefix_sums = Vec::with_capacity(256);
    let mut running_total = 0;

    for i in 0..256 {
        let count = counts[calculate_position(msb, level - 1, i)];
        local_counts.push(count);
        prefix_sums.push(running_total);
        running_total += count;
    }

    bucket.iter().for_each(|val| {
        let bucket = val.get_level(level) as usize;
        unsafe {
            let write_loc = prefix_sums.get_unchecked_mut(bucket);
            *tmp_bucket.get_unchecked_mut(*write_loc) = *val;
            *write_loc += 1;
        }
    });

    drop(prefix_sums);
    bucket.copy_from_slice(tmp_bucket);

    if level == 1 {
        return;
    } else {
        lsb_radix_sort_bucket(bucket, tmp_bucket, level - 1, msb, counts);
    }
}

fn radix_sort_inner<T>(bucket: &mut [T])
where
    T: RadixKey + Sized + Send + Ord + Copy + Sync,
{
    if T::LEVELS == 0 {
        panic!("RadixKey must have at least 1 level");
    }

    radix_sort_bucket_start(bucket);
}

pub trait RadixSort {
    /// radix_sort_unstable runs the actual radix sort based upon the `rdst::RadixKey` implementation
    /// of `T` in your `Vec<T>` or `[T]`.
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
