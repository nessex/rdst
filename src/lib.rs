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
//#![feature(test)]

#[cfg(all(test, feature = "bench"))]
extern crate test;

#[cfg(test)]
mod tests;

mod arbitrary_chunks;
#[cfg(all(test, feature = "bench"))]
mod benches;
mod radix_key;

use crate::arbitrary_chunks::*;
pub use radix_key::RadixKey;
use rayon::prelude::*;

fn get_counts<T>(data: &[T], level: usize) -> Vec<usize>
where
    T: RadixKey + Sync,
{
    if data.len() > 8192 {
        let chunk_size = (data.len() / num_cpus::get()) + 1;
        data.par_chunks(chunk_size)
            .fold(
                || vec![0; 256],
                |mut store, items| {
                    items.iter().for_each(|d| {
                        let val = d.get_level(level) as usize;
                        store[val] += 1;
                    });

                    store
                },
            )
            .reduce(
                || vec![0; 256],
                |mut store, d| {
                    for (i, c) in d.iter().enumerate() {
                        store[i] += c;
                    }

                    store
                },
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
        let mut new_bucket = Vec::with_capacity(bucket.len());
        unsafe {
            // This will leave the vec with garbage data
            // however as we account for every value when placing things
            // into new_bucket, this is "safe". This is used because it provides a
            // very significant speed improvement over resize, to_vec etc.
            new_bucket.set_len(bucket.len());
        }
        let counts = get_counts(bucket, level);
        let mut count_offsets = get_prefix_sums(&counts);

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

fn radix_sort_inner<T>(bucket: &mut [T])
where
    T: RadixKey + Sized + Send + Ord + Copy + Sync,
{
    if T::LEVELS == 0 {
        panic!("RadixKey must have at least 1 level");
    }

    radix_sort_bucket(bucket, 0, T::LEVELS);
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
