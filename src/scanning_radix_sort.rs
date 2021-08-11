use crate::lsb_radix_sort::lsb_radix_sort_bucket;
use crate::msb_ska_sort::msb_ska_sort;
use crate::RadixKey;
use arbitrary_chunks::ArbitraryChunks;
use nanorand::{Rng, WyRand};
use rayon::prelude::*;
use std::cmp::min;
use std::sync::Mutex;

struct ScannerBucket<'a, T> {
    write_head: usize,
    read_head: usize,
    len: isize,
    chunk: &'a mut [T],
}

#[inline]
fn get_scanner_buckets<'a, T>(
    counts: &Vec<usize>,
    bucket: &'a mut [T],
) -> Vec<Mutex<ScannerBucket<'a, T>>> {
    let mut out: Vec<_> = bucket
        .arbitrary_chunks_mut(counts.clone())
        .map(|chunk| {
            Mutex::new(ScannerBucket {
                write_head: 0,
                read_head: 0,
                len: chunk.len() as isize,
                chunk,
            })
        })
        .collect();

    out.resize_with(256, || {
        Mutex::new(ScannerBucket {
            write_head: 0,
            read_head: 0,
            len: 0,
            chunk: &mut [],
        })
    });

    out
}

fn scanner_thread<T>(
    scanner_buckets: &Vec<Mutex<ScannerBucket<T>>>,
    level: usize,
    scanner_read_size: isize,
) where
    T: RadixKey + Copy,
{
    let mut rng = WyRand::new();
    let pivot = rng.generate::<u8>() as usize;
    let (before, after) = scanner_buckets.split_at(pivot);

    let mut stash: Vec<Vec<T>> = Vec::with_capacity(256);
    stash.resize(256, Vec::with_capacity(128));
    let mut finished_count = 0;
    let mut finished_map: Vec<bool> = vec![false; 256];

    'outer: loop {
        for (i, m) in after
            .iter()
            .enumerate()
            .map(|(i, v)| (i + pivot, v))
            .chain(before.iter().enumerate())
        {
            if finished_map[i] {
                continue;
            }

            let mut guard = m.lock().unwrap();

            if guard.write_head >= guard.len as usize {
                finished_count += 1;
                finished_map[i] = true;

                if finished_count == 256 {
                    break 'outer;
                }

                continue;
            }

            let read_start = guard.read_head as isize;
            let to_read = min(guard.len - read_start, scanner_read_size);

            if to_read > 0 {
                let to_read = to_read as usize;
                let end = guard.read_head + to_read;
                let read_data = &guard.chunk[guard.read_head..end];

                read_data.iter().for_each(|v| {
                    let a = v.get_level(level) as usize;
                    stash[a].push(*v);
                });

                guard.read_head += to_read;
            }

            let to_write = min(
                stash[i].len() as isize,
                guard.read_head as isize - guard.write_head as isize,
            );

            if to_write < 1 {
                continue;
            }

            let to_write = to_write as usize;
            let split = stash[i].len() - to_write;
            let some = stash[i].split_off(split);
            let end = guard.write_head + to_write;
            let start = guard.write_head;

            guard.chunk[start..end].copy_from_slice(&some);
            guard.write_head += to_write;

            if guard.write_head >= guard.len as usize {
                finished_count += 1;
                finished_map[i] = true;

                if finished_count == 256 {
                    break 'outer;
                }
            }
        }
    }
}

// scanning_radix_sort_bucket does a parallel sort by the MSB. Following the MSB sort, it runs
// a simple LSB-first sort for each of the generated MSB buckets, making this a hybrid sort.
pub fn scanning_radix_sort_bucket<T>(bucket: &mut [T], msb_counts: Vec<usize>)
where
    T: RadixKey + Sized + Send + Ord + Copy + Sync,
{
    let level = 0;
    let scanner_buckets = get_scanner_buckets(&msb_counts, bucket);
    let cpus = num_cpus::get();
    let scaling_factor = min(1, (cpus as f32).log2().ceil() as isize);
    let scanner_read_size = (65536 / scaling_factor) as isize;

    rayon::scope(|s| {
        for _ in 0..cpus {
            s.spawn(|_| scanner_thread(&scanner_buckets, level, scanner_read_size));
        }
    });

    // Drop some data before recursing to reduce memory / thread usage
    drop(scanner_buckets);

    if level == T::LEVELS - 1 {
        return;
    }

    bucket
        .arbitrary_chunks_mut(msb_counts)
        .par_bridge()
        .for_each(|c| {
            if c.len() > 500_000 {
                msb_ska_sort(c, level + 1);
            } else {
                lsb_radix_sort_bucket(c, T::LEVELS - 1, 1);
            }
        });
}
