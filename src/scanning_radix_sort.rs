use crate::lsb_radix_sort::lsb_radix_sort_adapter;
use crate::msb_ska_sort::msb_ska_sort;
use crate::tuning_parameters::TuningParameters;
use crate::utils::*;
use crate::RadixKey;
use arbitrary_chunks::ArbitraryChunks;
use rayon::prelude::*;
use std::cmp::min;
use std::ptr::copy_nonoverlapping;
use std::sync::Mutex;

struct ScannerBucketInner<'a, T> {
    write_head: usize,
    read_head: usize,
    chunk: &'a mut [T],
}

struct ScannerBucket<'a, T> {
    index: usize,
    len: isize,
    inner: Mutex<ScannerBucketInner<'a, T>>,
}

#[inline]
fn get_scanner_buckets<'a, T>(
    counts: &[usize; 256],
    bucket: &'a mut [T],
) -> Vec<ScannerBucket<'a, T>> {
    let mut out: Vec<_> = bucket
        .arbitrary_chunks_mut(counts.to_vec())
        .enumerate()
        .map(|(index, chunk)| ScannerBucket {
            index,
            len: chunk.len() as isize,
            inner: Mutex::new(ScannerBucketInner {
                write_head: 0,
                read_head: 0,
                chunk,
            }),
        })
        .collect();

    out.sort_by_key(|b| b.len);
    out.reverse();

    out
}

fn scanner_thread<T>(
    scanner_buckets: &Vec<ScannerBucket<T>>,
    level: usize,
    scanner_read_size: isize,
) where
    T: RadixKey + Copy,
{
    let mut stash: Vec<Vec<T>> = Vec::with_capacity(256);
    stash.resize(256, Vec::with_capacity(128));
    let mut finished_count = 0;
    let mut finished_map = [false; 256];

    'outer: loop {
        for m in scanner_buckets {
            unsafe {
                if *finished_map.get_unchecked(m.index) {
                    continue;
                }
            }

            let mut guard = match m.inner.try_lock() {
                Ok(g) => g,
                Err(_) => continue,
            };

            if guard.write_head >= m.len as usize {
                finished_count += 1;
                finished_map[m.index] = true;

                if finished_count == scanner_buckets.len() {
                    break 'outer;
                }

                continue;
            }

            let read_start = guard.read_head as isize;
            let to_read = min(m.len - read_start, scanner_read_size);

            if to_read > 0 {
                let to_read = to_read as usize;
                let end = guard.read_head + to_read;
                let read_data = &guard.chunk[guard.read_head..end];
                let chunks = read_data.chunks_exact(8);
                let rem = chunks.remainder();

                chunks.into_iter().for_each(|chunk| unsafe {
                    let a = chunk.get_unchecked(0).get_level(level) as usize;
                    let b = chunk.get_unchecked(1).get_level(level) as usize;
                    let c = chunk.get_unchecked(2).get_level(level) as usize;
                    let d = chunk.get_unchecked(3).get_level(level) as usize;
                    let e = chunk.get_unchecked(4).get_level(level) as usize;
                    let f = chunk.get_unchecked(5).get_level(level) as usize;
                    let g = chunk.get_unchecked(6).get_level(level) as usize;
                    let h = chunk.get_unchecked(7).get_level(level) as usize;

                    stash.get_unchecked_mut(a).push(*chunk.get_unchecked(0));
                    stash.get_unchecked_mut(b).push(*chunk.get_unchecked(1));
                    stash.get_unchecked_mut(c).push(*chunk.get_unchecked(2));
                    stash.get_unchecked_mut(d).push(*chunk.get_unchecked(3));
                    stash.get_unchecked_mut(e).push(*chunk.get_unchecked(4));
                    stash.get_unchecked_mut(f).push(*chunk.get_unchecked(5));
                    stash.get_unchecked_mut(g).push(*chunk.get_unchecked(6));
                    stash.get_unchecked_mut(h).push(*chunk.get_unchecked(7));
                });

                rem.into_iter().for_each(|v| {
                    let a = v.get_level(level) as usize;
                    stash[a].push(*v);
                });

                guard.read_head += to_read;
            }

            let to_write = min(
                stash[m.index].len() as isize,
                guard.read_head as isize - guard.write_head as isize,
            );

            if to_write < 1 {
                continue;
            }

            let to_write = to_write as usize;
            let split = stash[m.index].len() - to_write;
            let some = stash[m.index].split_off(split);
            let end = guard.write_head + to_write;
            let start = guard.write_head;

            unsafe {
                copy_nonoverlapping(
                    some.get_unchecked(0),
                    guard.chunk.get_unchecked_mut(start),
                    end - start,
                );
            }

            guard.write_head += to_write;

            if guard.write_head >= m.len as usize {
                finished_count += 1;
                finished_map[m.index] = true;

                if finished_count == scanner_buckets.len() {
                    break 'outer;
                }
            }
        }
    }
}

// scanning_radix_sort does a parallel MSB-first sort. Following this, depending on the number of
// elements remaining in each bucket, it will either do an MSB-sort or an LSB-sort, making this
// a dynamic hybrid sort.
pub fn scanning_radix_sort<T>(tuning: &TuningParameters, bucket: &mut [T], start_level: usize, parallel_count: bool)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let (msb_counts, level) = if let Some(s) = get_counts_and_level(bucket, start_level, T::LEVELS - 1, parallel_count) {
        s
    } else {
        return;
    };

    let scanner_buckets = get_scanner_buckets(&msb_counts, bucket);
    let cpus = num_cpus::get();
    let threads = min(cpus, scanner_buckets.len());

    rayon::scope(|s| {
        for _ in 0..threads {
            s.spawn(|_| scanner_thread(&scanner_buckets, level, tuning.scanner_read_size as isize));
        }
    });

    // Drop some data before recursing to reduce memory usage
    drop(scanner_buckets);

    if level == T::LEVELS - 1 {
        return;
    }

    bucket
        .arbitrary_chunks_mut(msb_counts.to_vec())
        .par_bridge()
        .for_each(|c| {
            if c.len() > tuning.ska_sort_threshold {
                msb_ska_sort(tuning, c, level + 1);
            } else {
                lsb_radix_sort_adapter(c, T::LEVELS - 1, level + 1, false);
            }
        });
}
