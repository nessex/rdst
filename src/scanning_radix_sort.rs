use crate::lsb_radix_sort::lsb_radix_sort_adapter;
use crate::msb_ska_sort::msb_ska_sort;
use crate::tuning_parameters::TuningParameters;
use crate::utils::*;
use crate::RadixKey;
use arbitrary_chunks::ArbitraryChunks;
use rayon::prelude::*;
use std::cmp::min;
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
    counts: &Vec<usize>,
    bucket: &'a mut [T],
) -> Vec<ScannerBucket<'a, T>> {
    let mut out: Vec<_> = bucket
        .arbitrary_chunks_mut(counts.clone())
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
    let mut finished_map: Vec<bool> = vec![false; 256];

    'outer: loop {
        for m in scanner_buckets {
            if finished_map[m.index] {
                continue;
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

                chunks.into_iter().for_each(|chunk| {
                    let a = chunk[0].get_level(level) as usize;
                    let b = chunk[1].get_level(level) as usize;
                    let c = chunk[2].get_level(level) as usize;
                    let d = chunk[3].get_level(level) as usize;
                    let e = chunk[4].get_level(level) as usize;
                    let f = chunk[5].get_level(level) as usize;
                    let g = chunk[6].get_level(level) as usize;
                    let h = chunk[7].get_level(level) as usize;

                    stash[a].push(chunk[0]);
                    stash[b].push(chunk[1]);
                    stash[c].push(chunk[2]);
                    stash[d].push(chunk[3]);
                    stash[e].push(chunk[4]);
                    stash[f].push(chunk[5]);
                    stash[g].push(chunk[6]);
                    stash[h].push(chunk[7]);
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

            guard.chunk[start..end].copy_from_slice(&some);
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
pub fn scanning_radix_sort<T>(tuning: &TuningParameters, bucket: &mut [T], level: usize)
where
    T: RadixKey + Sized + Send + Ord + Copy + Sync,
{
    let msb_counts = if level == 0 && bucket.len() > tuning.par_count_threshold {
        par_get_counts(bucket, level)
    } else {
        get_counts(bucket, level)
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
        .arbitrary_chunks_mut(msb_counts)
        .par_bridge()
        .for_each(|c| {
            if c.len() > tuning.ska_sort_threshold {
                msb_ska_sort(tuning, c, level + 1);
            } else {
                lsb_radix_sort_adapter(tuning, c, T::LEVELS - 1, level + 1);
            }
        });
}
