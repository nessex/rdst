use crate::director::director;
use crate::tuning_parameters::TuningParameters;
use crate::utils::*;
use crate::RadixKey;
use arbitrary_chunks::ArbitraryChunks;
use partition::partition_index;
use std::cmp::min;
use std::ptr::copy_nonoverlapping;
use try_mutex::TryMutex;

struct ScannerBucketInner<'a, T> {
    write_head: usize,
    read_head: usize,
    chunk: &'a mut [T],
    locally_partitioned: bool,
    sorted: bool,
}

struct ScannerBucket<'a, T> {
    index: usize,
    len: isize,
    inner: TryMutex<ScannerBucketInner<'a, T>>,
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
            inner: TryMutex::new(ScannerBucketInner {
                write_head: 0,
                read_head: 0,
                chunk,
                locally_partitioned: false,
                sorted: false,
            }),
        })
        .collect();

    out.sort_by_key(|b| b.len);
    // out.reverse();

    out
}

fn scanner_thread<T>(
    thread_num: usize,
    tuning: &TuningParameters,
    scanner_buckets: &[ScannerBucket<T>],
    level: usize,
    scanner_read_size: isize,
) where
    T: RadixKey + Copy + Send + Sync,
{
    let mut stash: Vec<Vec<T>> = Vec::with_capacity(256);
    stash.resize(256, Vec::with_capacity(128));
    let mut stash_total = 0;
    let mut finished_count = 0;
    let mut finished_map = [false; 256];
    let cutoff = if thread_num == 0 {
        scanner_buckets.len()
    } else {
        scanner_buckets.len() - thread_num - 1
    };

    for m in scanner_buckets {
        let mut guard = match m.inner.try_lock() {
            Some(g) => g,
            None => continue,
        };

        if !guard.locally_partitioned {
            guard.locally_partitioned = true;
            let index = m.index as u8;

            let start = partition_index(&mut guard.chunk, |v| v.get_level(level) == index);

            guard.read_head = start;
            guard.write_head = start;
        }
    }

    'outer: loop {
        for m in scanner_buckets {
            unsafe {
                if *finished_map.get_unchecked(m.index) {
                    continue;
                }
            }

            let mut guard = match m.inner.try_lock() {
                Some(g) => g,
                None => continue,
            };

            if guard.write_head >= m.len as usize {
                finished_count += 1;
                finished_map[m.index] = true;

                if finished_count >= cutoff && stash_total == 0 {
                    break 'outer;
                }

                continue;
            }

            if !guard.locally_partitioned {
                guard.locally_partitioned = true;
                let index = m.index as u8;

                let start = partition_index(&mut guard.chunk, |v| v.get_level(level) == index);

                guard.read_head = start;
                guard.write_head = start;
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
                stash_total += to_read;
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
            stash_total -= to_write;

            if guard.write_head >= m.len as usize {
                finished_count += 1;
                finished_map[m.index] = true;

                if finished_count >= cutoff && stash_total == 0 {
                    break 'outer;
                }
            }
        }
    }

    if level == 0 {
        return;
    }

    for m in scanner_buckets {
        let mut guard = match m.inner.try_lock() {
            Some(g) => g,
            None => continue,
        };

        if !guard.sorted {
            guard.sorted = true;

            director(tuning, guard.chunk, level - 1, false);
        }
    }
}

// scanning_radix_sort does a parallel MSB-first sort. Following this, depending on the number of
// elements remaining in each bucket, it will either do an MSB-sort or an LSB-sort, making this
// a dynamic hybrid sort.
pub fn scanning_radix_sort<T>(
    tuning: &TuningParameters,
    bucket: &mut [T],
    start_level: usize,
    parallel_count: bool,
) where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let (msb_counts, level) =
        if let Some(s) = get_counts_and_level_descending(bucket, start_level, 0, parallel_count) {
            s
        } else {
            return;
        };

    let scanner_buckets = get_scanner_buckets(&msb_counts, bucket);
    let cpus = num_cpus::get();
    let threads = min(cpus, scanner_buckets.len());

    rayon::scope(|s| {
        for i in 0..threads {
            let buckets = scanner_buckets.as_slice();
            s.spawn(move |_| {
                scanner_thread(i, tuning, buckets, level, tuning.scanner_read_size as isize)
            });
        }
    });
}
