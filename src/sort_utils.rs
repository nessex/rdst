use crate::radix_array::RadixArray;
use crate::radix_key::RadixKeyChecked;
#[cfg(feature = "multi-threaded")]
use rayon::prelude::*;
#[cfg(feature = "multi-threaded")]
use std::sync::mpsc::channel;

#[inline]
pub fn get_prefix_sums(counts: &RadixArray<usize>) -> RadixArray<usize> {
    let mut sums = RadixArray::new(0);

    let mut running_total = 0;
    for (i, c) in counts.iter().enumerate() {
        *sums.get_mut(i) = running_total;
        running_total += c;
    }

    sums
}

#[inline]
pub fn get_end_offsets(
    counts: &RadixArray<usize>,
    prefix_sums: &RadixArray<usize>,
) -> RadixArray<usize> {
    let mut end_offsets = RadixArray::new(0);

    end_offsets.inner_mut()[0..=254].copy_from_slice(&prefix_sums.inner()[1..=255]);
    *end_offsets.get_mut(255) = counts.get(255) + prefix_sums.get(255);

    end_offsets
}

#[inline]
#[cfg(feature = "multi-threaded")]
pub fn par_get_counts_with_ends<T>(bucket: &[T], level: usize) -> (RadixArray<usize>, bool, u8, u8)
where
    T: RadixKeyChecked + Sized + Send + Sync,
{
    #[cfg(feature = "work_profiles")]
    println!("({}) PAR_COUNT", level);

    if bucket.len() < 400_000 {
        return get_counts_with_ends(bucket, level);
    }

    let threads = rayon::current_num_threads();
    let chunk_divisor = 8;
    let chunk_size = (bucket.len() / threads / chunk_divisor) + 1;
    let chunks = bucket.par_chunks(chunk_size);
    let len = chunks.len();
    let (tx, rx) = channel();

    chunks.enumerate().for_each_with(tx, |tx, (i, chunk)| {
        let counts = get_counts_with_ends(chunk, level);
        tx.send((i, counts.0, counts.1, counts.2, counts.3))
            .unwrap();
    });

    let mut msb_counts: RadixArray<usize> = RadixArray::new(0);
    let mut already_sorted = true;

    const BOUNDARIES_STACK_LEN: usize = 128;
    let mut boundaries_heap: Box<[(u8, u8)]>;
    let mut boundaries_stack: [(u8, u8); BOUNDARIES_STACK_LEN];

    let boundaries = if len <= BOUNDARIES_STACK_LEN {
        boundaries_stack = [(0u8, 0u8); BOUNDARIES_STACK_LEN];
        boundaries_stack.as_mut_slice()
    } else {
        boundaries_heap = unsafe {
            // SAFETY: [(0u8, 0u8)] and a zeroed slice are the same
            // at the bit level.
            Box::new_zeroed_slice(len).assume_init()
        };
        &mut boundaries_heap
    };

    for _ in 0..len {
        let (i, counts, chunk_sorted, start, end) = rx.recv().unwrap();

        if !chunk_sorted {
            already_sorted = false;
        }

        boundaries[i].0 = start;
        boundaries[i].1 = end;

        for (i, c) in counts.iter().enumerate() {
            *msb_counts.get_mut(i) += c;
        }
    }

    // Check the boundaries of each counted chunk, to see if the full bucket
    // is already sorted
    if already_sorted {
        for w in boundaries.windows(2) {
            if w[1].0 < w[0].1 {
                already_sorted = false;
                break;
            }
        }
    }

    (
        msb_counts,
        already_sorted,
        boundaries[0].0,
        boundaries[boundaries.len() - 1].1,
    )
}

#[inline]
pub fn get_counts_with_ends<T>(bucket: &[T], level: usize) -> (RadixArray<usize>, bool, u8, u8)
where
    T: RadixKeyChecked,
{
    #[cfg(feature = "work_profiles")]
    println!("({}) COUNT", level);

    let mut already_sorted = true;
    let mut continue_from = bucket.len();
    let mut counts_1 = RadixArray::new(0);
    let mut last = 0u8;

    for (i, item) in bucket.iter().enumerate() {
        let b = item.get_level_checked(level);
        *counts_1.get_mut(b) += 1;

        if b < last {
            continue_from = i + 1;
            already_sorted = false;
            break;
        }

        last = b;
    }

    if continue_from == bucket.len() {
        return (
            counts_1,
            already_sorted,
            bucket[0].get_level_checked(level),
            last,
        );
    }

    let mut counts_2 = RadixArray::new(0);
    let mut counts_3 = RadixArray::new(0);
    let mut counts_4 = RadixArray::new(0);
    let chunks = bucket[continue_from..].chunks_exact(4);
    let rem = chunks.remainder();

    chunks.into_iter().for_each(|chunk| {
        let a = chunk[0].get_level_checked(level);
        let b = chunk[1].get_level_checked(level);
        let c = chunk[2].get_level_checked(level);
        let d = chunk[3].get_level_checked(level);

        *counts_1.get_mut(a) += 1;
        *counts_2.get_mut(b) += 1;
        *counts_3.get_mut(c) += 1;
        *counts_4.get_mut(d) += 1;
    });

    rem.iter().for_each(|v| {
        let b = v.get_level_checked(level);
        *counts_1.get_mut(b) += 1;
    });

    for i in 0..=255 {
        *counts_1.get_mut(i) += counts_2.get(i);
        *counts_1.get_mut(i) += counts_3.get(i);
        *counts_1.get_mut(i) += counts_4.get(i);
    }

    let b_first = bucket.first().unwrap().get_level_checked(level);
    let b_last = bucket.last().unwrap().get_level_checked(level);

    (counts_1, already_sorted, b_first, b_last)
}

#[inline]
pub fn get_counts<T>(bucket: &[T], level: usize) -> (RadixArray<usize>, bool)
where
    T: RadixKeyChecked,
{
    if bucket.is_empty() {
        return (RadixArray::new(0), true);
    }

    let (counts, sorted, _, _) = get_counts_with_ends(bucket, level);

    (counts, sorted)
}

#[inline]
pub const fn cdiv(a: usize, b: usize) -> usize {
    (a + b - 1) / b
}

#[inline]
pub fn get_tile_counts<T>(
    bucket: &[T],
    tile_size: usize,
    level: usize,
) -> (Vec<RadixArray<usize>>, bool)
where
    T: RadixKeyChecked + Copy + Sized + Send + Sync,
{
    #[cfg(feature = "work_profiles")]
    println!("({}) TILE_COUNT", level);

    #[cfg(feature = "multi-threaded")]
    let (tile_counts, tile_meta): (Vec<RadixArray<usize>>, Vec<(bool, u8, u8)>) = bucket
        .par_chunks(tile_size)
        .map(|chunk| {
            let (c, s, start, end) = par_get_counts_with_ends(chunk, level);
            (c, (s, start, end))
        })
        .unzip();

    #[cfg(not(feature = "multi-threaded"))]
    let (tile_counts, tile_meta): (Vec<RadixArray<usize>>, Vec<(bool, u8, u8)>) = bucket
        .chunks(tile_size)
        .map(|chunk| {
            let (c, s, start, end) = get_counts_with_ends(chunk, level);
            (c, (s, start, end))
        })
        .unzip();
    let mut all_sorted = true;

    if tile_meta.len() == 1 {
        all_sorted = tile_meta[0].0;
    } else {
        for m in tile_meta.windows(2) {
            if !m[0].0 || !m[1].0 || m[1].1 < m[0].2 {
                all_sorted = false;
                break;
            }
        }
    }

    (tile_counts, all_sorted)
}

#[inline]
pub fn aggregate_tile_counts(tile_counts: &[RadixArray<usize>]) -> RadixArray<usize> {
    let mut out = tile_counts[0].clone();
    for tile in tile_counts.iter().skip(1) {
        for i in 0..=255 {
            *out.get_mut(i) += tile.get(i);
        }
    }

    out
}

#[inline]
pub fn is_homogenous_bucket(counts: &RadixArray<usize>) -> bool {
    let mut seen = false;
    for c in counts.iter() {
        if c > 0 {
            if seen {
                return false;
            } else {
                seen = true;
            }
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use crate::sort_utils::get_tile_counts;

    #[test]
    pub fn test_get_tile_counts_correctly_marks_already_sorted_single_tile() {
        let mut data: Vec<u8> = vec![0, 5, 2, 3, 1];

        let (_counts, already_sorted) = get_tile_counts(&mut data, 5, 0);
        assert_eq!(already_sorted, false);

        let mut data: Vec<u8> = vec![0, 0, 1, 1, 2];

        let (_counts, already_sorted) = get_tile_counts(&mut data, 5, 0);
        assert_eq!(already_sorted, true);
    }

    #[test]
    pub fn test_get_tile_counts_correctly_marks_already_sorted_multiple_tiles() {
        let mut data: Vec<u8> = vec![0, 5, 2, 3, 1];

        let (_counts, already_sorted) = get_tile_counts(&mut data, 2, 0);
        assert_eq!(already_sorted, false);

        let mut data: Vec<u8> = vec![0, 0, 1, 1, 2];

        let (_counts, already_sorted) = get_tile_counts(&mut data, 2, 0);
        assert_eq!(already_sorted, true);
    }
}
