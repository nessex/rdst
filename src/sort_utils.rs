use crate::radix_array::RadixArray;
use crate::sort_value::SortValue;
#[cfg(feature = "multi-threaded")]
use rayon::prelude::*;
use std::mem::MaybeUninit;
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
    let mut end_offsets = RadixArray::from_fn(|i| prefix_sums.get(i.saturating_add(1)));
    *end_offsets.get_mut(255) += counts.get(255);

    end_offsets
}

#[inline]
#[cfg(feature = "multi-threaded")]
pub fn par_get_counts_with_ends<T>(bucket: &[T], level: usize) -> (RadixArray<usize>, bool, u8, u8)
where
    T: SortValue,
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
        boundaries_heap = (0..len).map(|_| (0u8, 0u8)).collect();
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
pub fn get_counts_with_ends<T: SortValue>(
    bucket: &[T],
    level: usize,
) -> (RadixArray<usize>, bool, u8, u8) {
    #[cfg(feature = "work_profiles")]
    println!("({}) COUNT", level);

    if bucket.is_empty() {
        return (RadixArray::new(0), true, 0, 0);
    }

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
    T: SortValue,
{
    let (counts, sorted, _, _) = get_counts_with_ends(bucket, level);

    (counts, sorted)
}

#[inline]
pub fn get_tile_counts<T>(
    bucket: &[T],
    tile_size: usize,
    level: usize,
) -> (Vec<RadixArray<usize>>, bool)
where
    T: SortValue,
{
    #[cfg(feature = "work_profiles")]
    println!("({}) TILE_COUNT", level);

    let tile_counts: Vec<RadixArray<usize>>;
    let tile_meta: Vec<(bool, u8, u8)>;

    #[cfg(feature = "multi-threaded")]
    {
        (tile_counts, tile_meta) = bucket
            .par_chunks(tile_size)
            .map(|chunk| {
                let (c, s, start, end) = par_get_counts_with_ends(chunk, level);
                (c, (s, start, end))
            })
            .unzip();
    }

    #[cfg(not(feature = "multi-threaded"))]
    {
        (tile_counts, tile_meta) = bucket
            .chunks(tile_size)
            .map(|chunk| {
                let (c, s, start, end) = get_counts_with_ends(chunk, level);
                (c, (s, start, end))
            })
            .unzip();
    }

    if tile_meta.len() == 1 {
        // Tiles of length 1 are considered sorted
        return (tile_counts, tile_meta[0].0);
    }

    for m in tile_meta.windows(2) {
        if !m[0].0 || !m[1].0 || m[1].1 < m[0].2 {
            // Tiles are individually sorted _and_
            // the boundaries of tiles line up making
            // the combined set of tiles also sorted.
            return (tile_counts, false);
        }
    }

    (tile_counts, true)
}

#[inline]
pub fn aggregate_tile_counts(tile_counts: &[RadixArray<usize>]) -> RadixArray<usize> {
    RadixArray::from_fn(|i| tile_counts.iter().map(|t| t.get(i)).sum())
}

#[inline(always)]
pub const fn bucket_as_uninit<T>(src: &[T]) -> &[MaybeUninit<T>]
where
    T: SortValue,
{
    unsafe {
        // SAFETY: We are converting from
        // &[T] to &[MaybeUninit<T>]
        // [T] and [MaybeUninit<T>] have the same
        // layout and size.
        std::mem::transmute::<&[T], &[MaybeUninit<T>]>(src)
    }
}

#[inline(always)]
pub const fn bucket_as_uninit_mut<T>(src: &mut [T]) -> &mut [MaybeUninit<T>]
where
    T: SortValue,
{
    unsafe {
        // SAFETY: We are converting from
        // &mut [T] to &mut [MaybeUninit<T>]
        // [T] and [MaybeUninit<T>] have the same
        // layout and size.
        std::mem::transmute::<&mut [T], &mut [MaybeUninit<T>]>(src)
    }
}

/// assume_init_ref matches the std method on &[MaybeUninit<T>]
/// This was stabilized in 1.93.0, after our MSRV.
/// This alternative can be removed when that sees broader adoption.
///
/// This has the same safety requirement: it's up to the caller to ensure
/// all values in src are already fully initialized.
#[inline(always)]
pub const unsafe fn assume_init_ref<T>(src: &[MaybeUninit<T>]) -> &[T] {
    // SAFETY: casting `slice` to a `*const [T]` is safe since the caller guarantees that
    // `slice` is initialized, and `MaybeUninit` is guaranteed to have the same layout as `T`.
    // The pointer obtained is valid since it refers to memory owned by `slice` which is a
    // reference and thus guaranteed to be valid for reads.
    unsafe { &*(src as *const [MaybeUninit<T>] as *const [T]) }
}

#[inline]
pub fn partition_index<T, P>(data: &mut [T], predicate: P) -> usize
where
    P: Fn(&T) -> bool,
{
    // iter_mut() acts as a double-ended pointer window.
    // Originally this was a start & end cursor, but this
    // iterator means the compiler can reason about bounds
    // and elide bounds checks at runtime without unsafe.
    let mut iter = data.iter_mut();
    let mut left_count = 0;

    loop {
        // 1. Advance from the left until we find an element that FAILS the predicate
        let left_item = loop {
            match iter.next() {
                Some(item) if predicate(item) => left_count += 1,
                Some(item) => break item,
                None => return left_count, // Pointers crossed, we are done
            }
        };

        // 2. Retreat from the right until we find an element that PASSES the predicate
        let right_item = loop {
            match iter.next_back() {
                Some(item) if !predicate(item) => continue,
                Some(item) => break item,
                None => return left_count, // Pointers crossed, we are done
            }
        };

        // 3. Swap the values, left and right are both out of place
        std::mem::swap(left_item, right_item);

        // After swapping, the left successfully passes
        left_count += 1;
    }
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
