//! `out_of_place_sort` is an out-of-place single-threaded radix sort. This is the classic academic
//! implementation of counting sort. There are 4 different variants implemented here with varying
//! optimizations.
//!
//! This is used as a building block for other complete sorting algorithms.
//!
//! ### Standard out_of_place_sort
//!
//! This implementation is a very simple out-of-place counting sort. The only notable optimization
//! is to process data in chunks to take some advantage of multiple execution ports in each CPU core.
//!
//! ### out_of_place_sort_with_counts
//!
//! As the name suggests, this variant is the same as the standard out_of_place_sort except that
//! as it sorts into the output array, it also checks the next level and adds it to a counts array.
//!
//! This shortcut shaves off a tiny bit of time that would be spent counting the next level before
//! sorting. It doesn't make a huge difference as you impair caching and similar that would
//! otherwise perform better in both the sort and the next counting pass. That said, it is
//! significant enough to still include as an option.
//!
//! ### lr_out_of_place_sort
//!
//! This variant of the standard out_of_place_sort uses two sets of cursors, one left and one right
//! cursor for writing data. This is able to remain stable as it inspects the input array starting
//! from the right for all values placed to the right side of each output bucket. Thus maintaining
//! the stable ordering of values.
//!
//! This provides a significant performance benefit when there are many identical values as
//! typically a pair of identical values would prevent the CPU from using multiple execution ports.
//! With this variant however, the CPU can safely and independently work on two identical values at the
//! same time as there is no overlapping variable access in either the output array or the prefix
//! sums array.
//!
//! ### lr_out_of_place_sort_with_counts
//!
//! As with the other with_counts variant, this combines the left-right optimization with counting
//! the next level.
//!
//! ## Characteristics
//!
//!  * out-of-place
//!  * single-threaded
//!  * lsb-first

use crate::radix_array::RadixArray;
use crate::sort_utils::{bucket_as_uninit, get_prefix_sums};
use crate::sort_value::SortValue;
use std::mem::MaybeUninit;

#[inline]
pub fn out_of_place_sort<T>(
    src_bucket: &[T],
    // XXX: After calling this function, all
    // values in dst_bucket _must_ be considered
    // initialized. It's up to this function
    // to maintain that invariant for all callers
    // that expect this behaviour.
    dst_bucket: &mut [MaybeUninit<T>],
    counts: &RadixArray<usize>,
    level: usize,
) where
    T: SortValue,
{
    if src_bucket.len() < 2 {
        dst_bucket.copy_from_slice(bucket_as_uninit(src_bucket));
        return;
    }

    let mut prefix_sums = get_prefix_sums(counts);

    let chunks = src_bucket.chunks_exact(8);
    let rem = chunks.remainder();

    chunks.into_iter().for_each(|chunk| {
        let a = chunk[0].get_level_checked(level);
        let b = chunk[1].get_level_checked(level);
        let c = chunk[2].get_level_checked(level);
        let d = chunk[3].get_level_checked(level);
        let e = chunk[4].get_level_checked(level);
        let f = chunk[5].get_level_checked(level);
        let g = chunk[6].get_level_checked(level);
        let h = chunk[7].get_level_checked(level);

        dst_bucket[prefix_sums.get(a)] = MaybeUninit::new(chunk[0]);
        *prefix_sums.get_mut(a) += 1;
        dst_bucket[prefix_sums.get(b)] = MaybeUninit::new(chunk[1]);
        *prefix_sums.get_mut(b) += 1;
        dst_bucket[prefix_sums.get(c)] = MaybeUninit::new(chunk[2]);
        *prefix_sums.get_mut(c) += 1;
        dst_bucket[prefix_sums.get(d)] = MaybeUninit::new(chunk[3]);
        *prefix_sums.get_mut(d) += 1;
        dst_bucket[prefix_sums.get(e)] = MaybeUninit::new(chunk[4]);
        *prefix_sums.get_mut(e) += 1;
        dst_bucket[prefix_sums.get(f)] = MaybeUninit::new(chunk[5]);
        *prefix_sums.get_mut(f) += 1;
        dst_bucket[prefix_sums.get(g)] = MaybeUninit::new(chunk[6]);
        *prefix_sums.get_mut(g) += 1;
        dst_bucket[prefix_sums.get(h)] = MaybeUninit::new(chunk[7]);
        *prefix_sums.get_mut(h) += 1;
    });

    rem.iter().for_each(|val| {
        let b = val.get_level_checked(level);
        dst_bucket[prefix_sums.get(b)] = MaybeUninit::new(*val);
        *prefix_sums.get_mut(b) += 1;
    });
}

#[inline]
pub fn out_of_place_sort_with_counts<T>(
    src_bucket: &[T],
    // XXX: After calling this function, all
    // values in dst_bucket _must_ be considered
    // initialized. It's up to this function
    // to maintain that invariant for all callers
    // that expect this behaviour.
    dst_bucket: &mut [MaybeUninit<T>],
    counts: &RadixArray<usize>,
    level: usize,
) -> RadixArray<usize>
where
    T: SortValue,
{
    if src_bucket.is_empty() {
        return RadixArray::new(0);
    } else if src_bucket.len() == 1 {
        let mut counts = RadixArray::new(0);
        dst_bucket[0] = MaybeUninit::new(src_bucket[0]);
        *counts.get_mut(src_bucket[0].get_level_checked(level)) = 1;
        return counts;
    }

    let next_level = level + 1;
    let mut prefix_sums = get_prefix_sums(counts);
    let mut next_counts_0 = RadixArray::new(0);
    let mut next_counts_1 = RadixArray::new(0);

    let chunks = src_bucket.chunks_exact(8);
    let rem = chunks.remainder();

    chunks.into_iter().for_each(|chunk| {
        let b0 = chunk[0].get_level_checked(level);
        let bn0 = chunk[0].get_level_checked(next_level);
        let b1 = chunk[1].get_level_checked(level);
        let bn1 = chunk[1].get_level_checked(next_level);
        let b2 = chunk[2].get_level_checked(level);
        let bn2 = chunk[2].get_level_checked(next_level);
        let b3 = chunk[3].get_level_checked(level);
        let bn3 = chunk[3].get_level_checked(next_level);
        let b4 = chunk[4].get_level_checked(level);
        let bn4 = chunk[4].get_level_checked(next_level);
        let b5 = chunk[5].get_level_checked(level);
        let bn5 = chunk[5].get_level_checked(next_level);
        let b6 = chunk[6].get_level_checked(level);
        let bn6 = chunk[6].get_level_checked(next_level);
        let b7 = chunk[7].get_level_checked(level);
        let bn7 = chunk[7].get_level_checked(next_level);

        dst_bucket[prefix_sums.get(b0)] = MaybeUninit::new(chunk[0]);
        *prefix_sums.get_mut(b0) += 1;
        *next_counts_0.get_mut(bn0) += 1;
        dst_bucket[prefix_sums.get(b1)] = MaybeUninit::new(chunk[1]);
        *prefix_sums.get_mut(b1) += 1;
        *next_counts_1.get_mut(bn1) += 1;
        dst_bucket[prefix_sums.get(b2)] = MaybeUninit::new(chunk[2]);
        *prefix_sums.get_mut(b2) += 1;
        *next_counts_0.get_mut(bn2) += 1;
        dst_bucket[prefix_sums.get(b3)] = MaybeUninit::new(chunk[3]);
        *prefix_sums.get_mut(b3) += 1;
        *next_counts_1.get_mut(bn3) += 1;
        dst_bucket[prefix_sums.get(b4)] = MaybeUninit::new(chunk[4]);
        *prefix_sums.get_mut(b4) += 1;
        *next_counts_0.get_mut(bn4) += 1;
        dst_bucket[prefix_sums.get(b5)] = MaybeUninit::new(chunk[5]);
        *prefix_sums.get_mut(b5) += 1;
        *next_counts_1.get_mut(bn5) += 1;
        dst_bucket[prefix_sums.get(b6)] = MaybeUninit::new(chunk[6]);
        *prefix_sums.get_mut(b6) += 1;
        *next_counts_0.get_mut(bn6) += 1;
        dst_bucket[prefix_sums.get(b7)] = MaybeUninit::new(chunk[7]);
        *prefix_sums.get_mut(b7) += 1;
        *next_counts_1.get_mut(bn7) += 1;
    });

    rem.iter().for_each(|val| {
        let b = val.get_level_checked(level);
        let bn = val.get_level_checked(next_level);
        dst_bucket[prefix_sums.get(b)] = MaybeUninit::new(*val);
        *prefix_sums.get_mut(b) += 1;
        *next_counts_0.get_mut(bn) += 1;
    });

    for i in 0..=255 {
        *next_counts_0.get_mut(i) += next_counts_1.get(i);
    }

    next_counts_0
}

#[inline]
pub fn lr_out_of_place_sort<T>(
    src_bucket: &[T],
    // XXX: After calling this function, all
    // values in dst_bucket _must_ be considered
    // initialized. It's up to this function
    // to maintain that invariant for all callers
    // that expect this behaviour.
    dst_bucket: &mut [MaybeUninit<T>],
    counts: &RadixArray<usize>,
    level: usize,
) where
    T: SortValue,
{
    if src_bucket.len() < 2 {
        dst_bucket.copy_from_slice(bucket_as_uninit(src_bucket));
        return;
    }

    let mut offsets = get_prefix_sums(counts);
    let mut ends = RadixArray::new(0);

    for (i, b) in offsets.iter().enumerate() {
        *ends.get_mut(i) = b + counts.get(i).saturating_sub(1);
    }

    let mut left = 0;
    let mut right = src_bucket.len() - 1;
    let pre = src_bucket.len() % 8;

    for _ in 0..pre {
        let b = src_bucket[right].get_level_checked(level);

        dst_bucket[ends.get(b)] = MaybeUninit::new(src_bucket[right]);
        *ends.get_mut(b) = ends.get(b).saturating_sub(1);
        right = right.saturating_sub(1);
    }

    if pre == src_bucket.len() {
        return;
    }

    let end = (src_bucket.len() - pre) / 2;

    while left < end {
        let bl_0 = src_bucket[left].get_level_checked(level);
        let bl_1 = src_bucket[left + 1].get_level_checked(level);
        let bl_2 = src_bucket[left + 2].get_level_checked(level);
        let bl_3 = src_bucket[left + 3].get_level_checked(level);
        let br_0 = src_bucket[right].get_level_checked(level);
        let br_1 = src_bucket[right - 1].get_level_checked(level);
        let br_2 = src_bucket[right - 2].get_level_checked(level);
        let br_3 = src_bucket[right - 3].get_level_checked(level);

        dst_bucket[offsets.get(bl_0)] = MaybeUninit::new(src_bucket[left]);
        *offsets.get_mut(bl_0) = offsets.get(bl_0).wrapping_add(1);
        dst_bucket[ends.get(br_0)] = MaybeUninit::new(src_bucket[right]);
        *ends.get_mut(br_0) = ends.get(br_0).wrapping_sub(1);
        dst_bucket[offsets.get(bl_1)] = MaybeUninit::new(src_bucket[left + 1]);
        *offsets.get_mut(bl_1) = offsets.get(bl_1).wrapping_add(1);
        dst_bucket[ends.get(br_1)] = MaybeUninit::new(src_bucket[right - 1]);
        *ends.get_mut(br_1) = ends.get(br_1).wrapping_sub(1);
        dst_bucket[offsets.get(bl_2)] = MaybeUninit::new(src_bucket[left + 2]);
        *offsets.get_mut(bl_2) = offsets.get(bl_2).wrapping_add(1);
        dst_bucket[ends.get(br_2)] = MaybeUninit::new(src_bucket[right - 2]);
        *ends.get_mut(br_2) = ends.get(br_2).wrapping_sub(1);
        dst_bucket[offsets.get(bl_3)] = MaybeUninit::new(src_bucket[left + 3]);
        *offsets.get_mut(bl_3) = offsets.get(bl_3).wrapping_add(1);
        dst_bucket[ends.get(br_3)] = MaybeUninit::new(src_bucket[right - 3]);
        *ends.get_mut(br_3) = ends.get(br_3).wrapping_sub(1);

        left += 4;
        right -= 4;
    }
}

#[inline]
pub fn lr_out_of_place_sort_with_counts<T>(
    src_bucket: &[T],
    // XXX: After calling this function, all
    // values in dst_bucket _must_ be considered
    // initialized. It's up to this function
    // to maintain that invariant for all callers
    // that expect this behaviour.
    dst_bucket: &mut [MaybeUninit<T>],
    counts: &RadixArray<usize>,
    level: usize,
) -> RadixArray<usize>
where
    T: SortValue,
{
    if src_bucket.is_empty() {
        return RadixArray::new(0);
    } else if src_bucket.len() == 1 {
        let mut counts = RadixArray::new(0);
        dst_bucket[0] = MaybeUninit::new(src_bucket[0]);
        *counts.get_mut(src_bucket[0].get_level_checked(level)) = 1;
        return counts;
    }

    let next_level = level + 1;
    let mut next_counts_0 = RadixArray::new(0);
    let mut next_counts_1 = RadixArray::new(0);

    let mut offsets = get_prefix_sums(counts);
    let mut ends = RadixArray::new(0);

    for (i, b) in offsets.iter().enumerate() {
        *ends.get_mut(i) = b + counts.get(i).saturating_sub(1);
    }

    let mut left = 0;
    let mut right = src_bucket.len() - 1;
    let pre = src_bucket.len() % 8;

    for _ in 0..pre {
        let b = src_bucket[right].get_level_checked(level);
        let bn = src_bucket[right].get_level_checked(next_level);

        dst_bucket[ends.get(b)] = MaybeUninit::new(src_bucket[right]);
        *ends.get_mut(b) = ends.get(b).wrapping_sub(1);
        right = right.wrapping_sub(1);
        *next_counts_0.get_mut(bn) += 1;
    }

    if pre == src_bucket.len() {
        return next_counts_0;
    }

    let end = (src_bucket.len() - pre) / 2;

    while left < end {
        let bl_0 = src_bucket[left].get_level_checked(level);
        let bl_1 = src_bucket[left + 1].get_level_checked(level);
        let bl_2 = src_bucket[left + 2].get_level_checked(level);
        let bl_3 = src_bucket[left + 3].get_level_checked(level);
        let br_0 = src_bucket[right].get_level_checked(level);
        let br_1 = src_bucket[right - 1].get_level_checked(level);
        let br_2 = src_bucket[right - 2].get_level_checked(level);
        let br_3 = src_bucket[right - 3].get_level_checked(level);

        dst_bucket[offsets.get(bl_0)] = MaybeUninit::new(src_bucket[left]);
        dst_bucket[ends.get(br_0)] = MaybeUninit::new(src_bucket[right]);
        *ends.get_mut(br_0) = ends.get(br_0).wrapping_sub(1);
        *offsets.get_mut(bl_0) = offsets.get(bl_0).wrapping_add(1);

        dst_bucket[offsets.get(bl_1)] = MaybeUninit::new(src_bucket[left + 1]);
        dst_bucket[ends.get(br_1)] = MaybeUninit::new(src_bucket[right - 1]);
        *ends.get_mut(br_1) = ends.get(br_1).wrapping_sub(1);
        *offsets.get_mut(bl_1) = offsets.get(bl_1).wrapping_add(1);

        dst_bucket[offsets.get(bl_2)] = MaybeUninit::new(src_bucket[left + 2]);
        dst_bucket[ends.get(br_2)] = MaybeUninit::new(src_bucket[right - 2]);
        *ends.get_mut(br_2) = ends.get(br_2).wrapping_sub(1);
        *offsets.get_mut(bl_2) = offsets.get(bl_2).wrapping_add(1);

        dst_bucket[offsets.get(bl_3)] = MaybeUninit::new(src_bucket[left + 3]);
        dst_bucket[ends.get(br_3)] = MaybeUninit::new(src_bucket[right - 3]);
        *ends.get_mut(br_3) = ends.get(br_3).wrapping_sub(1);
        *offsets.get_mut(bl_3) = offsets.get(bl_3).wrapping_add(1);

        let bnl_0 = src_bucket[left].get_level_checked(next_level);
        let bnl_1 = src_bucket[left + 1].get_level_checked(next_level);
        let bnl_2 = src_bucket[left + 2].get_level_checked(next_level);
        let bnl_3 = src_bucket[left + 3].get_level_checked(next_level);
        let bnr_0 = src_bucket[right].get_level_checked(next_level);
        let bnr_1 = src_bucket[right - 1].get_level_checked(next_level);
        let bnr_2 = src_bucket[right - 2].get_level_checked(next_level);
        let bnr_3 = src_bucket[right - 3].get_level_checked(next_level);

        *next_counts_0.get_mut(bnl_0) += 1;
        *next_counts_1.get_mut(bnr_0) += 1;
        *next_counts_0.get_mut(bnl_1) += 1;
        *next_counts_1.get_mut(bnr_1) += 1;
        *next_counts_0.get_mut(bnl_2) += 1;
        *next_counts_1.get_mut(bnr_2) += 1;
        *next_counts_0.get_mut(bnl_3) += 1;
        *next_counts_1.get_mut(bnr_3) += 1;

        left += 4;
        right -= 4;
    }

    for i in 0..=255 {
        *next_counts_0.get_mut(i) += next_counts_1.get(i);
    }

    next_counts_0
}
