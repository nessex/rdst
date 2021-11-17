use crate::utils::*;
use crate::RadixKey;

#[inline]
pub fn out_of_place_sort<T>(src_bucket: &[T], dst_bucket: &mut [T], counts: &[usize], level: usize)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let mut prefix_sums = get_prefix_sums(counts);
    let mut end_offsets = get_end_offsets(&counts, &prefix_sums);

    for e in end_offsets.iter_mut() {
        if *e == 0 {
            continue;
        }

        *e -= 1;
    }

    let mut left = 0;
    let mut right = src_bucket.len() - 1;
    let pre = src_bucket.len() % 8;

    for _ in 0..pre {
        let b = src_bucket[right].get_level(level) as usize;

        dst_bucket[end_offsets[b]] = src_bucket[right];
        end_offsets[b] = end_offsets[b].saturating_sub(1);
        right = right.saturating_sub(1);
    }

    loop {
        if left >= right {
            break;
        }

        let bl_0 = src_bucket[left].get_level(level) as usize;
        let bl_1 = src_bucket[left + 1].get_level(level) as usize;
        let bl_2 = src_bucket[left + 2].get_level(level) as usize;
        let bl_3 = src_bucket[left + 3].get_level(level) as usize;
        let br_0 = src_bucket[right].get_level(level) as usize;
        let br_1 = src_bucket[right - 1].get_level(level) as usize;
        let br_2 = src_bucket[right - 2].get_level(level) as usize;
        let br_3 = src_bucket[right - 3].get_level(level) as usize;

        dst_bucket[prefix_sums[bl_0]] = src_bucket[left];
        prefix_sums[bl_0] += 1;
        dst_bucket[end_offsets[br_0]] = src_bucket[right];
        end_offsets[br_0] = end_offsets[br_0].saturating_sub(1);
        dst_bucket[prefix_sums[bl_1]] = src_bucket[left + 1];
        prefix_sums[bl_1] += 1;
        dst_bucket[end_offsets[br_1]] = src_bucket[right - 1];
        end_offsets[br_1] = end_offsets[br_1].saturating_sub(1);
        dst_bucket[prefix_sums[bl_2]] = src_bucket[left + 2];
        prefix_sums[bl_2] += 1;
        dst_bucket[end_offsets[br_2]] = src_bucket[right - 2];
        end_offsets[br_2] = end_offsets[br_2].saturating_sub(1);
        dst_bucket[prefix_sums[bl_3]] = src_bucket[left + 3];
        prefix_sums[bl_3] += 1;
        dst_bucket[end_offsets[br_3]] = src_bucket[right - 3];
        end_offsets[br_3] = end_offsets[br_3].saturating_sub(1);

        left += 4;
        right = right.saturating_sub(4);
    }
}
