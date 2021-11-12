use crate::utils::*;
use crate::RadixKey;

#[inline]
pub fn out_of_place_sort<T>(src_bucket: &[T], dst_bucket: &mut [T], counts: &[usize], level: usize)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let mut prefix_sums = get_prefix_sums(counts);

    let chunks = src_bucket.chunks_exact(8);
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

        dst_bucket[prefix_sums[a]] = chunk[0];
        prefix_sums[a] += 1;
        dst_bucket[prefix_sums[b]] = chunk[1];
        prefix_sums[b] += 1;
        dst_bucket[prefix_sums[c]] = chunk[2];
        prefix_sums[c] += 1;
        dst_bucket[prefix_sums[d]] = chunk[3];
        prefix_sums[d] += 1;
        dst_bucket[prefix_sums[e]] = chunk[4];
        prefix_sums[e] += 1;
        dst_bucket[prefix_sums[f]] = chunk[5];
        prefix_sums[f] += 1;
        dst_bucket[prefix_sums[g]] = chunk[6];
        prefix_sums[g] += 1;
        dst_bucket[prefix_sums[h]] = chunk[7];
        prefix_sums[h] += 1;
    });

    rem.iter().for_each(|val| {
        let b = val.get_level(level) as usize;
        dst_bucket[prefix_sums[b]] = *val;
        prefix_sums[b] += 1;
    });
}
