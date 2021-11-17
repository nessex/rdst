use arbitrary_chunks::ArbitraryChunks;
use rayon::prelude::*;
use crate::utils::*;
use crate::RadixKey;
use crate::sorts::ska_sort::ska_sort;

#[inline]
pub const fn cdiv(a: usize, b: usize) -> usize {
    (a + b - 1) / b
}

fn counts_to_minor_counts(counts: Vec<[usize; 256]>) -> Vec<usize> {
    let mut minor_counts = Vec::with_capacity(256 * counts.len());

    for b in 0..256 {
        for c in 0..counts.len() {
            minor_counts.push(counts[c][b]);
        }
    }

    minor_counts
}

fn get_minor_counts<T>(tile_size: usize, src_bucket: &mut [T], level: usize) -> Vec<usize>
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let locals: Vec<[usize; 256]> = src_bucket
        .par_chunks(tile_size)
        .map(|chunk| get_counts(chunk, level))
        .collect();

    counts_to_minor_counts(locals)
}

pub fn chunk_and_collate<T>(dst_bucket: &mut [T], minor_counts: Vec<usize>, num_tiles: usize) -> Vec<Vec<(usize, &mut [T])>>
{
    let chunks: Vec<&mut [T]> = dst_bucket.arbitrary_chunks_mut(minor_counts).collect();

    let mut running_total = 0;
    let mut chunks: Vec<(usize, &mut [T])> = chunks.into_iter().map(|c| {
        let t = running_total;
        running_total += c.len();

        (t, c)
    }).collect();

    chunks.reverse();

    let mut collated_chunks: Vec<Vec<(usize, &mut [T])>> = Vec::with_capacity(num_tiles);
    collated_chunks.resize_with(num_tiles, || Vec::with_capacity(256));

    for _ in 0..256 {
        for t in 0..num_tiles {
            collated_chunks[t].push(chunks.pop().unwrap());
        }
    }

    collated_chunks
}

pub fn write_and_count<T>(
    src_bucket: &mut [T],
    dst_chunks: Vec<Vec<(usize, &mut [T])>>,
    level: usize,
    tile_exponent: usize,
    tile_size: usize,
    num_tiles: usize,
) -> Vec<usize>
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let next_level = level + 1;
    let mut all_counts: Vec<Vec<[usize; 256]>> = dst_chunks
        .into_par_iter()
        .zip(src_bucket.par_chunks(tile_size))
        .map(|(mut buckets, bucket)| {
            let mut offsets = [0usize; 256];
            let mut next_counts = Vec::new();
            next_counts.resize_with(num_tiles, || [0usize; 256]);

            let chunks = bucket.chunks_exact(4);
            let rem = chunks.remainder();

            chunks.into_iter().for_each(|chunk| {
                let b0 = chunk[0].get_level(level) as usize;
                let nb0 = chunk[0].get_level(next_level) as usize;
                let b1 = chunk[1].get_level(level) as usize;
                let nb1 = chunk[1].get_level(next_level) as usize;
                let b2 = chunk[2].get_level(level) as usize;
                let nb2 = chunk[2].get_level(next_level) as usize;
                let b3 = chunk[3].get_level(level) as usize;
                let nb3 = chunk[3].get_level(next_level) as usize;

                next_counts[(buckets[b0].0 + offsets[b0]) >> tile_exponent][nb0] += 1;
                buckets[b0].1[offsets[b0]] = chunk[0];
                offsets[b0] += 1;

                next_counts[(buckets[b1].0 + offsets[b1]) >> tile_exponent][nb1] += 1;
                buckets[b1].1[offsets[b1]] = chunk[1];
                offsets[b1] += 1;

                next_counts[(buckets[b2].0 + offsets[b2]) >> tile_exponent][nb2] += 1;
                buckets[b2].1[offsets[b2]] = chunk[2];
                offsets[b2] += 1;

                next_counts[(buckets[b3].0 + offsets[b3]) >> tile_exponent][nb3] += 1;
                buckets[b3].1[offsets[b3]] = chunk[3];
                offsets[b3] += 1;
            });

            rem.into_iter().for_each(|v| {
                let b = v.get_level(level) as usize;
                let next_b = v.get_level(next_level) as usize;
                let offs = offsets[b];

                next_counts[(buckets[b].0 + offs) >> tile_exponent][next_b] += 1;
                buckets[b].1[offs] = *v;
                offsets[b] += 1;
            });

            next_counts
        })
        .collect();

    let mut out = all_counts.pop().unwrap();

    while let Some(subcounts) = all_counts.pop() {
        for (i, counts) in subcounts.into_iter().enumerate() {
            for (ii, c) in counts.iter().enumerate() {
                out[i][ii] += c;
            }
        }
    }

    counts_to_minor_counts(out)
}

pub fn write<T>(
    src_bucket: &mut [T],
    dst_chunks: Vec<Vec<(usize, &mut [T])>>,
    level: usize,
    tile_size: usize,
)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    dst_chunks
        .into_par_iter()
        .zip(src_bucket.par_chunks(tile_size))
        .for_each(|(mut buckets, bucket)| {
            let mut offsets = [0usize; 256];

            let chunks = bucket.chunks_exact(4);
            let rem = chunks.remainder();

            chunks.into_iter().for_each(|chunk| {
                let b0 = chunk[0].get_level(level) as usize;
                let b1 = chunk[1].get_level(level) as usize;
                let b2 = chunk[2].get_level(level) as usize;
                let b3 = chunk[3].get_level(level) as usize;

                buckets[b0].1[offsets[b0]] = chunk[0];
                offsets[b0] += 1;

                buckets[b1].1[offsets[b1]] = chunk[1];
                offsets[b1] += 1;

                buckets[b2].1[offsets[b2]] = chunk[2];
                offsets[b2] += 1;

                buckets[b3].1[offsets[b3]] = chunk[3];
                offsets[b3] += 1;
            });

            rem.into_iter().for_each(|v| {
                let b = v.get_level(level) as usize;
                let offs = offsets[b];

                buckets[b].1[offs] = *v;
                offsets[b] += 1;
            });
        });
}

pub fn mt_lsb_sort_adapter<T>(bucket: &mut [T], start_level: usize, end_level: usize)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    if bucket.len() < 2 {
        return;
    }

    let bucket_len = bucket.len();
    let mut tmp_bucket = get_tmp_bucket(bucket_len);
    let levels: Vec<usize> = (start_level..=end_level).into_iter().collect();
    let mut invert = false;
    let mut first = true;

    let tile_exponent = 23;
    let tile_size = 1 << tile_exponent;
    let num_tiles = cdiv(bucket_len, tile_size);
    let mut next_counts = Vec::new();

    for level in levels {
        if first == true && (end_level - start_level) % 2 == 0 {
            // Use ska sort if the levels in question here will likely require an additional copy
            // at the end.
            let counts = par_get_counts(bucket, level);
            let plateaus = detect_plateaus(bucket, level);
            let (mut prefix_sums, end_offsets) = apply_plateaus(bucket, &counts, &plateaus);
            ska_sort(bucket, &mut prefix_sums, &end_offsets, level);
        } else {
            if invert {
                if next_counts.is_empty() {
                    next_counts = get_minor_counts(tile_size, &mut tmp_bucket, level);
                }

                let dst_chunks = chunk_and_collate(bucket, next_counts.clone(), num_tiles);

                if level == end_level {
                    write(&mut tmp_bucket, dst_chunks, level, tile_size);
                } else {
                    next_counts = write_and_count(&mut tmp_bucket, dst_chunks, level, tile_exponent, tile_size, num_tiles);
                }
            } else {
                if next_counts.is_empty() {
                    next_counts = get_minor_counts(tile_size, bucket, level);
                }

                let dst_chunks = chunk_and_collate(&mut tmp_bucket, next_counts.clone(), num_tiles);

                if level == end_level {
                    write(bucket, dst_chunks, level, tile_size);
                } else {
                    next_counts = write_and_count(bucket, dst_chunks, level, tile_exponent, tile_size, num_tiles);
                }
            };

            invert = !invert;
        }

        first = false;
    }

    if invert {
        bucket.copy_from_slice(&tmp_bucket);
    }
}

#[cfg(test)]
mod tests {
    use crate::RadixSort;
    use crate::sorts::mt_lsb_sort::mt_lsb_sort_adapter;
    use crate::test_utils::{sort_comparison_suite, NumericTest};

    fn test_mt_lsb_sort_adapter<T>(shift: T)
    where
        T: NumericTest<T>,
    {
        sort_comparison_suite(shift, |inputs| mt_lsb_sort_adapter(inputs, 0, T::LEVELS - 1));
    }

    #[test]
    pub fn test_u8() {
        test_mt_lsb_sort_adapter(0u8);
    }

    #[test]
    pub fn test_u16() {
        test_mt_lsb_sort_adapter(8u16);
    }

    #[test]
    pub fn test_u32() {
        test_mt_lsb_sort_adapter(16u32);
    }

    #[test]
    pub fn test_u64() {
        test_mt_lsb_sort_adapter(32u64);
    }

    #[test]
    pub fn test_u128() {
        test_mt_lsb_sort_adapter(64u128);
    }

    #[test]
    pub fn test_usize() {
        test_mt_lsb_sort_adapter(32usize);
    }

    #[test]
    pub fn test_sample() {
        let mut data = [9, 8, 7, 6, 5, 4, 3, 2, 1, 0u32];

        data.radix_sort_unstable();

        assert_eq!(data, [0, 1, 2, 3u32, 4, 5, 6, 7, 8, 9]);
    }
}
