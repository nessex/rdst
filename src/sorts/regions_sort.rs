use crate::director::director;
use crate::tuning_parameters::TuningParameters;
use crate::utils::*;
use crate::RadixKey;
use arbitrary_chunks::ArbitraryChunks;
use rayon::prelude::*;
use crate::sorts::ska_sort::ska_sort;

struct Edge {
    weight: usize,
    src: usize,
    dst: usize,
    head: usize,
    tail: usize,
}

pub fn regions_sort<T>(tuning: &TuningParameters, bucket: &mut [T], level: usize)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    let bucket_len = bucket.len();
    let chunk_size = (bucket_len / tuning.cpus) + 1;

    let locals: Vec<([usize; 256], [usize; 256])> = bucket
        .par_chunks_mut(chunk_size)
        .map(|chunk| {
            let counts = get_counts(chunk, level);

            ska_sort(chunk, &counts, level);

            let sums = get_prefix_sums(&counts);

            (counts, sums)
        })
        .collect();

    let mut global_counts = vec![0usize; 256];

    locals.iter().for_each(|(counts, _)| {
        for (i, c) in counts.iter().enumerate() {
            global_counts[i] += *c;
        }
    });

    let global_sums = get_prefix_sums(&global_counts);

    let (graph_heads, graph_tails) = {
        let mut tails = Vec::new();

        tails.extend(global_sums[1..].iter());
        tails.push(global_sums[255] + global_counts[255]);

        for (index, (counts, sums)) in locals.iter().enumerate() {
            let offset = index * chunk_size;

            tails.extend(sums[1..].iter().map(|s| s + offset));
        }

        let mut chunk_end = chunk_size;
        loop {
            tails.push(chunk_end);
            chunk_end += chunk_size;
            if chunk_end > bucket_len {
                break;
            }
        }

        tails.par_sort_unstable();

        let mut heads = vec![0usize; 1];
        heads.extend(tails.iter());
        let _ = heads.pop();

        (heads, tails)
    };

    let mut edges: Vec<Edge> = Vec::with_capacity(graph_heads.len());
    let mut country = 0;
    let mut country_end = global_counts[0];

    for (h, t) in graph_heads.iter().zip(graph_tails.iter()) {
        let weight = *t - *h;
        let mut src = country;

        while country_end < *t {
            country += 1;
            country_end += global_counts[country];
            src = country;
        }

        let mut dst = bucket[*h].get_level(level) as usize;

        if src == dst {
            continue;
        }

        edges.push(Edge {
            weight,
            src,
            dst,
            head: *h,
            tail: *t,
        });
    }

    if level == 0 {
        return;
    }

    bucket
        .arbitrary_chunks_mut(global_counts)
        .par_bridge()
        .for_each(|chunk| director(tuning, chunk, bucket_len, level - 1));
}

#[cfg(test)]
mod tests {
    use crate::sorts::recombinating_sort::recombinating_sort;
    use crate::test_utils::{sort_comparison_suite, NumericTest};
    use crate::tuning_parameters::TuningParameters;

    fn test_recombinating_sort<T>(shift: T)
    where
        T: NumericTest<T>,
    {
        let tuning = TuningParameters::new(T::LEVELS);
        sort_comparison_suite(shift, |inputs| {
            recombinating_sort(&tuning, inputs, T::LEVELS - 1)
        });
    }

    #[test]
    pub fn test_u8() {
        test_recombinating_sort(0u8);
    }

    #[test]
    pub fn test_u16() {
        test_recombinating_sort(8u16);
    }

    #[test]
    pub fn test_u32() {
        test_recombinating_sort(16u32);
    }

    #[test]
    pub fn test_u64() {
        test_recombinating_sort(32u64);
    }

    #[test]
    pub fn test_u128() {
        test_recombinating_sort(64u128);
    }

    #[test]
    pub fn test_usize() {
        test_recombinating_sort(32usize);
    }
}
