use std::cmp::min;

pub struct TuningParameters {
    pub comparison_sort_threshold: usize,
    pub scanning_sort_threshold: usize,
    pub ska_sort_threshold: usize,
    pub par_count_threshold: usize,
    pub scanner_read_size: usize,
}

impl TuningParameters {
    pub fn new(levels: usize) -> Self {
        Self {
            comparison_sort_threshold: 32,
            scanning_sort_threshold: Self::scanning_sort_threshold(),
            ska_sort_threshold: Self::ska_sort_threshold(levels),
            par_count_threshold: Self::par_count_threshold(),
            scanner_read_size: Self::scanner_read_size(),
        }
    }

    fn scanning_sort_threshold() -> usize {
        200_000
    }

    fn ska_sort_threshold(levels: usize) -> usize {
        if levels <= 4 {
            500_000
        } else {
            200_000
        }
    }

    fn par_count_threshold() -> usize {
        220_000
    }

    fn scanner_read_size() -> usize {
        let cpus = num_cpus::get();
        let scaling_factor = min(1, (cpus as f32).log2().ceil() as isize) as usize;

        65536 / scaling_factor
    }
}