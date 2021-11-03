use std::cmp::max;

#[derive(Clone)]
pub struct TuningParameters {
    pub cpus: usize,
    pub regions_sort_threshold: usize,
    pub inplace_sort_lsb_threshold: usize,
    pub recombinating_sort_threshold: usize,
    pub scanning_sort_threshold: usize,
    pub ska_sort_threshold: usize,
    pub comparative_sort_threshold: usize,
    pub par_count_threshold: usize,
    pub scanner_read_size: usize,
}

impl TuningParameters {
    pub fn new(_levels: usize) -> Self {
        let cpus = rayon::current_num_threads();
        Self {
            cpus,
            regions_sort_threshold: Self::regions_sort_threshold(),
            inplace_sort_lsb_threshold: Self::inplace_sort_lsb_threshold(),
            recombinating_sort_threshold: Self::recombinating_sort_threshold(),
            scanning_sort_threshold: Self::scanning_sort_threshold(),
            ska_sort_threshold: Self::ska_sort_threshold(),
            comparative_sort_threshold: Self::comparative_sort_threshold(),
            par_count_threshold: Self::par_count_threshold(),
            scanner_read_size: Self::scanner_read_size(cpus),
        }
    }

    fn regions_sort_threshold() -> usize {
        800_000
    }

    fn inplace_sort_lsb_threshold() -> usize {
        50_000
    }

    fn recombinating_sort_threshold() -> usize {
        200_000
    }

    fn scanning_sort_threshold() -> usize {
        50_000_000
    }

    fn ska_sort_threshold() -> usize {
        85_000
    }

    fn comparative_sort_threshold() -> usize {
        128
    }

    fn par_count_threshold() -> usize {
        400_000
    }

    fn scanner_read_size(cpus: usize) -> usize {
        let scaling_factor = max(1, (cpus as f32).log2().ceil() as isize) as usize;

        32768 / scaling_factor
    }
}
