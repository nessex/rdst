use crate::tuner::{Algorithm, Tuner, TuningParams};

pub struct LowMemoryTuner;
impl Tuner for LowMemoryTuner {
    #[inline]
    fn pick_algorithm(&self, p: &TuningParams, counts: &[usize]) -> Algorithm {
        if p.input_len <= 128 {
            return Algorithm::ComparativeSort;
        }

        let depth = p.total_levels - p.level - 1;

        if p.input_len >= 5_000 {
            let distribution_threshold = (p.input_len / 256) * 2;

            // Distribution occurs when the input to be sorted has counts significantly
            // larger than the others
            for c in counts {
                if *c >= distribution_threshold {
                    return if depth == 0 {
                        match p.input_len {
                            0..=50_000 => Algorithm::LrLsbSort,
                            50_001..=1_000_000 => Algorithm::SkaSort,
                            1_000_001..=usize::MAX => Algorithm::RegionsSort,
                            _ => Algorithm::LsbSort,
                        }
                    } else {
                        match p.input_len {
                            0..=50_000 => Algorithm::LrLsbSort,
                            50_001..=1_000_000 => Algorithm::SkaSort,
                            1_000_001..=usize::MAX => Algorithm::RegionsSort,
                            _ => Algorithm::LsbSort,
                        }
                    };
                }
            }
        }

        if depth == 0 {
            match p.input_len {
                0..=50_000 => Algorithm::LsbSort,
                50_001..=1_000_000 => Algorithm::SkaSort,
                1_000_001..=usize::MAX => Algorithm::RegionsSort,
                _ => Algorithm::LsbSort,
            }
        } else {
            match p.input_len {
                0..=50_000 => Algorithm::LsbSort,
                50_001..=1_000_000 => Algorithm::SkaSort,
                1_000_001..=usize::MAX => Algorithm::RegionsSort,
                _ => Algorithm::LsbSort,
            }
        }
    }
}
