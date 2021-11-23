use crate::tuner::{Algorithm, Tuner, TuningParams};

pub struct StandardTuner;
impl Tuner for StandardTuner {
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
                            0..=200_000 => Algorithm::LrLsbSort,
                            200_001..=350_000 => Algorithm::SkaSort,
                            350_001..=4_000_000 => Algorithm::MtLsbSort,
                            4_000_001..=usize::MAX => Algorithm::RegionsSort,
                            _ => Algorithm::LrLsbSort,
                        }
                    } else {
                        match p.input_len {
                            0..=200_000 => Algorithm::LrLsbSort,
                            200_001..=800_000 => Algorithm::SkaSort,
                            800_001..=5_000_000 => Algorithm::RecombinatingSort,
                            5_000_001..=usize::MAX => Algorithm::RegionsSort,
                            _ => Algorithm::LrLsbSort,
                        }
                    };
                }
            }
        }

        if depth > 0 {
            match p.input_len {
                0..=200_000 => Algorithm::LsbSort,
                200_001..=800_000 => Algorithm::SkaSort,
                800_001..=50_000_000 => Algorithm::RecombinatingSort,
                50_000_001..=usize::MAX => Algorithm::ScanningSort,
                _ => Algorithm::LsbSort,
            }
        } else {
            match p.input_len {
                0..=150_000 => Algorithm::LsbSort,
                150_001..=260_000 => Algorithm::SkaSort,
                260_001..=50_000_000 => Algorithm::RecombinatingSort,
                50_000_001..=usize::MAX => Algorithm::ScanningSort,
                _ => Algorithm::LsbSort,
            }
        }
    }
}
