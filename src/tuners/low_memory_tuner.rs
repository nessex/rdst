use crate::tuner::{Algorithm, Tuner, TuningParams};

pub struct LowMemoryTuner;
impl Tuner for LowMemoryTuner {
    #[inline]
    fn pick_algorithm(&self, p: &TuningParams, counts: &[usize]) -> Algorithm {
        if p.input_len <= 128 {
            return Algorithm::Comparative;
        }

        if p.input_len >= 5_000 {
            let distribution_threshold = (p.input_len / 256) * 2;

            // Distribution occurs when the input to be sorted has counts significantly
            // larger than the others
            for c in counts {
                if *c >= distribution_threshold {
                    return match p.input_len {
                        0..=50_000 => Algorithm::LrLsb,
                        50_001..=1_000_000 => Algorithm::Ska,
                        1_000_001..=usize::MAX => Algorithm::Regions,
                        _ => Algorithm::Lsb,
                    }
                }
            }
        }

        match p.input_len {
            0..=50_000 => Algorithm::Lsb,
            50_001..=1_000_000 => Algorithm::Ska,
            1_000_001..=usize::MAX => Algorithm::Regions,
            _ => Algorithm::Lsb,
        }
    }
}
