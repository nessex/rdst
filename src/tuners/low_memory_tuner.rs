//! `LowMemoryTuner` provides a tuning that uses primarily in-place or low-memory algorithms.
//! It is not entirely in-place as the speed impact of that is too extreme. Rather, it opts to use
//! out-of-place algorithms only for small inputs or small portions of larger inputs.
//!
//! LowMemoryTuner algorithm choice is:
//!  * multi-threaded
//!  * low-memory / in-place algorithms preferred
//!  * aware of basic count distributions
//!  * dynamic msb / lsb

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

            for c in counts {
                if *c >= distribution_threshold {
                    return match p.input_len {
                        0..=50_000 => Algorithm::LrLsb,
                        50_001..=1_000_000 => Algorithm::Ska,
                        1_000_001..=usize::MAX => Algorithm::Regions,
                        _ => Algorithm::LrLsb,
                    };
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
