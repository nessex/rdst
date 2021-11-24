//! `SingleThreadedTuner` is a tuner which only uses single-threaded algorithms.
//!
//! Typically this will be expected to be used in conjunction with
//! `radix_sort_builder().with_parallel(false)` for fully single-threaded operation.
//!
//! SingleThreadedTuner algorithm choice is:
//!  * single-threaded only
//!  * aware of basic count distributions
//!  * dynamic msb / lsb

use crate::tuner::{Algorithm, Tuner, TuningParams};

pub struct SingleThreadedTuner;
impl Tuner for SingleThreadedTuner {
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
                        50_001..=usize::MAX => Algorithm::Ska,
                        _ => Algorithm::LrLsb,
                    }
                }
            }
        }

        match p.input_len {
            0..=128 => Algorithm::Comparative,
            129..=400_000 => Algorithm::Lsb,
            400_001..=usize::MAX => Algorithm::Ska,
            _ => Algorithm::Lsb,
        }
    }
}
