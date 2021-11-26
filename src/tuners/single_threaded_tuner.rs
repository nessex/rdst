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

        let depth = p.total_levels - p.level - 1;

        if p.input_len >= 5_000 {
            let distribution_threshold = (p.input_len / 256) * 2;

            for c in counts {
                if *c >= distribution_threshold {
                    return if p.input_len > 100_000 && depth < 2 {
                        Algorithm::Ska
                    } else {
                        Algorithm::LrLsb
                    }
                }
            }
        }

        if p.input_len > 800_000 && depth == 0 {
            Algorithm::Ska
        } else {
            Algorithm::Lsb
        }
    }
}
