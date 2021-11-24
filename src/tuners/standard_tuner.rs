//! `StandardTuner` represents the default tuning of algorithm choices offered by rdst.
//!
//! StandardTuner algorithm choice is:
//!  * multi-threaded
//!  * aware of basic count distributions
//!  * dynamic msb / lsb

use crate::tuner::{Algorithm, Tuner, TuningParams};

pub struct StandardTuner;
impl Tuner for StandardTuner {
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
                    return if depth == 0 {
                        match p.input_len {
                            0..=200_000 => Algorithm::LrLsb,
                            200_001..=350_000 => Algorithm::Ska,
                            350_001..=4_000_000 => Algorithm::MtLsb,
                            4_000_001..=usize::MAX => Algorithm::Regions,
                            _ => Algorithm::LrLsb,
                        }
                    } else {
                        match p.input_len {
                            0..=200_000 => Algorithm::LrLsb,
                            200_001..=800_000 => Algorithm::Ska,
                            800_001..=5_000_000 => Algorithm::Recombinating,
                            5_000_001..=usize::MAX => Algorithm::Regions,
                            _ => Algorithm::LrLsb,
                        }
                    };
                }
            }
        }

        if depth > 0 {
            match p.input_len {
                0..=200_000 => Algorithm::Lsb,
                200_001..=800_000 => Algorithm::Ska,
                800_001..=50_000_000 => Algorithm::Recombinating,
                50_000_001..=usize::MAX => Algorithm::Scanning,
                _ => Algorithm::Lsb,
            }
        } else {
            match p.input_len {
                0..=150_000 => Algorithm::Lsb,
                150_001..=260_000 => Algorithm::Ska,
                260_001..=50_000_000 => Algorithm::Recombinating,
                50_000_001..=usize::MAX => Algorithm::Scanning,
                _ => Algorithm::Lsb,
            }
        }
    }
}
