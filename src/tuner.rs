#[derive(Clone)]
pub struct TuningParams {
    pub threads: usize,
    pub level: usize,
    pub total_levels: usize,
    pub input_len: usize,
    pub parent_len: usize,
    pub in_place: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Algorithm {
    MtOopSort,
    MtLsbSort,
    ScanningSort,
    RecombinatingSort,
    ComparativeSort,
    LsbSort,
    RegionsSort,
    SkaSort,
}

pub trait Tuner {
    #[inline]
    fn pick_algorithm(&self, p: &TuningParams, counts: &[usize]) -> Algorithm {
        if p.input_len <= 128 {
            return Algorithm::ComparativeSort;
        }

        let depth = p.total_levels - p.level - 1;

        if p.input_len >= 300_000 {
            let distribution_threshold = ((p.input_len / p.threads) as f64 * 1.4) as usize;

            // Distribution occurs when the input to be sorted has a single count larger
            // than the others.
            for c in counts {
                if *c >= distribution_threshold {
                    return if depth == 0 {
                        if p.input_len >= 1_000_000 {
                            Algorithm::RegionsSort
                        } else if p.input_len >= 30_000 {
                            Algorithm::MtLsbSort
                        } else {
                            Algorithm::LsbSort
                        }
                    } else {
                        if p.input_len >= 5_000_000 {
                            Algorithm::RegionsSort
                        } else if p.input_len >= 30_000 {
                            Algorithm::MtLsbSort
                        } else {
                            Algorithm::LsbSort
                        }
                    }
                }
            }
        }

        if depth > 0 && p.in_place {
            match p.input_len {
                0..=1_000_000 => Algorithm::SkaSort,
                1_000_001..=usize::MAX => Algorithm::RegionsSort,
                _ => Algorithm::LsbSort,
            }
        } else if depth > 0 && !p.in_place {
            match p.input_len {
                200_001..=800_000 => Algorithm::SkaSort,
                800_001..=50_000_000 => Algorithm::RecombinatingSort,
                50_000_001..=usize::MAX => Algorithm::ScanningSort,
                _ => Algorithm::LsbSort,
            }
        } else if depth == 0 && p.in_place {
            match p.input_len {
                0..=1_000_000 => Algorithm::SkaSort,
                1_000_001..=usize::MAX => Algorithm::RegionsSort,
                _ => Algorithm::LsbSort,
            }
        } else if depth == 0 && !p.in_place {
            match p.input_len {
                260_001..=50_000_000 => Algorithm::RecombinatingSort,
                50_000_001..=usize::MAX => Algorithm::ScanningSort,
                _ => Algorithm::LsbSort,
            }
        } else {
            Algorithm::LsbSort
        }
    }
}

pub struct DefaultTuner {}
impl Tuner for DefaultTuner {}
