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
                        } else if p.input_len >= 260_000 {
                            Algorithm::RecombinatingSort
                        } else {
                            Algorithm::LsbSort
                        }
                    } else {
                        if p.input_len >= 5_000_000 {
                            Algorithm::RegionsSort
                        } else if p.input_len >= 800_000 {
                            Algorithm::RecombinatingSort
                        } else {
                            Algorithm::LsbSort
                        }
                    }
                }
            }

            if depth > 0 {
                let to_split = p.input_len > ((p.parent_len / 256) as f64 * 1.4) as usize;

                // Splitting occurs when input is larger than it should be relative to other tasks
                // spawned from the same parent.
                if to_split {
                    return match p.input_len {
                        400_000..=50_000_000 => Algorithm::RecombinatingSort,
                        50_000_0001..=usize::MAX => Algorithm::ScanningSort,
                        _ => Algorithm::SkaSort,
                    };
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
                260_000..=49_999_999 => Algorithm::RecombinatingSort,
                50_000_000..=usize::MAX => Algorithm::ScanningSort,
                _ => Algorithm::LsbSort,
            }
        } else {
            Algorithm::LsbSort
        }
    }
}

pub struct DefaultTuner {}
impl Tuner for DefaultTuner {}
