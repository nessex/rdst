#[derive(Clone)]
pub struct TuningParams {
    pub threads: usize,
    pub level: usize,
    pub total_levels: usize,
    pub input_len: usize,
    pub parent_len: usize,
    pub in_place: bool,
    pub serial: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Algorithm {
    ScanningSort,
    RecombinatingSort,
    ComparativeSort,
    LsbSort,
    RegionsSort,
    SkaSort,
}

pub trait Tuner {
    fn pick_algorithm(&self, p: &TuningParams) -> Algorithm {
        if p.in_place && p.serial {
            match p.input_len {
                0..=1_000_000 => Algorithm::SkaSort,
                1_000_001..=usize::MAX => Algorithm::RegionsSort,
                _ => Algorithm::SkaSort,
            }
        } else if p.in_place && !p.serial {
            match p.input_len {
                0..=50_000 => Algorithm::LsbSort,
                50_001..=usize::MAX => Algorithm::SkaSort,
                _ => Algorithm::LsbSort,
            }
        } else if !p.in_place && p.serial {
            match p.input_len {
                0..=260_000 => Algorithm::SkaSort,
                260_001..=40_000_000 => Algorithm::RecombinatingSort,
                40_000_001..=usize::MAX => Algorithm::ScanningSort,
                _ => Algorithm::LsbSort,
            }
        } else {
            match p.input_len {
                0..=50_000 => Algorithm::LsbSort,
                50_001..=800_000 => Algorithm::SkaSort,
                800_001..=usize::MAX => Algorithm::RecombinatingSort,
                _ => Algorithm::LsbSort,
            }
        }
    }
}

pub struct DefaultTuner {}
impl Tuner for DefaultTuner {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Point {
    pub depth: usize,
    pub algorithm: Algorithm,
    pub start: usize,
}

#[derive(Clone, Debug)]
pub struct MLTuner {
    pub points: Vec<Point>,
}

impl Tuner for MLTuner {
    fn pick_algorithm(&self, p: &TuningParams) -> Algorithm {
        let depth = p.total_levels - 1 - p.level;
        for point in self.points.iter() {
            if depth == point.depth && p.input_len >= point.start {
                return point.algorithm;
            }
        }

        return Algorithm::LsbSort;
    }
}
