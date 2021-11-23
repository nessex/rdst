#[derive(Clone)]
pub struct TuningParams {
    pub threads: usize,
    pub level: usize,
    pub total_levels: usize,
    pub input_len: usize,
    pub parent_len: usize,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Algorithm {
    MtOopSort,
    MtLsbSort,
    ScanningSort,
    RecombinatingSort,
    ComparativeSort,
    LrLsbSort,
    LsbSort,
    RegionsSort,
    SkaSort,
}

pub trait Tuner {
    fn pick_algorithm(&self, p: &TuningParams, counts: &[usize]) -> Algorithm;
}
