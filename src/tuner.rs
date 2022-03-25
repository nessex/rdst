#[derive(Clone)]
pub struct TuningParams {
    pub threads: usize,
    pub level: usize,
    pub total_levels: usize,
    pub input_len: usize,
    pub parent_len: Option<usize>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[cfg(feature = "multi-threaded")]
pub enum Algorithm {
    MtOop,
    MtLsb,
    Scanning,
    Recombinating,
    Comparative,
    LrLsb,
    Lsb,
    Regions,
    Ska,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[cfg(not(feature = "multi-threaded"))]
pub enum Algorithm {
    Comparative,
    LrLsb,
    Lsb,
    Ska,
}

pub trait Tuner {
    fn pick_algorithm(&self, p: &TuningParams, counts: &[usize]) -> Algorithm;
}
