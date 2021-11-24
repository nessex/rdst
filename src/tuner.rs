#[derive(Clone)]
pub struct TuningParams {
    pub threads: usize,
    pub level: usize,
    pub total_levels: usize,
    pub input_len: usize,
    pub parent_len: Option<usize>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Algorithm {
    #[allow(unused)]
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

pub trait Tuner {
    fn pick_algorithm(&self, p: &TuningParams, counts: &[usize]) -> Algorithm;
}
