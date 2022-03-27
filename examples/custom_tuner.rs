use rdst::tuner::{Algorithm, Tuner, TuningParams};
use rdst::RadixSort;

struct MyTuner;

impl Tuner for MyTuner {
    fn pick_algorithm(&self, p: &TuningParams, _counts: &[usize]) -> Algorithm {
        if p.input_len >= 500_000 {
            Algorithm::Ska
        } else {
            Algorithm::Lsb
        }
    }
}

fn main() {
    let mut inputs = Vec::new();
    inputs.extend_from_slice(&[55, 22, 73, 4, 89, 0, 100, 3]);

    inputs.radix_sort_builder().with_tuner(&MyTuner {}).sort();
    println!("{:?}", &inputs[..]);
}
