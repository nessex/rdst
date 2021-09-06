use nanorand::{RandomGen, Rng, WyRand};
use rdst::{RadixKey, RadixSort};
use std::fmt::{Debug, Display};
use std::ops::{Shl, Shr};

fn run<T>(shift: T)
where
    T: RadixKey
        + Ord
        + RandomGen<WyRand>
        + Clone
        + Debug
        + Display
        + Send
        + Copy
        + Sync
        + Shl<Output = T>
        + Shr<Output = T>,
{
    let n = 200_000_000;
    let mut inputs: Vec<T> = Vec::with_capacity(n);
    let mut rng = WyRand::new();

    for _ in 0..(n / 2) {
        inputs.push(rng.generate::<T>() >> shift);
    }

    for _ in 0..(n / 2) {
        inputs.push(rng.generate::<T>() << shift);
    }

    inputs.radix_sort_unstable();
    println!("{}", inputs[0]);
}

fn main() {
    run::<u64>(32);
}
