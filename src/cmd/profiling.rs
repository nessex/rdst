/// NOTE: The primary use-case for this example is for running a large sort with cargo-instruments.
/// It must be run with `--features=tuning`.
///
/// e.g.
/// ```
/// RUSTFLAGS='--cfg bench --cfg tuning -g -C opt-level=3 -C force-frame-pointers=y -C target-cpu=native -C target-feature=+neon' cargo +nightly instruments -t time --bin profiling --features profiling
/// ```

#[cfg(not(all(tuning, bench)))]
compile_error!("This binary must be run with `RUSTFLAGS='--cfg tuning --cfg bench'`");

use rdst::utils::test_utils::gen_inputs;
use rdst::RadixSort;
use std::thread::sleep;
use std::time::{Duration, Instant};

fn main() {
    // Randomly generate an array of
    // 200_000_000 u64's with half shifted >> 32 and half shifted << 32
    let mut inputs = gen_inputs(200_000_000, 16u32);

    // Input generation is multi-threaded and hard to differentiate from the actual
    // sorting algorithm, depending on the profiler. This makes it more obvious.
    sleep(Duration::from_millis(300));

    let time = Instant::now();
    inputs.radix_sort_unstable();

    println!("Elapsed: {}ms", time.elapsed().as_millis());
    println!("{:?}", &inputs[0..5]);
}
