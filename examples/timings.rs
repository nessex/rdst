//! # timings
//!
//! This example is more of a benchmark. It is used to run the sorting algorithm across
//! a medley of inputs and output the results as CSV. All numbers are in nanoseconds.
//!
//! ## Usage
//!
//! You may need to tweak the command below for your own machine.
//!
//! ```
//! RUSTFLAGS='-g -C opt-level=3 -C target-cpu=native -C target-feature=+neon' cargo +nightly run --example timings --features=bench -- 1234 "Hello world"
//! ```
//!
//!  - `1234` is where you place the ID for your run. If you are just running a brief test this can be `N/A`, otherwise it should be something like a commit SHA that you can use to find the code for this run again.
//!  - `Hello world` is a description so you can be aware of what you were testing when running this experiment
//!  - `HEADERS=true` is an environment variable you can add to print a header row before the output

#![feature(string_remove_matches)]

use rdst::{RadixKey, RadixSort};
use std::time::Instant;
use rdst::bench_utils::gen_bench_exponential_input_set;

fn print_row(data: Vec<String>) {
    let mut first = true;

    for mut o in data {
        if !first {
            print!(",");
        } else {
            first = false;
        }

        o.remove_matches("\"");

        if o.contains(" ") {
            print!("\"{}\"", o);
        } else {
            print!("{}", o);
        }
    }

    print!("\n");
}

fn bench<T>(inputs: Vec<Vec<T>>, name: &str, results: &mut Vec<String>, headers: &mut Vec<String>)
where
    T: RadixKey + Clone + Copy + Send + Sync
{
    for i in inputs {
        if i.len() == 0 {
            continue;
        }
        headers.push(format!("{}_{}", name,  i.len()).to_string());
        // Warmup
        i.clone().radix_sort_unstable();

        let mut to_sort = i.clone();
        let time = Instant::now();
        to_sort.radix_sort_unstable();
        let elapsed = time.elapsed().as_nanos();
        let items_per_sec = ((i.len() as f64 / elapsed as f64) * 1_000_000_000f64) as u64;

        let mut to_sort = i.clone();
        let time = Instant::now();
        to_sort.radix_sort_unstable();
        let elapsed = time.elapsed().as_nanos();
        let items_per_sec_2 = ((i.len() as f64 / elapsed as f64) * 1_000_000_000f64) as u64;

        let items_per_sec = (items_per_sec + items_per_sec_2) / 2;

        results.push(items_per_sec.to_string());
    }
}

fn main() {
    let print_headers: bool = std::env::var("HEADERS")
        .unwrap_or_else(|_| "false".to_string())
        .parse()
        .unwrap();
    let mut out: Vec<String> = std::env::args().skip(1).take(2).collect();
    assert_eq!(out.len(), 2);
    let mut headers = vec![
        "id".to_string(),
        "description".to_string(),
    ];

    let inputs = gen_bench_exponential_input_set(0u32);
    bench(inputs, "u32", &mut out, &mut headers);

    let inputs = gen_bench_exponential_input_set(16u32);
    bench(inputs, "u32_bimodal", &mut out, &mut headers);

    let inputs = gen_bench_exponential_input_set(0u64);
    bench(inputs, "u64", &mut out, &mut headers);

    let inputs = gen_bench_exponential_input_set(32u64);
    bench(inputs, "u64_bimodal", &mut out, &mut headers);

    if print_headers {
        print_row(headers);
    }

    print_row(out);
}