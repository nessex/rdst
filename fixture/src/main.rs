use rdst::RadixSort;

fn main() {
    let mut inputs = vec![55, 22, 73, 4, 89, 0, 100, 3];

    inputs.radix_sort_unstable();
    println!("{:?}", &inputs[..]);
}
