use rdst::RadixSort;


fn main() {
    let mut inputs = Vec::new();
    inputs.extend_from_slice(&[55, 22, 73, 4, 89, 0, 100, 3]);

    inputs.radix_sort_unstable();
    println!("{:?}", &inputs[..]);
}
