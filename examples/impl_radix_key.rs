use rdst::RadixKey;
use rdst::RadixSort;

// All types implementing `RadixKey` require `Copy` and `Clone`
#[derive(Debug, Copy, Clone)]
struct PackedU8(pub [u8; 4]);

impl RadixKey for PackedU8 {
    const LEVELS: usize = 4;

    #[inline(always)]
    fn get_level(&self, level: usize) -> u8 {
        // Sorts all bytes, from byte 3 down to byte 0
        // NOTE: This is lexicographic ordering
        //
        // level:  0, 1, 2, 3
        //         v  v  v  v
        // byte:  [3, 2, 1, 0]
        //
        // Sorted order:
        // [
        //   [2,3,4,5],
        //   [5,4,3,2],
        // ]
        //
        // By default, `rdst` sorts [u8; N] arrays in the same
        // lexicographic order shown here.
        self.0[3 - level]
    }
}

#[derive(Debug, Copy, Clone)]
struct EvenSortedPackedU8(PackedU8);
impl RadixKey for EvenSortedPackedU8 {
    const LEVELS: usize = 2;

    #[inline(always)]
    fn get_level(&self, level: usize) -> u8 {
        // Only sort the even bytes, [ignore, 2, ignore, 0]
        // Sorts by byte 2, then byte 0
        self.0.0[3 - level * 2]
    }
}

#[derive(Debug, Copy, Clone)]
struct OddSortedPackedU8(PackedU8);
impl RadixKey for OddSortedPackedU8 {
    const LEVELS: usize = 2;

    #[inline(always)]
    fn get_level(&self, level: usize) -> u8 {
        // Only sort the odd bytes, [3, ignore, 1, ignore]
        // Sorts by byte 3, then byte 1
        self.0.0[3 - (level * 2 + 1)]
    }
}

fn main() {
    // 1. Sort all bytes in lexicographic order
    let mut inputs = vec![
        PackedU8([3, 2, 2, 3]),
        PackedU8([2, 2, 2, 2]),
        PackedU8([3, 1, 3, 1]),
    ];

    inputs.radix_sort_unstable();
    println!("{:?}", &inputs[..]);
    // [
    //   PackedU8([2, 2, 2, 2]),
    //   PackedU8([3, 1, 3, 1]),
    //   PackedU8([3, 2, 2, 3]),
    // ]

    // 2: Wrap the same inputs in a type that only sorts the even bytes

    let mut inputs_by_even: Vec<EvenSortedPackedU8> = inputs
        .clone()
        .into_iter()
        .map(|v| EvenSortedPackedU8(v))
        .collect();

    inputs_by_even.radix_sort_unstable();
    println!("{:?}", &inputs_by_even[..]);
    // [
    //   EvenSortedPackedU8(PackedU8([3, 1, 3, 1])), // [ignore, 1, ignore, 1],
    //   EvenSortedPackedU8(PackedU8([2, 2, 2, 2])), // [ignore, 2, ignore, 2],
    //   EvenSortedPackedU8(PackedU8([3, 2, 2, 3])), // [ignore, 2, ignore, 3],
    // ]

    // 3. Wrap the same inputs again, this time to sort only by the odd bytes
    let mut inputs_by_odd: Vec<OddSortedPackedU8> = inputs
        .clone()
        .into_iter()
        .map(|v| OddSortedPackedU8(v))
        .collect();

    inputs_by_odd.radix_sort_unstable();
    println!("{:?}", &inputs_by_odd[..]);
    // [
    //   OddSortedPackedU8(PackedU8([2, 2, 2, 2])), // [2, ignore, 2, ignore],
    //   OddSortedPackedU8(PackedU8([3, 2, 2, 3])), // [3, ignore, 2, ignore],
    //   OddSortedPackedU8(PackedU8([3, 1, 3, 1])), // [3, ignore, 3, ignore],
    // ]
}
