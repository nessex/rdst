# rdst

![Crates.io](https://img.shields.io/crates/l/rdst?style=flat-square)
![Crates.io](https://img.shields.io/crates/v/rdst?style=flat-square)

rdst is a flexible native Rust implementation of unstable radix sort.

## Usage

In the simplest case, you can use this sort by simply calling `my_vec.radix_sort_unstable()`. If you have a custom type to sort, you may need to implement `RadixKey` for that type.

## Default Implementations

`RadixKey` is implemented for `Vec` of the following types out-of-the-box:

 * `u8`
 * `u16`
 * `u32`
 * `u64`
 * `u128`
 * `[u8; N]`

### Implementing `RadixKey`

To be able to sort custom types, implement `RadixKey` as below.

 * `LEVELS` should be set to the total number of bytes you will consider for each item being sorted
 * `get_level` should return the corresponding bytes in the order you would like them to be sorted. This library is intended to be used starting from the MSB (most significant bits).

Note that this also allows you to implement radix keys that span multiple values.

```rust
impl RadixKey for u16 {
    const LEVELS: usize = 2;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        let b = self.to_le_bytes();

        match level {
            0 => b[1],
            _ => b[0],
        }
    }
}
```

## License

Licensed under either of

* Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
