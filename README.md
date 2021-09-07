# rdst

![Crates.io](https://img.shields.io/crates/l/rdst?style=flat-square)
![Crates.io](https://img.shields.io/crates/v/rdst?style=flat-square)

rdst is a flexible native Rust implementation of unstable radix sort.

## Usage

In the simplest case, you can use this sort by simply calling `my_vec.radix_sort_unstable()`. If you have a custom type to sort, you may need to implement `RadixKey` for that type.

## Default Implementations

`RadixKey` is implemented for `Vec` of the following types out-of-the-box:

 * `u8`, `u16`, `u32`, `u64`, `u128`, `usize`
 * `i8`, `i16`, `i32`, `i64`, `i128`, `isize`
 * `f32`, `f64`
 * `[u8; N]`

### Implementing `RadixKey`

To be able to sort custom types, implement `RadixKey` as below.

 * `LEVELS` should be set to the total number of bytes you will consider for each item being sorted
 * `get_level` should return the corresponding bytes from the least significant byte to the most significant byte

Notes:
 * This allows you to implement radix keys that span multiple values, or to implement radix keys that only look at part of a value.
 * You should try to make this as fast as possible, so consider using branchless implementations wherever possible

```rust
impl RadixKey for u16 {
    const LEVELS: usize = 2;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        self.to_le_bytes()[level]
    }
}
```

#### Partial `RadixKey`

If you know your type has bytes that will always be zero, you can skip those bytes to speed up the sorting process. For instance, if you have a `u32` where values never exceed `10000`, you only need to consider two of the bytes. You could implement this as such:

```rust
impl RadixKey for u32 {
    const LEVELS: usize = 2;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        (self >> (level * 8)) as u8
    }
}
```

#### Multi-value `RadixKey`

If your type has multiple values you need to search by, simply create a `RadixKey` that spans both values.

```rust
impl RadixKey for MyStruct {
    const LEVELS: usize = 4;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        match level {
          0 => self.key_1[0],
          1 => self.key_1[1],
          2 => self.key_2[0],
          3 => self.key_2[1],
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
