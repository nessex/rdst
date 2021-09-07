use crate::RadixKey;

impl RadixKey for u8 {
    const LEVELS: usize = 1;

    #[inline]
    fn get_level(&self, _: usize) -> u8 {
        *self
    }
}

impl RadixKey for u16 {
    const LEVELS: usize = 2;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        (self >> (level * 8)) as u8
    }
}

impl RadixKey for u32 {
    const LEVELS: usize = 4;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        (self >> (level * 8)) as u8
    }
}

impl RadixKey for u64 {
    const LEVELS: usize = 8;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        (self >> (level * 8)) as u8
    }
}

impl RadixKey for u128 {
    const LEVELS: usize = 16;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        (self >> (level * 8)) as u8
    }
}

#[cfg(target_pointer_width = "16")]
impl RadixKey for usize {
    const LEVELS: usize = 2;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        (self >> (level * 8)) as u8
    }
}

#[cfg(target_pointer_width = "32")]
impl RadixKey for usize {
    const LEVELS: usize = 4;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        (self >> (level * 8)) as u8
    }
}

#[cfg(target_pointer_width = "64")]
impl RadixKey for usize {
    const LEVELS: usize = 8;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        (self >> (level * 8)) as u8
    }
}

impl<const N: usize> RadixKey for [u8; N] {
    const LEVELS: usize = N;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        self[level]
    }
}

impl RadixKey for i8 {
    const LEVELS: usize = 1;

    #[inline]
    fn get_level(&self, _: usize) -> u8 {
        (*self ^ i8::MIN) as u8
    }
}

impl RadixKey for i16 {
    const LEVELS: usize = 2;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        ((self ^ i16::MIN) >> (level * 8)) as u8
    }
}

impl RadixKey for i32 {
    const LEVELS: usize = 4;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        ((self ^ i32::MIN) >> (level * 8)) as u8
    }
}

impl RadixKey for i64 {
    const LEVELS: usize = 8;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        ((self ^ i64::MIN) >> (level * 8)) as u8
    }
}

impl RadixKey for i128 {
    const LEVELS: usize = 16;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        ((self ^ i128::MIN) >> (level * 8)) as u8
    }
}

#[cfg(target_pointer_width = "16")]
impl RadixKey for isize {
    const LEVELS: usize = 2;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        ((self ^ isize::MIN) >> (level * 8)) as u8
    }
}

#[cfg(target_pointer_width = "32")]
impl RadixKey for isize {
    const LEVELS: usize = 4;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        ((self ^ isize::MIN) >> (level * 8)) as u8
    }
}

#[cfg(target_pointer_width = "64")]
impl RadixKey for isize {
    const LEVELS: usize = 8;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        ((self ^ isize::MIN) >> (level * 8)) as u8
    }
}
