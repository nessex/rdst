pub trait RadixKey {
    const LEVELS: usize;

    fn get_level(&self, level: usize) -> u8;
}

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
        ((self >> ((Self::LEVELS - 1 - level) * 8)) as u8) & 0xff
    }
}

impl RadixKey for u32 {
    const LEVELS: usize = 4;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        ((self >> ((Self::LEVELS - 1 - level) * 8)) as u8) & 0xff
    }
}

impl RadixKey for u64 {
    const LEVELS: usize = 8;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        ((self >> ((Self::LEVELS - 1 - level) * 8)) as u8) & 0xff
    }
}

impl RadixKey for u128 {
    const LEVELS: usize = 16;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        ((self >> ((Self::LEVELS - 1 - level) * 8)) as u8) & 0xff
    }
}

impl<const N: usize> RadixKey for [u8; N] {
    const LEVELS: usize = N;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        self[level]
    }
}
