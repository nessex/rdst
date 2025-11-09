pub trait RadixKey {
    const LEVELS: usize;

    fn get_level(&self, level: usize) -> u8;
}

pub(crate) trait RadixKeyChecked {
    const LEVELS: usize;

    fn get_level_checked(&self, level: usize) -> u8;
}

impl<T: RadixKey> RadixKeyChecked for T {
    const LEVELS: usize = T::LEVELS;

    #[inline(always)]
    fn get_level_checked(&self, level: usize) -> u8 {
        debug_assert!(level < Self::LEVELS);
        self.get_level(level)
    }
}
