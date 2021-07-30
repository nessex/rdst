pub trait RadixKey {
    const LEVELS: usize;

    fn get_level(&self, level: usize) -> u8;
}
