pub trait RadixKey {
    const LEVELS: usize;

    fn get_level(&self, level: usize) -> u8;

    fn get_double_level(&self, level: usize) -> u16 {
        let l = self.get_level(level) as u16;
        let r = self.get_level(level + 1) as u16;
        l << 8 | r
    }
}
