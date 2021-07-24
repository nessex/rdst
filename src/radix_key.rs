pub trait RadixKey {
    const LEVELS: usize;

    fn get_level(&self, level: usize) -> u8;
}

impl RadixKey for u8 {
    const LEVELS: usize = 1;

    fn get_level(&self, _: usize) -> u8 {
        *self
    }
}

impl RadixKey for u16 {
    const LEVELS: usize = 2;

    fn get_level(&self, level: usize) -> u8 {
        let b = self.to_le_bytes();

        match level {
            0 => b[1],
            _ => b[0],
        }
    }
}

impl RadixKey for u32 {
    const LEVELS: usize = 4;

    fn get_level(&self, level: usize) -> u8 {
        let b = self.to_le_bytes();

        match level {
            0 => b[3],
            1 => b[2],
            2 => b[1],
            _ => b[0],
        }
    }
}

impl RadixKey for u64 {
    const LEVELS: usize = 8;

    fn get_level(&self, level: usize) -> u8 {
        let b = self.to_le_bytes();

        match level {
            0 => b[7],
            1 => b[6],
            2 => b[5],
            3 => b[4],
            4 => b[3],
            5 => b[2],
            6 => b[1],
            _ => b[0],
        }
    }
}

impl RadixKey for u128 {
    const LEVELS: usize = 16;

    fn get_level(&self, level: usize) -> u8 {
        let b = self.to_le_bytes();

        match level {
            0 => b[15],
            1 => b[14],
            2 => b[13],
            3 => b[12],
            4 => b[11],
            5 => b[10],
            6 => b[9],
            7 => b[8],
            8 => b[7],
            9 => b[6],
            10 => b[5],
            11 => b[4],
            12 => b[3],
            13 => b[2],
            14 => b[1],
            _ => b[0],
        }
    }
}

impl<const N: usize> RadixKey for [u8; N] {
    const LEVELS: usize = N;

    fn get_level(&self, level: usize) -> u8 {
        if level < N {
            self[level]
        } else {
            0
        }
    }
}
