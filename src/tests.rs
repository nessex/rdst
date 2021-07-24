use crate::{RadixKey, RadixSort};
use rand::{thread_rng, RngCore};

#[derive(Debug, Eq, PartialEq, Clone, Copy, Ord, PartialOrd)]
struct TestLevel1 {
    key: u8,
}

impl RadixKey for TestLevel1 {
    const LEVELS: usize = 1;

    #[inline]
    fn get_level(&self, _level: usize) -> u8 {
        self.key
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Ord, PartialOrd)]
struct TestLevel4 {
    key: u32,
}

impl RadixKey for TestLevel4 {
    const LEVELS: usize = 4;

    #[inline]
    fn get_level(&self, level: usize) -> u8 {
        let b = self.key.to_le_bytes();

        match level {
            0 => b[3],
            1 => b[2],
            2 => b[1],
            _ => b[0],
        }
    }
}

#[test]
pub fn test_1_level() {
    let mut inputs = vec![
        TestLevel1 { key: 5 },
        TestLevel1 { key: 2 },
        TestLevel1 { key: 7 },
        TestLevel1 { key: 3 },
    ];

    RadixSort::sort(&mut inputs);

    assert_eq!(
        inputs,
        vec![
            TestLevel1 { key: 2 },
            TestLevel1 { key: 3 },
            TestLevel1 { key: 5 },
            TestLevel1 { key: 7 },
        ]
    );
}

#[test]
pub fn test_4_level() {
    let mut inputs = vec![
        TestLevel4 { key: 4294967295 },
        TestLevel4 { key: 4294967294 },
        TestLevel4 { key: 543 },
        TestLevel4 { key: 544 },
        TestLevel4 { key: 0 },
    ];

    RadixSort::sort(&mut inputs);

    assert_eq!(
        inputs,
        vec![
            TestLevel4 { key: 0 },
            TestLevel4 { key: 543 },
            TestLevel4 { key: 544 },
            TestLevel4 { key: 4294967294 },
            TestLevel4 { key: 4294967295 },
        ]
    );
}

#[test]
pub fn test_random_4_level() {
    let mut inputs = Vec::new();
    let mut rng = thread_rng();

    for _ in 0..2000 {
        inputs.push(TestLevel4 {
            key: rng.next_u32(),
        })
    }

    let mut inputs_clone = inputs[..].to_vec();

    RadixSort::sort(&mut inputs);
    inputs_clone.sort_by_key(|i| i.key);

    assert_eq!(inputs, inputs_clone);
}

#[test]
pub fn test_series_4_level() {
    let mut inputs = Vec::new();

    for i in 0..500000 {
        inputs.push(TestLevel4 { key: i })
    }

    let mut inputs_clone = inputs[..].to_vec();

    RadixSort::sort(&mut inputs);
    inputs_clone.sort_by_key(|i| i.key);

    assert_eq!(inputs, inputs_clone);
}
