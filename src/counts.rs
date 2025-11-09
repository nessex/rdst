use std::cell::RefCell;

use std::ops::{Index, IndexMut};
use std::ptr::copy_nonoverlapping;

use crate::radix_key::RadixKeyChecked;
use std::rc::Rc;
use std::slice::{Iter, SliceIndex};

#[derive(Default)]
pub(crate) struct CountManager {}

#[repr(C, align(4096))]
#[derive(Clone)]
pub(crate) struct Counter([usize; 256 * 4]);

impl Default for Counter {
    fn default() -> Self {
        Counter([0usize; 256 * 4])
    }
}

#[repr(C, align(2048))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Counts([usize; 256]);
pub type PrefixSums = Counts;
pub type EndOffsets = Counts;

impl<I> Index<I> for Counts
where
    I: SliceIndex<[usize]>,
{
    type Output = I::Output;

    #[inline(always)]
    fn index(&self, index: I) -> &I::Output {
        &self.0[index]
    }
}

impl<I> IndexMut<I> for Counts
where
    I: SliceIndex<[usize]>,
{
    #[inline(always)]
    fn index_mut(&mut self, index: I) -> &mut I::Output {
        &mut self.0[index]
    }
}

impl Default for Counts {
    fn default() -> Self {
        Counts([0usize; 256])
    }
}

#[derive(Default, Clone, Copy)]
pub(crate) struct CountMeta {
    pub first: u8,
    pub last: u8,
    pub already_sorted: bool,
}

#[derive(Default)]
struct ThreadContext {
    pub counter: RefCell<Counter>,
    pub counts: RefCell<Vec<Rc<RefCell<Counts>>>>,
    pub tmp: RefCell<Vec<u8>>,
}

impl CountManager {
    thread_local! {
        static THREAD_CTX: ThreadContext = Default::default();
    }

    #[inline(never)]
    pub fn get_empty_counts(&self) -> Rc<RefCell<Counts>> {
        Self::THREAD_CTX.with(|ct| ct.counts.borrow_mut().pop().unwrap_or_default())
    }

    #[inline(never)]
    pub fn return_counts(&self, counts: Rc<RefCell<Counts>>) {
        counts.borrow_mut().clear();
        Self::THREAD_CTX.with(|ct| ct.counts.borrow_mut().push(counts));
    }

    pub fn count_into<T: RadixKeyChecked>(
        &self,
        counts: &mut Counts,
        meta: &mut CountMeta,
        bucket: &[T],
        level: usize,
    ) {
        Self::THREAD_CTX.with(|ct| {
            ct.counter
                .borrow_mut()
                .count_into(counts, meta, bucket, level)
        })
    }

    #[inline(always)]
    pub fn counts<T: RadixKeyChecked>(
        &self,
        bucket: &[T],
        level: usize,
    ) -> (Rc<RefCell<Counts>>, bool) {
        let counts = self.get_empty_counts();
        let mut meta = CountMeta::default();
        Self::THREAD_CTX.with(|ct| {
            ct.counter
                .borrow_mut()
                .count_into(&mut counts.borrow_mut(), &mut meta, bucket, level)
        });

        (counts, meta.already_sorted)
    }

    #[inline(always)]
    pub fn prefix_sums(&self, counts: &Counts) -> Rc<RefCell<PrefixSums>> {
        let sums = self.get_empty_counts();
        let mut s = sums.borrow_mut();

        let mut running_total = 0;
        for (i, c) in counts.into_iter().enumerate() {
            s[i] = running_total;
            running_total += c;
        }
        drop(s);

        sums
    }

    #[inline(always)]
    pub fn end_offsets(
        &self,
        counts: &Counts,
        prefix_sums: &PrefixSums,
    ) -> Rc<RefCell<EndOffsets>> {
        let end_offsets = self.get_empty_counts();
        let mut eo = end_offsets.borrow_mut();

        eo[0..255].copy_from_slice(&prefix_sums[1..256]);
        eo[255] = counts[255] + prefix_sums[255];
        drop(eo);

        end_offsets
    }

    #[inline(always)]
    pub fn with_tmp_buffer<T, F>(&self, src_bucket: &mut [T], mut f: F)
    where
        T: Copy,
        F: FnMut(&CountManager, &mut [T], &mut [T]),
    {
        Self::THREAD_CTX.with(|ct| {
            let byte_len = size_of_val(src_bucket);
            let thread_tmp = ct.tmp.try_borrow_mut();
            let one_off_tmp: RefCell<Vec<u8>>;

            let mut t = match thread_tmp {
                Ok(mut t) => {
                    if t.len() < byte_len {
                        *t = Vec::with_capacity(byte_len);
                    }

                    t
                }
                Err(_) => {
                    one_off_tmp = RefCell::new(Vec::with_capacity(byte_len));
                    one_off_tmp.borrow_mut()
                }
            };

            // Safety: The buffer is guaranteed to have enough capacity by the logic above.
            // As the data is copied from the source buffer to the temporary buffer, and
            // T is Copy, the data is therefore correctly initialized (assuming the source itself is).
            // Len is set to 0 until the end to ensure that the compiler doesn't assume the buffer
            // is fully initialized before that point.
            let tmp = unsafe {
                t.set_len(0);
                let ptr = t.as_mut_ptr() as *mut T;
                copy_nonoverlapping(src_bucket.as_ptr(), ptr, src_bucket.len());
                t.set_len(byte_len);
                std::slice::from_raw_parts_mut(ptr, src_bucket.len())
            };

            f(self, src_bucket, tmp);
        });
    }
}

impl Counter {
    #[inline(always)]
    fn clear(&mut self) {
        self.0.fill(0)
    }

    #[inline(always)]
    pub fn count_into<T: RadixKeyChecked>(
        &mut self,
        counts: &mut Counts,
        meta: &mut CountMeta,
        bucket: &[T],
        level: usize,
    ) {
        #[cfg(feature = "work_profiles")]
        println!("({}) COUNT", level);

        self.clear();
        meta.already_sorted = true;

        if bucket.is_empty() {
            return;
        } else if bucket.len() == 1 {
            let b = bucket[0].get_level_checked(level) as usize;
            counts[b] = 1;

            meta.first = b as u8;
            meta.last = b as u8;
            return;
        }

        meta.first = unsafe { bucket.get_unchecked(0).get_level_checked(level) };
        meta.last = unsafe {
            bucket
                .get_unchecked(bucket.len() - 1)
                .get_level_checked(level)
        };

        let mut continue_from = 0;
        let mut prev = 0usize;

        // First, count directly into the output buffer until we find a value that is out of order.
        for item in bucket {
            let b = item.get_level_checked(level) as usize;
            unsafe { *self.0.get_unchecked_mut(b * 4) += 1 }

            continue_from += 1;

            if b < prev {
                meta.already_sorted = false;
                break;
            }

            prev = b;
        }

        if continue_from == bucket.len() {
            for i in 0..256 {
                counts[i] = unsafe { *self.0.get_unchecked_mut(i * 4) }
            }
            return;
        }

        let chunks = bucket[continue_from..].chunks_exact(4);
        let rem = chunks.remainder();

        chunks.for_each(|chunk| unsafe {
            let a = chunk.get_unchecked(0).get_level_checked(level) as usize * 4;
            let b = chunk.get_unchecked(1).get_level_checked(level) as usize * 4 + 1;
            let c = chunk.get_unchecked(2).get_level_checked(level) as usize * 4 + 2;
            let d = chunk.get_unchecked(3).get_level_checked(level) as usize * 4 + 3;

            debug_assert!(a < 1024);
            debug_assert!(b < 1024);
            debug_assert!(c < 1024);
            debug_assert!(d < 1024);

            *self.0.get_unchecked_mut(a) += 1;
            *self.0.get_unchecked_mut(b) += 1;
            *self.0.get_unchecked_mut(c) += 1;
            *self.0.get_unchecked_mut(d) += 1;
        });

        rem.iter().for_each(|v| unsafe {
            let b = v.get_level_checked(level) as usize * 4;
            *self.0.get_unchecked_mut(b) += 1;
        });

        for i in 0..256 {
            let a = i * 4;

            unsafe {
                *counts.0.get_unchecked_mut(i) = *self.0.get_unchecked(a)
                    + *self.0.get_unchecked(a + 1)
                    + *self.0.get_unchecked(a + 2)
                    + *self.0.get_unchecked(a + 3);
            }
        }
    }
}

impl Counts {
    #[inline(always)]
    pub fn clear(&mut self) {
        self.0.fill(0);
    }

    #[inline]
    pub fn inner(&self) -> &[usize; 256] {
        &self.0
    }
}

impl IntoIterator for Counts {
    type Item = usize;
    type IntoIter = core::array::IntoIter<usize, 256>;

    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        IntoIterator::into_iter(self.0)
    }
}

impl<'a> IntoIterator for &'a Counts {
    type Item = &'a usize;
    type IntoIter = Iter<'a, usize>;

    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_counting() {
        let count_manager = CountManager::default();

        let data: [u16; 5] = [0x0000, 0x0101, 0x0200, 0x0200, 0xFFFF];
        let counts_lower = count_manager.counts(&data, 0);
        let counts_upper = count_manager.counts(&data, 1);
        let mut expected_lower = Counts::default();
        let mut expected_upper = Counts::default();
        expected_lower[0] = 3;
        expected_lower[1] = 1;
        expected_lower[255] = 1;

        expected_upper[0] = 1;
        expected_upper[1] = 1;
        expected_upper[2] = 2;
        expected_upper[255] = 1;

        assert_eq!(counts_lower.0.take(), expected_lower);
        assert_eq!(counts_upper.0.take(), expected_upper);
    }

    #[test]
    pub fn test_reuse() {
        let count_manager = CountManager::default();

        let data_1: [u16; 5] = [0x0000, 0x0101, 0x0200, 0x0200, 0xFFFF];
        let data_2: [u16; 5] = [0x0101, 0x0202, 0x0301, 0x0301, 0x0000];
        let counts_1 = count_manager.counts(&data_1, 0);
        let counts_2 = count_manager.counts(&data_2, 0);
        let mut expected_1 = Counts::default();
        let mut expected_2 = Counts::default();
        expected_1[0] = 3;
        expected_1[1] = 1;
        expected_1[255] = 1;

        expected_2[0] = 1;
        expected_2[1] = 3;
        expected_2[2] = 1;

        assert_eq!(counts_1.0.take(), expected_1);
        assert_eq!(counts_2.0.take(), expected_2);
    }
}
