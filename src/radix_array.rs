const VALUES: usize = 256; // a.k.a. u8::MAX + 1

#[derive(Clone)]
pub(crate) struct RadixArray<T: Copy + Sized>([T; VALUES]);

impl<T> RadixArray<T>
where
    T: Copy + Sized,
{
    pub const fn new(initial_value: T) -> Self {
        Self([initial_value; VALUES])
    }

    #[inline]
    pub fn from_fn<F>(f: F) -> Self
    where
        F: Fn(u8) -> T,
    {
        Self(std::array::from_fn(|i| f(i as u8)))
    }

    #[inline(always)]
    pub const fn get(&self, index: u8) -> T {
        self.0[index as usize]
    }

    #[inline(always)]
    pub const fn get_mut(&mut self, index: u8) -> &mut T {
        &mut self.0[index as usize]
    }

    #[inline(always)]
    pub const fn iter(&self) -> RadixArrayIter<'_, T> {
        RadixArrayIter::new(self)
    }

    #[inline(always)]
    pub const fn inner(&self) -> &[T; VALUES] {
        &self.0
    }
}

impl<T: Copy> From<[T; VALUES]> for RadixArray<T> {
    #[inline]
    fn from(value: [T; VALUES]) -> Self {
        Self(value)
    }
}

pub struct RadixArrayIter<'radix_array, T: Copy> {
    next: Option<u8>,
    array: &'radix_array RadixArray<T>,
}

impl<'radix_array, T: Copy> RadixArrayIter<'radix_array, T> {
    pub const fn new(array: &'radix_array RadixArray<T>) -> Self {
        Self {
            next: Some(0),
            array,
        }
    }

    pub const fn enumerate(self) -> RadixArrayIterEnumerated<'radix_array, T> {
        RadixArrayIterEnumerated(self)
    }
}

impl<'radix_array, T: Copy> Iterator for RadixArrayIter<'radix_array, T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        match self.next {
            Some(u8::MAX) => {
                self.next = None;
                Some(self.array.get(u8::MAX))
            }
            Some(i) => {
                self.next = Some(i + 1);
                Some(self.array.get(i))
            }
            None => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem: usize = 256 - self.next.map(usize::from).unwrap_or(256);
        (rem, Some(rem))
    }
}

impl<'radix_array, T: Copy> ExactSizeIterator for RadixArrayIter<'radix_array, T> {}

pub struct RadixArrayIterEnumerated<'radix_array, T: Copy>(RadixArrayIter<'radix_array, T>);

impl<'radix_array, T: Copy> Iterator for RadixArrayIterEnumerated<'radix_array, T> {
    type Item = (u8, T);
    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next {
            Some(u8::MAX) => {
                self.0.next = None;
                Some((u8::MAX, self.0.array.get(u8::MAX)))
            }
            Some(i) => {
                self.0.next = Some(i + 1);
                Some((i, self.0.array.get(i)))
            }
            None => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem: usize = 256 - self.0.next.map(usize::from).unwrap_or(256);
        (rem, Some(rem))
    }
}

impl<'radix_array, T: Copy> ExactSizeIterator for RadixArrayIterEnumerated<'radix_array, T> {}
