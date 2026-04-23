const VALUES: usize = 256; // a.k.a. u8::MAX + 1

#[derive(Clone)]
pub(crate) struct RadixArray<T: Copy + Sized>([T; VALUES]);

impl<T> RadixArray<T>
where
    T: Copy + Sized,
{
    pub fn new(initial_value: T) -> Self {
        Self([initial_value; VALUES])
    }

    pub fn get(&self, index: u8) -> T {
        unsafe {
            // SAFETY: every valid u8 is a valid index
            // into this 256 value array.
            *self.0.get_unchecked(index as usize)
        }
    }

    pub fn get_mut(&mut self, index: u8) -> &mut T {
        unsafe {
            // SAFETY: every valid u8 is a valid index
            // into this 256 value array.
            self.0.get_unchecked_mut(index as usize)
        }
    }

    pub fn iter(&self) -> RadixArrayIter<'_, T> {
        RadixArrayIter::new(self)
    }

    pub fn inner(&self) -> &[T; VALUES] {
        &self.0
    }

    pub fn inner_mut(&mut self) -> &mut [T; VALUES] {
        &mut self.0
    }
}

pub struct RadixArrayIter<'radix_array, T: Copy> {
    next: Option<u8>,
    array: &'radix_array RadixArray<T>,
}

impl<'radix_array, T: Copy> RadixArrayIter<'radix_array, T> {
    pub fn new(array: &'radix_array RadixArray<T>) -> Self {
        Self {
            next: Some(0),
            array,
        }
    }

    pub fn enumerate(self) -> RadixArrayIterEnumerated<'radix_array, T> {
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
            Some(i @ _) => {
                self.next = Some(i + 1);
                Some(self.array.get(i))
            }
            None => None,
        }
    }
}

pub struct RadixArrayIterEnumerated<'radix_array, T: Copy>(RadixArrayIter<'radix_array, T>);

impl<'radix_array, T: Copy> Iterator for RadixArrayIterEnumerated<'radix_array, T> {
    type Item = (u8, T);
    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next {
            Some(u8::MAX) => {
                self.0.next = None;
                Some((u8::MAX, self.0.array.get(u8::MAX)))
            }
            Some(i @ _) => {
                self.0.next = Some(i + 1);
                Some((i, self.0.array.get(i)))
            }
            None => None,
        }
    }
}
