use std::iter::Rev;
use std::mem;
use std::vec::IntoIter;

pub struct ArbitraryChunkMut<'a, T: 'a>(&'a mut [T], Rev<IntoIter<usize>>);

impl<'a, T> Iterator for ArbitraryChunkMut<'a, T> {
    type Item = &'a mut [T];

    fn next(&mut self) -> Option<Self::Item> {
        let c = self.1.next()?;
        if self.0.is_empty() {
            return None;
        }

        let slice = mem::replace(&mut self.0, &mut []);
        let (l, r) = slice.split_at_mut(c);
        self.0 = r;

        Some(l)
    }
}

pub trait ArbitraryChunks<T> {
    fn arbitrary_chunks_mut(&mut self, counts: Vec<usize>) -> ArbitraryChunkMut<T>;
}

impl<T> ArbitraryChunks<T> for [T] {
    fn arbitrary_chunks_mut(&mut self, counts: Vec<usize>) -> ArbitraryChunkMut<T> {
        ArbitraryChunkMut(self, counts.into_iter().rev())
    }
}
