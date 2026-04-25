use crate::radix_key::RadixKeyChecked;

#[cfg(feature = "multi-threaded")]
pub(crate) trait SortValue: RadixKeyChecked + Copy + Sized + Send + Sync {}
#[cfg(feature = "multi-threaded")]
impl<T> SortValue for T where T: RadixKeyChecked + Copy + Sized + Send + Sync {}

#[cfg(not(feature = "multi-threaded"))]
pub(crate) trait SortValue: RadixKeyChecked + Copy + Sized {}
#[cfg(not(feature = "multi-threaded"))]
impl<T> SortValue for T where T: RadixKeyChecked + Copy + Sized {}
