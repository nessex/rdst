use crate::msb_ska_sort;
use crate::scanning_radix_sort;
use crate::{lsb_radix_sort_adapter, RadixKey, TuningParameters};

#[inline]
pub fn director<T>(tuning: &TuningParameters, bucket: &mut [T], level: usize, parallel: bool)
where
    T: RadixKey + Sized + Send + Copy + Sync,
{
    if bucket.len() >= tuning.ska_sort_threshold {
        msb_ska_sort(tuning, bucket, level, parallel);
    } else {
        lsb_radix_sort_adapter(bucket, 0, level, parallel);
    }
}
