use crate::sorts::comparative_sort::comparative_sort;
use crate::sorts::lsb_sort::lsb_sort_adapter;
use crate::sorts::recombinating_sort::recombinating_sort_adapter;
use crate::sorts::regions_sort::regions_sort_adapter;
use crate::sorts::scanning_sort::scanning_sort_adapter;
use crate::sorts::ska_sort::ska_sort_adapter;
use crate::tuner::{
    Algorithm,
    Algorithm::{LsbSort, RecombinatingSort, RegionsSort, ScanningSort, SkaSort},
    MLTuner, Point, Tuner, TuningParams,
};
use crate::RadixKey;
use rayon::current_num_threads;

pub struct SortManager {
    tuner: Box<dyn Tuner + Send + Sync>,
}

impl SortManager {
    pub fn new<T>() -> Self
    where
        T: RadixKey + Sized + Send + Sync + Copy,
    {
        assert_ne!(T::LEVELS, 0, "RadixKey must have at least 1 level");

        Self {
            tuner: Box::new(MLTuner {
                points: vec![
                    Point {
                        depth: 7,
                        algorithm: RecombinatingSort,
                        start: 26875000,
                    },
                    Point {
                        depth: 7,
                        algorithm: RegionsSort,
                        start: 8437500,
                    },
                    Point {
                        depth: 7,
                        algorithm: SkaSort,
                        start: 5312500,
                    },
                    Point {
                        depth: 7,
                        algorithm: ScanningSort,
                        start: 312500,
                    },
                    Point {
                        depth: 6,
                        algorithm: SkaSort,
                        start: 33125000,
                    },
                    Point {
                        depth: 6,
                        algorithm: RegionsSort,
                        start: 14062500,
                    },
                    Point {
                        depth: 6,
                        algorithm: ScanningSort,
                        start: 14062500,
                    },
                    Point {
                        depth: 6,
                        algorithm: RecombinatingSort,
                        start: 12812500,
                    },
                    Point {
                        depth: 5,
                        algorithm: SkaSort,
                        start: 25937500,
                    },
                    Point {
                        depth: 5,
                        algorithm: RegionsSort,
                        start: 24062500,
                    },
                    Point {
                        depth: 5,
                        algorithm: ScanningSort,
                        start: 9687500,
                    },
                    Point {
                        depth: 5,
                        algorithm: RecombinatingSort,
                        start: 312500,
                    },
                    Point {
                        depth: 4,
                        algorithm: RecombinatingSort,
                        start: 27187500,
                    },
                    Point {
                        depth: 4,
                        algorithm: RegionsSort,
                        start: 26875000,
                    },
                    Point {
                        depth: 4,
                        algorithm: SkaSort,
                        start: 25625000,
                    },
                    Point {
                        depth: 4,
                        algorithm: ScanningSort,
                        start: 1250000,
                    },
                    Point {
                        depth: 3,
                        algorithm: RecombinatingSort,
                        start: 33125000,
                    },
                    Point {
                        depth: 3,
                        algorithm: RegionsSort,
                        start: 15625000,
                    },
                    Point {
                        depth: 3,
                        algorithm: ScanningSort,
                        start: 9687500,
                    },
                    Point {
                        depth: 3,
                        algorithm: SkaSort,
                        start: 1562500,
                    },
                    Point {
                        depth: 2,
                        algorithm: RegionsSort,
                        start: 10625000,
                    },
                    Point {
                        depth: 2,
                        algorithm: SkaSort,
                        start: 9687500,
                    },
                    Point {
                        depth: 2,
                        algorithm: ScanningSort,
                        start: 7187500,
                    },
                    Point {
                        depth: 2,
                        algorithm: RecombinatingSort,
                        start: 2187500,
                    },
                    Point {
                        depth: 1,
                        algorithm: ScanningSort,
                        start: 20625000,
                    },
                    Point {
                        depth: 1,
                        algorithm: RegionsSort,
                        start: 14375000,
                    },
                    Point {
                        depth: 1,
                        algorithm: RecombinatingSort,
                        start: 2812500,
                    },
                    Point {
                        depth: 1,
                        algorithm: SkaSort,
                        start: 0,
                    },
                    Point {
                        depth: 0,
                        algorithm: SkaSort,
                        start: 38125000,
                    },
                    Point {
                        depth: 0,
                        algorithm: ScanningSort,
                        start: 25625000,
                    },
                    Point {
                        depth: 0,
                        algorithm: RegionsSort,
                        start: 6562500,
                    },
                    Point {
                        depth: 0,
                        algorithm: RecombinatingSort,
                        start: 5937500,
                    },
                ],
            }),
        }
    }

    #[cfg(feature = "tuning")]
    pub fn new_with_tuning<T>(tuner: Box<dyn Tuner + Send + Sync>) -> Self
    where
        T: RadixKey + Sized + Send + Sync + Copy,
    {
        assert_ne!(T::LEVELS, 0, "RadixKey must have at least 1 level");

        Self { tuner }
    }

    pub fn sort<T>(&self, bucket: &mut [T])
    where
        T: RadixKey + Sized + Send + Sync + Copy,
    {
        let bucket_len = bucket.len();

        if bucket_len <= 1 {
            return;
        }

        let tp = TuningParams {
            threads: current_num_threads(),
            level: T::LEVELS - 1,
            total_levels: T::LEVELS,
            input_len: bucket.len(),
            parent_len: bucket.len(),
            in_place: false,
            serial: true,
        };

        match self.tuner.pick_algorithm(&tp) {
            Algorithm::ScanningSort => {
                scanning_sort_adapter(&*self.tuner, tp.in_place, bucket, tp.level)
            }
            Algorithm::RecombinatingSort => {
                recombinating_sort_adapter(&*self.tuner, tp.in_place, bucket, tp.level)
            }
            Algorithm::LsbSort => lsb_sort_adapter(bucket, 0, tp.level),
            Algorithm::SkaSort => ska_sort_adapter(&*self.tuner, tp.in_place, bucket, tp.level),
            Algorithm::ComparativeSort => comparative_sort(bucket, tp.level),
            Algorithm::RegionsSort => {
                regions_sort_adapter(&*self.tuner, tp.in_place, bucket, tp.level)
            }
        };
    }

    pub fn sort_in_place<T>(&self, bucket: &mut [T])
    where
        T: RadixKey + Sized + Send + Sync + Copy,
    {
        let bucket_len = bucket.len();

        if bucket_len <= 1 {
            return;
        }

        let tp = TuningParams {
            threads: current_num_threads(),
            level: T::LEVELS - 1,
            total_levels: T::LEVELS,
            input_len: bucket.len(),
            parent_len: bucket.len(),
            in_place: true,
            serial: true,
        };

        match self.tuner.pick_algorithm(&tp) {
            Algorithm::ScanningSort => {
                scanning_sort_adapter(&*self.tuner, tp.in_place, bucket, tp.level)
            }
            Algorithm::RecombinatingSort => {
                recombinating_sort_adapter(&*self.tuner, tp.in_place, bucket, tp.level)
            }
            Algorithm::LsbSort => lsb_sort_adapter(bucket, 0, tp.level),
            Algorithm::SkaSort => ska_sort_adapter(&*self.tuner, tp.in_place, bucket, tp.level),
            Algorithm::ComparativeSort => comparative_sort(bucket, tp.level),
            Algorithm::RegionsSort => {
                regions_sort_adapter(&*self.tuner, tp.in_place, bucket, tp.level)
            }
        };
    }
}
