use crate::sorts::comparative_sort::comparative_sort;
use crate::sorts::lsb_sort::lsb_sort_adapter;
use crate::sorts::recombinating_sort::recombinating_sort_adapter;
use crate::sorts::regions_sort::regions_sort_adapter;
use crate::sorts::scanning_sort::scanning_sort_adapter;
use crate::sorts::ska_sort::ska_sort_adapter;
use crate::tuner::{
    Algorithm,
    Algorithm::{LsbSort, RecombinatingSort, RegionsSort, ScanningSort, SkaSort},
    DefaultTuner, MLTuner, Point, Tuner, TuningParams,
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
                        level: 3,
                        algorithm: ScanningSort,
                        start: 36404009,
                    },
                    Point {
                        level: 3,
                        algorithm: LsbSort,
                        start: 4118246,
                    },
                    Point {
                        level: 3,
                        algorithm: SkaSort,
                        start: 3770741,
                    },
                    Point {
                        level: 3,
                        algorithm: RecombinatingSort,
                        start: 0,
                    },
                    Point {
                        level: 3,
                        algorithm: RegionsSort,
                        start: 0,
                    },
                    Point {
                        level: 2,
                        algorithm: RecombinatingSort,
                        start: 14083510,
                    },
                    Point {
                        level: 2,
                        algorithm: ScanningSort,
                        start: 2040035,
                    },
                    Point {
                        level: 2,
                        algorithm: SkaSort,
                        start: 0,
                    },
                    Point {
                        level: 2,
                        algorithm: LsbSort,
                        start: 0,
                    },
                    Point {
                        level: 2,
                        algorithm: RegionsSort,
                        start: 0,
                    },
                    Point {
                        level: 1,
                        algorithm: SkaSort,
                        start: 21232126,
                    },
                    Point {
                        level: 1,
                        algorithm: RecombinatingSort,
                        start: 16235292,
                    },
                    Point {
                        level: 1,
                        algorithm: ScanningSort,
                        start: 12676474,
                    },
                    Point {
                        level: 1,
                        algorithm: LsbSort,
                        start: 3491346,
                    },
                    Point {
                        level: 1,
                        algorithm: RegionsSort,
                        start: 0,
                    },
                    Point {
                        level: 0,
                        algorithm: ScanningSort,
                        start: 41892246,
                    },
                    Point {
                        level: 0,
                        algorithm: SkaSort,
                        start: 13424419,
                    },
                    Point {
                        level: 0,
                        algorithm: RecombinatingSort,
                        start: 11165210,
                    },
                    Point {
                        level: 0,
                        algorithm: LsbSort,
                        start: 0,
                    },
                    Point {
                        level: 0,
                        algorithm: RegionsSort,
                        start: 0,
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
