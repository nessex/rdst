use crate::sorts::comparative_sort::comparative_sort;
use crate::sorts::lsb_sort::lsb_sort_adapter;
use crate::sorts::recombinating_sort::recombinating_sort_adapter;
use crate::sorts::regions_sort::regions_sort_adapter;
use crate::sorts::scanning_sort::scanning_sort_adapter;
use crate::sorts::ska_sort::ska_sort_adapter;
use crate::tuner::{Algorithm, DefaultTuner, MLTuner, Point, Tuner, TuningParams};
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
                        algorithm: Algorithm::SkaSort,
                        start: 2257303,
                    },
                    Point {
                        level: 3,
                        algorithm: Algorithm::ScanningSort,
                        start: 1646704,
                    },
                    Point {
                        level: 3,
                        algorithm: Algorithm::RecombinatingSort,
                        start: 544756,
                    },
                    Point {
                        level: 3,
                        algorithm: Algorithm::RegionsSort,
                        start: 216495,
                    },
                    Point {
                        level: 3,
                        algorithm: Algorithm::LsbSort,
                        start: 0,
                    },
                    Point {
                        level: 2,
                        algorithm: Algorithm::SkaSort,
                        start: 3284072,
                    },
                    Point {
                        level: 2,
                        algorithm: Algorithm::ScanningSort,
                        start: 500810,
                    },
                    Point {
                        level: 2,
                        algorithm: Algorithm::RegionsSort,
                        start: 410166,
                    },
                    Point {
                        level: 2,
                        algorithm: Algorithm::LsbSort,
                        start: 0,
                    },
                    Point {
                        level: 2,
                        algorithm: Algorithm::RecombinatingSort,
                        start: 0,
                    },
                    Point {
                        level: 1,
                        algorithm: Algorithm::LsbSort,
                        start: 7620565,
                    },
                    Point {
                        level: 1,
                        algorithm: Algorithm::ScanningSort,
                        start: 3830901,
                    },
                    Point {
                        level: 1,
                        algorithm: Algorithm::RegionsSort,
                        start: 3024038,
                    },
                    Point {
                        level: 1,
                        algorithm: Algorithm::SkaSort,
                        start: 692593,
                    },
                    Point {
                        level: 1,
                        algorithm: Algorithm::RecombinatingSort,
                        start: 172735,
                    },
                    Point {
                        level: 0,
                        algorithm: Algorithm::RegionsSort,
                        start: 1771548,
                    },
                    Point {
                        level: 0,
                        algorithm: Algorithm::SkaSort,
                        start: 1226143,
                    },
                    Point {
                        level: 0,
                        algorithm: Algorithm::ScanningSort,
                        start: 680218,
                    },
                    Point {
                        level: 0,
                        algorithm: Algorithm::LsbSort,
                        start: 0,
                    },
                    Point {
                        level: 0,
                        algorithm: Algorithm::RecombinatingSort,
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
