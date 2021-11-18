use crate::sorts::comparative_sort::comparative_sort;
use crate::sorts::lsb_sort::lsb_sort_adapter;
use crate::sorts::recombinating_sort::recombinating_sort_adapter;
use crate::sorts::regions_sort::regions_sort_adapter;
use crate::sorts::scanning_sort::scanning_sort_adapter;
use crate::sorts::ska_sort::ska_sort_adapter;
use crate::tuner::{Algorithm, Algorithm::{ComparativeSort, LsbSort, RecombinatingSort, RegionsSort, ScanningSort, SkaSort}, MLTuner, Point, Tuner, TuningParams};
use crate::RadixKey;
use rayon::current_num_threads;
use crate::sorts::mt_lsb_sort::mt_lsb_sort_adapter;

pub struct SortManager {
    tuner: Box<dyn Tuner + Send + Sync>,
}

impl SortManager {
    fn in_place_serial_points() -> Vec<Point> {
        vec![
            Point {
                depth: 7,
                algorithm: RegionsSort,
                start: 800_000,
            },
            Point {
                depth: 7,
                algorithm: SkaSort,
                start: 50_000,
            },
            Point {
                depth: 6,
                algorithm: RegionsSort,
                start: 800_000,
            },
            Point {
                depth: 6,
                algorithm: SkaSort,
                start: 50_000,
            },
            Point {
                depth: 5,
                algorithm: RegionsSort,
                start: 800_000,
            },
            Point {
                depth: 5,
                algorithm: SkaSort,
                start: 50_000,
            },
            Point {
                depth: 4,
                algorithm: RegionsSort,
                start: 800_000,
            },
            Point {
                depth: 4,
                algorithm: SkaSort,
                start: 50_000,
            },
            Point {
                depth: 3,
                algorithm: RegionsSort,
                start: 800_000,
            },
            Point {
                depth: 3,
                algorithm: SkaSort,
                start: 50_000,
            },
            Point {
                depth: 2,
                algorithm: RegionsSort,
                start: 800_000,
            },
            Point {
                depth: 2,
                algorithm: SkaSort,
                start: 1_000_000,
            },
            Point {
                depth: 1,
                algorithm: RegionsSort,
                start: 800_000,
            },
            Point {
                depth: 1,
                algorithm: SkaSort,
                start: 50_000,
            },
            Point {
                depth: 0,
                algorithm: RegionsSort,
                start: 800_000,
            },
            Point {
                depth: 0,
                algorithm: SkaSort,
                start: 50_000,
            },
        ]
    }

    fn in_place_parallel_points() -> Vec<Point> {
        vec![
            Point {
                depth: 7,
                algorithm: RegionsSort,
                start: 800_000,
            },
            Point {
                depth: 7,
                algorithm: SkaSort,
                start: 50_000,
            },
            Point {
                depth: 6,
                algorithm: RegionsSort,
                start: 800_000,
            },
            Point {
                depth: 6,
                algorithm: SkaSort,
                start: 50_000,
            },
            Point {
                depth: 5,
                algorithm: RegionsSort,
                start: 800_000,
            },
            Point {
                depth: 5,
                algorithm: SkaSort,
                start: 50_000,
            },
            Point {
                depth: 4,
                algorithm: RegionsSort,
                start: 800_000,
            },
            Point {
                depth: 4,
                algorithm: SkaSort,
                start: 50_000,
            },
            Point {
                depth: 3,
                algorithm: RegionsSort,
                start: 800_000,
            },
            Point {
                depth: 3,
                algorithm: SkaSort,
                start: 50_000,
            },
            Point {
                depth: 2,
                algorithm: RegionsSort,
                start: 800_000,
            },
            Point {
                depth: 2,
                algorithm: SkaSort,
                start: 1_000_000,
            },
            Point {
                depth: 1,
                algorithm: RegionsSort,
                start: 800_000,
            },
            Point {
                depth: 1,
                algorithm: SkaSort,
                start: 50_000,
            },
            Point {
                depth: 0,
                algorithm: RegionsSort,
                start: 800_000,
            },
            Point {
                depth: 0,
                algorithm: SkaSort,
                start: 50_000,
            },
        ]
    }

    fn standard_serial_points() -> Vec<Point> {
        vec![
            Point {
                depth: 7,
                algorithm: ScanningSort,
                start: 50_000_000,
            },
            Point {
                depth: 7,
                algorithm: RecombinatingSort,
                start: 800_000,
            },
            Point {
                depth: 7,
                algorithm: SkaSort,
                start: 50_000,
            },
            Point {
                depth: 7,
                algorithm: LsbSort,
                start: 128,
            },
            Point {
                depth: 7,
                algorithm: ComparativeSort,
                start: 0,
            },
            Point {
                depth: 6,
                algorithm: ScanningSort,
                start: 50_000_000,
            },
            Point {
                depth: 6,
                algorithm: RecombinatingSort,
                start: 800_000,
            },
            Point {
                depth: 6,
                algorithm: SkaSort,
                start: 50_000,
            },
            Point {
                depth: 6,
                algorithm: LsbSort,
                start: 128,
            },
            Point {
                depth: 6,
                algorithm: ComparativeSort,
                start: 0,
            },
            Point {
                depth: 5,
                algorithm: ScanningSort,
                start: 50_000_000,
            },
            Point {
                depth: 5,
                algorithm: RecombinatingSort,
                start: 800_000,
            },
            Point {
                depth: 5,
                algorithm: SkaSort,
                start: 50_000,
            },
            Point {
                depth: 5,
                algorithm: LsbSort,
                start: 128,
            },
            Point {
                depth: 5,
                algorithm: ComparativeSort,
                start: 0,
            },
            Point {
                depth: 4,
                algorithm: ScanningSort,
                start: 50_000_000,
            },
            Point {
                depth: 4,
                algorithm: RecombinatingSort,
                start: 800_000,
            },
            Point {
                depth: 4,
                algorithm: SkaSort,
                start: 50_000,
            },
            Point {
                depth: 4,
                algorithm: LsbSort,
                start: 128,
            },
            Point {
                depth: 4,
                algorithm: ComparativeSort,
                start: 0,
            },
            Point {
                depth: 3,
                algorithm: ScanningSort,
                start: 50_000_000,
            },
            Point {
                depth: 3,
                algorithm: RecombinatingSort,
                start: 260_000,
            },
            Point {
                depth: 3,
                algorithm: SkaSort,
                start: 50_000,
            },
            Point {
                depth: 3,
                algorithm: LsbSort,
                start: 128,
            },
            Point {
                depth: 3,
                algorithm: ComparativeSort,
                start: 0,
            },
            Point {
                depth: 2,
                algorithm: ScanningSort,
                start: 50_000_000,
            },
            Point {
                depth: 2,
                algorithm: RecombinatingSort,
                start: 800_000,
            },
            Point {
                depth: 2,
                algorithm: SkaSort,
                start: 300_000,
            },
            Point {
                depth: 2,
                algorithm: LsbSort,
                start: 128,
            },
            Point {
                depth: 2,
                algorithm: ComparativeSort,
                start: 0,
            },
            Point {
                depth: 1,
                algorithm: ScanningSort,
                start: 50_000_000,
            },
            Point {
                depth: 1,
                algorithm: RecombinatingSort,
                start: 1_000_000,
            },
            Point {
                depth: 1,
                algorithm: SkaSort,
                start: 300_000,
            },
            Point {
                depth: 1,
                algorithm: LsbSort,
                start: 128,
            },
            Point {
                depth: 1,
                algorithm: ComparativeSort,
                start: 0,
            },
            Point {
                depth: 0,
                algorithm: ScanningSort,
                start: 50_000_000,
            },
            Point {
                depth: 0,
                algorithm: RecombinatingSort,
                start: 260_000,
            },
            Point {
                depth: 0,
                algorithm: LsbSort,
                start: 128,
            },
            Point {
                depth: 0,
                algorithm: ComparativeSort,
                start: 0,
            }
        ]
    }

    fn standard_parallel_points() -> Vec<Point> {
        vec![
            Point {
                depth: 7,
                algorithm: SkaSort,
                start: 300_000,
            },
            Point {
                depth: 6,
                algorithm: SkaSort,
                start: 300_000,
            },
            Point {
                depth: 5,
                algorithm: SkaSort,
                start: 300_000,
            },
            Point {
                depth: 4,
                algorithm: SkaSort,
                start: 300_000,
            },
            Point {
                depth: 3,
                algorithm: SkaSort,
                start: 300_000,
            },
            Point {
                depth: 2,
                algorithm: SkaSort,
                start: 300_000,
            },
            Point {
                depth: 1,
                algorithm: SkaSort,
                start: 300_000,
            },
            Point {
                depth: 0,
                algorithm: SkaSort,
                start: 300_000,
            },
        ]
    }

    pub fn new<T>() -> Self
    where
        T: RadixKey + Sized + Send + Sync + Copy,
    {
        assert_ne!(T::LEVELS, 0, "RadixKey must have at least 1 level");

        Self {
            tuner: Box::new(MLTuner {
                points_standard_serial: Self::standard_serial_points(),
                points_in_place_serial: Self::in_place_serial_points(),
                points_standard_parallel: Self::standard_parallel_points(),
                points_in_place_parallel: Self::in_place_parallel_points(),
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
            input_len: bucket_len,
            parent_len: bucket_len,
            in_place: false,
            serial: true,
        };

        if bucket_len >= 400_000 {
            mt_lsb_sort_adapter(bucket, 0, tp.level);
        } else if bucket_len >= 128 {
            lsb_sort_adapter(bucket, 0, tp.level);
        } else {
            comparative_sort(bucket, tp.level);
        }
        //
        // match self.tuner.pick_algorithm(&tp) {
        //     Algorithm::ScanningSort => {
        //         scanning_sort_adapter(&*self.tuner, tp.in_place, bucket, tp.level)
        //     }
        //     Algorithm::RecombinatingSort => {
        //         recombinating_sort_adapter(&*self.tuner, tp.in_place, bucket, tp.level)
        //     }
        //     Algorithm::LsbSort => lsb_sort_adapter(bucket, 0, tp.level),
        //     Algorithm::SkaSort => ska_sort_adapter(&*self.tuner, tp.in_place, bucket, tp.level),
        //     Algorithm::ComparativeSort => comparative_sort(bucket, tp.level),
        //     Algorithm::RegionsSort => {
        //         regions_sort_adapter(&*self.tuner, tp.in_place, bucket, tp.level)
        //     }
        // };
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
