use block_pseudorand::block_rand;
use lazy_static::lazy_static;
use oxigen::{
    AgeFunctions, AgeSlope, AgeThreshold, GeneticExecution, Genotype, MutationRates,
    SelectionFunctions, SelectionRates, SlopeParams, StopCriteria,
};
use rand::prelude::*;
use rayon::prelude::*;
use rdst::tuner::Algorithm::{RecombinatingSort, RegionsSort, ScanningSort, SkaSort};
use rdst::tuner::{Algorithm, Tuner, TuningParams};
use rdst::RadixSort;
use rlp_iter::RlpIterator;
use std::collections::hash_map::DefaultHasher;
use std::fmt::{Debug, Display, Formatter};
use std::fs::File;
use std::hash::Hasher;
use std::ops::{Shr, ShrAssign, Sub};
use std::slice::Iter;
use std::time::Instant;
use std::vec::IntoIter;

static N: usize = 200_000_000;
lazy_static! {
    static ref DATA_U32: Vec<u32> = gen_inputs(N, 0u32);
    static ref DATA_U32_BIMODAL: Vec<u32> = gen_inputs(N, 16u32);
    static ref DATA_U64: Vec<u64> = gen_inputs(N, 0u64);
    static ref DATA_U64_BIMODAL: Vec<u64> = gen_inputs(N, 32u64);
    static ref ITER: Vec<usize> = (0..=N).rlp_iter().collect();
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct Point {
    depth: usize,
    algorithm: Algorithm,
    start: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MLTuner {
    points: Vec<Point>,
}

#[derive(Debug, Clone)]
struct GeneticSort {
    tuner: MLTuner,
    intervals: Vec<f64>,
}

impl MLTuner {
    fn new(points: Vec<Point>) -> Self {
        Self {
            points: points.into_iter().filter(|v| v.start != 0).collect(),
        }
    }
}

impl Display for GeneticSort {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("{:?}", self.tuner.points))
    }
}

impl Tuner for MLTuner {
    fn pick_algorithm(&self, p: &TuningParams) -> Algorithm {
        let depth = p.total_levels - 1 - p.level;
        for point in self.points.iter() {
            if depth == point.depth && p.input_len >= point.start {
                return point.algorithm;
            }
        }

        return Algorithm::LsbSort;
    }
}

impl Genotype<f64> for GeneticSort {
    type ProblemSize = usize;
    type GenotypeHash = u64;

    fn iter(&self) -> Iter<f64> {
        self.intervals.iter()
    }

    fn into_iter(self) -> IntoIter<f64> {
        self.intervals.into_iter()
    }

    fn from_iter<I: Iterator<Item = f64>>(&mut self, iter: I) {
        self.intervals = iter.collect();
    }

    fn generate(_size: &Self::ProblemSize) -> Self {
        let points = get_nodes();
        let intervals = points.iter().map(|v| v.start as f64).collect();
        Self {
            tuner: MLTuner::new(points),
            intervals,
        }
    }

    fn fitness(&self) -> f64 {
        (1_000_000_000_000_000u64 - (fitness(self.tuner.clone()) as u64)) as f64
    }

    fn mutate(&mut self, rgen: &mut SmallRng, index: usize) {
        let mut last = None;
        let skip = rgen.gen_range(-5, 20);
        let mut last_idx = 0;
        for (i, v) in ITER.iter().enumerate() {
            if let Some(last) = last {
                if self.intervals[index].sub(last as f64).abs() < 0.5 {
                    last_idx = i as i64;

                    break;
                }
            }

            last = Some(*v);
        }

        self.intervals[index] = ITER[last_idx.saturating_add(skip) as usize] as f64;

        let mut nodes = get_nodes();
        for (node, interval) in nodes.iter_mut().zip(self.intervals.iter()) {
            node.start = *interval as usize;
        }

        sort_nodes(&mut nodes);
        self.tuner.points = nodes;
    }

    fn is_solution(&self, _fitness: f64) -> bool {
        false
    }

    fn hash(&self) -> Self::GenotypeHash {
        let mut hasher = DefaultHasher::new();
        self.intervals
            .iter()
            .map(|v| *v as usize)
            .for_each(|v| hasher.write_usize(v));

        hasher.finish()
    }
}

fn get_nodes() -> Vec<Point> {
    let out = vec![
        Point {
            depth: 7,
            algorithm: RecombinatingSort,
            start: 0,
        },
        Point {
            depth: 7,
            algorithm: RegionsSort,
            start: 0,
        },
        Point {
            depth: 7,
            algorithm: SkaSort,
            start: 0,
        },
        Point {
            depth: 7,
            algorithm: ScanningSort,
            start: 0,
        },
        Point {
            depth: 6,
            algorithm: SkaSort,
            start: 0,
        },
        Point {
            depth: 6,
            algorithm: RegionsSort,
            start: 0,
        },
        Point {
            depth: 6,
            algorithm: ScanningSort,
            start: 0,
        },
        Point {
            depth: 6,
            algorithm: RecombinatingSort,
            start: 0,
        },
        Point {
            depth: 5,
            algorithm: SkaSort,
            start: 0,
        },
        Point {
            depth: 5,
            algorithm: RegionsSort,
            start: 0,
        },
        Point {
            depth: 5,
            algorithm: ScanningSort,
            start: 0,
        },
        Point {
            depth: 5,
            algorithm: RecombinatingSort,
            start: 0,
        },
        Point {
            depth: 4,
            algorithm: RecombinatingSort,
            start: 0,
        },
        Point {
            depth: 4,
            algorithm: RegionsSort,
            start: 0,
        },
        Point {
            depth: 4,
            algorithm: SkaSort,
            start: 0,
        },
        Point {
            depth: 4,
            algorithm: ScanningSort,
            start: 0,
        },
        Point {
            depth: 3,
            algorithm: RecombinatingSort,
            start: 0,
        },
        Point {
            depth: 3,
            algorithm: RegionsSort,
            start: 0,
        },
        Point {
            depth: 3,
            algorithm: ScanningSort,
            start: 0,
        },
        Point {
            depth: 3,
            algorithm: SkaSort,
            start: 0,
        },
        Point {
            depth: 2,
            algorithm: RegionsSort,
            start: 0,
        },
        Point {
            depth: 2,
            algorithm: SkaSort,
            start: 0,
        },
        Point {
            depth: 2,
            algorithm: ScanningSort,
            start: 0,
        },
        Point {
            depth: 2,
            algorithm: RecombinatingSort,
            start: 0,
        },
        Point {
            depth: 1,
            algorithm: ScanningSort,
            start: 0,
        },
        Point {
            depth: 1,
            algorithm: RegionsSort,
            start: 0,
        },
        Point {
            depth: 1,
            algorithm: RecombinatingSort,
            start: 0,
        },
        Point {
            depth: 1,
            algorithm: SkaSort,
            start: 0,
        },
        Point {
            depth: 0,
            algorithm: SkaSort,
            start: 50_000,
        },
        Point {
            depth: 0,
            algorithm: ScanningSort,
            start: 40_000_000,
        },
        Point {
            depth: 0,
            algorithm: RegionsSort,
            start: 100_000_000,
        },
        Point {
            depth: 0,
            algorithm: RecombinatingSort,
            start: 260_000,
        },
    ];

    out
}

fn sort_nodes(nodes: &mut Vec<Point>) {
    nodes.sort_by(|a, b| a.depth.cmp(&b.depth).then(a.start.cmp(&b.start)));
    nodes.reverse();
}

fn fitness(ml_tuner: MLTuner) -> u128 {
    let mut total = 0;
    let lens = [
        100,
        1_000,
        10_000,
        100_000,
        1_000_000,
        10_000_000,
        100_000_000,
        200_000_000,
    ];

    for len in lens {
        let offset = (N - len) / 2;
        let end = offset + len;

        let mut d = DATA_U32[offset..end].to_vec();
        let start = Instant::now();
        d.radix_sort_unstable_with_tuning(Box::new(ml_tuner.clone()));
        total += start.elapsed().as_nanos();
        drop(d);

        let mut d = DATA_U32_BIMODAL[offset..end].to_vec();
        let start = Instant::now();
        d.radix_sort_unstable_with_tuning(Box::new(ml_tuner.clone()));
        total += start.elapsed().as_nanos();
        drop(d);

        let mut d = DATA_U64[offset..end].to_vec();
        let start = Instant::now();
        d.radix_sort_unstable_with_tuning(Box::new(ml_tuner.clone()));
        total += start.elapsed().as_nanos();
        drop(d);

        let mut d = DATA_U64_BIMODAL[offset..end].to_vec();
        let start = Instant::now();
        d.radix_sort_unstable_with_tuning(Box::new(ml_tuner.clone()));
        total += start.elapsed().as_nanos();
        drop(d);
    }

    total
}

fn gen_inputs<T>(n: usize, shift: T) -> Vec<T>
where
    T: Copy + Sized + Send + Sync + Shr<Output = T> + ShrAssign,
{
    let mut inputs: Vec<T> = block_rand(n);
    inputs[0..(n / 2)].par_iter_mut().for_each(|v| *v >>= shift);
    inputs
}

fn main() {
    let progress_log = File::create("progress.csv").expect("Error creating progress log file");
    let population_log =
        File::create("population.txt").expect("Error creating population log file");

    let (solutions, generation, progress, _population) =
        GeneticExecution::<f64, GeneticSort>::new()
            .population_size(100)
            .genotype_size(1)
            .global_cache(true)
            .cache_fitness(true)
            .mutation_rate(Box::new(MutationRates::Linear(SlopeParams {
                start: 0.1_f64,
                bound: 0.005,
                coefficient: -0.0002,
            })))
            .selection_rate(Box::new(SelectionRates::Linear(SlopeParams {
                start: 4_f64,
                bound: 1.5,
                coefficient: -0.0005,
            })))
            .select_function(Box::new(SelectionFunctions::Cup))
            .age_function(Box::new(AgeFunctions::Quadratic(
                AgeThreshold(50),
                AgeSlope(1_f64),
            )))
            .progress_log(1, progress_log)
            .population_log(1, population_log)
            .stop_criterion(Box::new(StopCriteria::SolutionFound))
            .run();

    println!("{:?} {} {}", solutions, generation, progress);
}
