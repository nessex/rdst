use block_pseudorand::block_rand;
use lazy_static::lazy_static;
use oxigen::{AgeFunctions, AgeSlope, AgeThreshold, GeneticExecution, Genotype, MutationRates, SelectionFunctions, SelectionRates, SlopeParams, StopCriteria};
use rand::distributions::Exp1;
use rand::prelude::*;
use rand::Rng;
use rayon::prelude::*;
use rdst::tuner::Algorithm::{LsbSort, RecombinatingSort, RegionsSort, ScanningSort, SkaSort};
use rdst::tuner::{Algorithm, Tuner, TuningParams};
use rdst::RadixSort;
use std::fmt::{Debug, Display, Formatter};
use std::fs::File;
use std::ops::{Shr, ShrAssign};
use std::slice::Iter;
use std::time::Instant;
use std::vec::IntoIter;

static N: usize = 100_000_000;
lazy_static! {
    static ref DATA: Vec<u32> = gen_inputs(N, 16u32);
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct Point {
    depth: usize,
    algorithm: Algorithm,
    start: usize,
}

#[derive(Clone, Debug)]
struct MLTuner {
    points: Vec<Point>,
}

#[derive(Debug, Clone)]
struct GeneticSort {
    tuner: MLTuner,
    intervals: Vec<f64>,
}

impl Display for GeneticSort {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("{:?}", self.tuner.points))
    }
}

impl Tuner for MLTuner {
    fn pick_algorithm(&self, p: &TuningParams) -> Algorithm {
        let depth = p.total_levels - p.level;
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
            tuner: MLTuner { points },
            intervals,
        }
    }

    fn fitness(&self) -> f64 {
        (1_000_000_000_000u64 - (fitness(self.tuner.clone()) as u64)) as f64
    }

    fn mutate(&mut self, rgen: &mut SmallRng, index: usize) {
        self.intervals[index] = (rgen.sample(Exp1) - 0.5) * (N as f64);

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
}

fn get_nodes() -> Vec<Point> {
    let out = vec![
        Point {
            depth: 3,
            algorithm: RegionsSort,
            start: 100000,
        },
        Point {
            depth: 3,
            algorithm: ScanningSort,
            start: 100000,
        },
        Point {
            depth: 3,
            algorithm: SkaSort,
            start: 50000,
        },
        Point {
            depth: 3,
            algorithm: LsbSort,
            start: 10000,
        },
        Point {
            depth: 3,
            algorithm: RecombinatingSort,
            start: 0,
        },
        Point {
            depth: 2,
            algorithm: RegionsSort,
            start: 900000000,
        },
        Point {
            depth: 2,
            algorithm: ScanningSort,
            start: 35642635,
        },
        Point {
            depth: 2,
            algorithm: RecombinatingSort,
            start: 1559909,
        },
        Point {
            depth: 2,
            algorithm: LsbSort,
            start: 10000,
        },
        Point {
            depth: 2,
            algorithm: SkaSort,
            start: 0,
        },
        Point {
            depth: 1,
            algorithm: RegionsSort,
            start: 900000000,
        },
        Point {
            depth: 1,
            algorithm: ScanningSort,
            start: 44339106,
        },
        Point {
            depth: 1,
            algorithm: RecombinatingSort,
            start: 900000,
        },
        Point {
            depth: 1,
            algorithm: SkaSort,
            start: 50000,
        },
        Point {
            depth: 1,
            algorithm: LsbSort,
            start: 10000,
        },
        Point {
            depth: 0,
            algorithm: RegionsSort,
            start: 900000000,
        },
        Point {
            depth: 0,
            algorithm: ScanningSort,
            start: 40000000,
        },
        Point {
            depth: 0,
            algorithm: RecombinatingSort,
            start: 900000,
        },
        Point {
            depth: 0,
            algorithm: LsbSort,
            start: 710609,
        },
        Point {
            depth: 0,
            algorithm: SkaSort,
            start: 50000,
        },
    ];

    out
}

fn sort_nodes(nodes: &mut Vec<Point>) {
    nodes.sort_unstable_by(|a, b| a.depth.cmp(&b.depth).then(a.start.cmp(&b.start)));
    nodes.reverse();
}

fn fitness(ml_tuner: MLTuner) -> u128 {
    let mut total = 0;
    let lens = [100, 1000, 10000, 100000, 1000000, 10000000, 100000000];

    for len in lens {
        let offset = len / 2;
        let end = DATA.len() - offset;
        let mut d = DATA[offset..end].to_vec();
        let start = Instant::now();
        d.radix_sort_unstable_with_tuning(Box::new(ml_tuner.clone()));
        let el = start.elapsed().as_nanos();

        total += el / (len / 100) as u128;
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
            .population_size(10)
            .genotype_size(N)
            .mutation_rate(Box::new(MutationRates::Linear(SlopeParams {
                start: 0.1_f64,
                bound: 0.005,
                coefficient: -0.0002,
            })))
            .selection_rate(Box::new(SelectionRates::Linear(SlopeParams {
                start: 3_f64,
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
