use block_pseudorand::block_rand;
use lazy_static::lazy_static;
use nanorand::{Rng as WyRng, WyRand};
use oxigen::{
    AgeFunctions, AgeSlope, AgeThreshold, GeneticExecution, Genotype, MutationRates,
    SelectionFunctions, SelectionRates, SlopeParams,
};
use rand::distributions::Exp1;
use rand::prelude::*;
use rand::Rng;
use rayon::prelude::*;
use rdst::tuner::Algorithm::{LsbSort, RecombinatingSort, RegionsSort, ScanningSort, SkaSort};
use rdst::tuner::{Algorithm, Tuner, TuningParams};
use rdst::{RadixKey, RadixSort};
use std::fmt::{Debug, Display, Formatter};
use std::fs::File;
use std::ops::{Shr, ShrAssign};
use std::slice::Iter;
use std::time::Instant;
use std::vec::IntoIter;

static N: usize = 2_000_000;
lazy_static! {
    static ref DATA: Vec<Vec<u32>> = gen_tests(N, 16u32);
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct Point {
    level: usize,
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
        for point in self.points.iter() {
            if p.level == point.level && p.input_len >= point.start {
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

    fn generate(size: &Self::ProblemSize) -> Self {
        Self {
            tuner: MLTuner {
                points: get_nodes(),
            },
            intervals: vec![0f64; *size],
        }
    }

    fn fitness(&self) -> f64 {
        (1_000_000_000u32 - (fitness(self.tuner.clone()) as u32)) as f64
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

    fn is_solution(&self, fitness: f64) -> bool {
        fitness > 999_999_999.0
    }
}

fn get_nodes() -> Vec<Point> {
    let out = vec![
        Point {
            level: 3,
            algorithm: RegionsSort,
            start: 100000,
        },
        Point {
            level: 3,
            algorithm: ScanningSort,
            start: 100000,
        },
        Point {
            level: 3,
            algorithm: SkaSort,
            start: 50000,
        },
        Point {
            level: 3,
            algorithm: LsbSort,
            start: 10000,
        },
        Point {
            level: 3,
            algorithm: RecombinatingSort,
            start: 0,
        },
        Point {
            level: 2,
            algorithm: RegionsSort,
            start: 900000000,
        },
        Point {
            level: 2,
            algorithm: ScanningSort,
            start: 35642635,
        },
        Point {
            level: 2,
            algorithm: RecombinatingSort,
            start: 1559909,
        },
        Point {
            level: 2,
            algorithm: LsbSort,
            start: 10000,
        },
        Point {
            level: 2,
            algorithm: SkaSort,
            start: 0,
        },
        Point {
            level: 1,
            algorithm: RegionsSort,
            start: 900000000,
        },
        Point {
            level: 1,
            algorithm: ScanningSort,
            start: 44339106,
        },
        Point {
            level: 1,
            algorithm: RecombinatingSort,
            start: 900000,
        },
        Point {
            level: 1,
            algorithm: SkaSort,
            start: 50000,
        },
        Point {
            level: 1,
            algorithm: LsbSort,
            start: 10000,
        },
        Point {
            level: 0,
            algorithm: RegionsSort,
            start: 900000000,
        },
        Point {
            level: 0,
            algorithm: ScanningSort,
            start: 40000000,
        },
        Point {
            level: 0,
            algorithm: RecombinatingSort,
            start: 900000,
        },
        Point {
            level: 0,
            algorithm: LsbSort,
            start: 710609,
        },
        Point {
            level: 0,
            algorithm: SkaSort,
            start: 50000,
        },
    ];

    out
}

fn mutate_nodes(nodes: &mut Vec<Point>) {
    let mut rng = WyRand::new();
    let level = rng.generate_range(0..=3);
    let algo = rng.generate_range(1..=5);
    let algorithm = match algo {
        0 => Algorithm::ComparativeSort,
        1 => Algorithm::LsbSort,
        2 => Algorithm::SkaSort,
        3 => Algorithm::RecombinatingSort,
        4 => Algorithm::RegionsSort,
        5 => Algorithm::ScanningSort,
        _ => panic!(),
    };
    let action: usize = rng.generate_range(0..=1);
    let mut last_level = 0;
    let mut last_start = 100_000;
    let mut repl_i = 0;
    let mut repl_start = 100_000;
    for (i, mut node) in nodes.iter_mut().enumerate() {
        if algorithm == node.algorithm && level == node.level {
            let offset = (node.start as f64 * 0.1) as usize + rng.generate_range(0..1_000_000);

            match action {
                0 => node.start = node.start.saturating_add(offset),
                1 => node.start = node.start.saturating_sub(offset),
                2 => {
                    if last_level == node.level && i > 0 {
                        repl_i = i - 1;
                        repl_start = node.start;
                        node.start = last_start;
                    }
                }
                _ => panic!(),
            };
        } else {
            last_level = node.level;
            last_start = node.start;
        }
    }

    nodes[repl_i].start = repl_start;
}

fn sort_nodes(nodes: &mut Vec<Point>) {
    nodes.sort_unstable_by(|a, b| a.level.cmp(&b.level).then(a.start.cmp(&b.start)));
    nodes.reverse();
}

fn fitness(ml_tuner: MLTuner) -> u128 {
    let mut total = 0;
    let mut data = DATA.clone();

    for mut row in data.into_iter() {
        let start = Instant::now();
        row.radix_sort_unstable_with_tuning(Box::new(ml_tuner.clone()));
        let el = start.elapsed().as_nanos();

        total += el / (row.len() / 100) as u128;
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

fn gen_tests<T>(n: usize, shift: T) -> Vec<Vec<T>>
where
    T: Copy + Sized + Send + Sync + Shr<Output = T> + ShrAssign,
{
    let raw_data = gen_inputs::<T>(n, shift);
    let mut data = Vec::new();
    data.push(raw_data.clone());

    let mut i = 10;
    loop {
        data.push(raw_data[(n / i)..(n - (n / i))].to_vec());
        i = i + (i / 2);

        if i > n {
            break;
        }
    }

    data
}

fn main() {
    let progress_log = File::create("progress.csv").expect("Error creating progress log file");
    let population_log =
        File::create("population.txt").expect("Error creating population log file");
    let population_size = 10;

    let (solutions, generation, progress, _population) =
        GeneticExecution::<f64, GeneticSort>::new()
            .population_size(population_size)
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
            .run();

    println!("{:?} {} {}", solutions, generation, progress);
}
