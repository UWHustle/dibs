use clap::{App, Arg};
use dibs::OptimizationLevel;
use dibs_experiments::benchmarks::tatp;
use dibs_experiments::benchmarks::tatp::TATPGenerator;
use dibs_experiments::runner;
use dibs_experiments::systems::arrow::{ArrowTATPConnection, ArrowTATPDatabase};
use dibs_experiments::worker::{StandardWorker, Worker};
use std::str::FromStr;
use std::sync::Arc;

fn main() {
    let matches = App::new("TATP on Arrow")
        .arg(Arg::with_name("num_rows").required(true))
        .arg(
            Arg::with_name("optimization")
                .possible_values(&["ungrouped", "grouped", "prepared", "filtered"])
                .required(true),
        )
        .arg(Arg::with_name("num_workers").required(true))
        .get_matches();

    let num_rows = u32::from_str(matches.value_of("num_rows").unwrap()).unwrap();
    let optimization =
        OptimizationLevel::from_str(matches.value_of("optimization").unwrap()).unwrap();
    let num_workers = usize::from_str(matches.value_of("num_workers").unwrap()).unwrap();

    let dibs = Arc::new(tatp::dibs(optimization));

    let db = Arc::new(ArrowTATPDatabase::new(num_rows));

    let mut workers: Vec<Box<dyn Worker + Send>> = vec![];

    for worker_id in 0..num_workers {
        workers.push(Box::new(StandardWorker::new(
            worker_id,
            Arc::clone(&dibs),
            TATPGenerator::new(num_rows),
            ArrowTATPConnection::new(Arc::clone(&db)),
        )));
    }

    runner::run(workers);
}
