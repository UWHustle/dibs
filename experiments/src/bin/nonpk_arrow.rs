use clap::{App, Arg};
use dibs_experiments::benchmarks::nonpk;
use dibs_experiments::runner;
use dibs_experiments::systems::arrow::nonpk::{ArrowNonPKConnection, ArrowNonPKDatabase};
use dibs_experiments::worker::{StandardWorker, Worker};
use std::str::FromStr;
use std::sync::Arc;

const NUM_ROWS: u32 = 1000000;

fn main() {
    let matches = App::new("NonPK on Arrow")
        .arg(Arg::with_name("non_pk").required(true))
        .arg(Arg::with_name("filter_magnitude").required(true))
        .arg(Arg::with_name("num_workers").required(true))
        .get_matches();

    let non_pk = f64::from_str(matches.value_of("non_pk").unwrap()).unwrap();
    let filter_magnitude = usize::from_str(matches.value_of("filter_magnitude").unwrap()).unwrap();
    let num_workers = usize::from_str(matches.value_of("num_workers").unwrap()).unwrap();

    let dibs = Arc::new(nonpk::dibs(filter_magnitude));

    let db = Arc::new(ArrowNonPKDatabase::new(NUM_ROWS));

    let mut workers: Vec<Box<dyn Worker + Send>> = vec![];

    for worker_id in 0..num_workers {
        workers.push(Box::new(StandardWorker::new(
            worker_id,
            Some(Arc::clone(&dibs)),
            nonpk::NonPKGenerator::new(NUM_ROWS, non_pk),
            ArrowNonPKConnection::new(Arc::clone(&db)),
        )))
    }

    runner::run(workers);
}
