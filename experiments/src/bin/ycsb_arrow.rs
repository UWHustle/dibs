use clap::{App, Arg};
use dibs::OptimizationLevel;
use dibs_experiments::benchmarks::ycsb;
use dibs_experiments::runner;
use dibs_experiments::systems::arrow::{ArrowYCSBConnection, ArrowYCSBDatabase};
use dibs_experiments::worker::{StandardWorker, Worker};
use std::str::FromStr;
use std::sync::Arc;

fn main() {
    let matches = App::new("YCSB on Arrow")
        .arg(Arg::with_name("num_rows").required(true))
        .arg(Arg::with_name("field_size").required(true))
        .arg(Arg::with_name("select_mix").required(true))
        .arg(Arg::with_name("num_statements_per_transaction").required(true))
        .arg(Arg::with_name("skew").required(true))
        .arg(
            Arg::with_name("optimization")
                .possible_values(&["ungrouped", "grouped", "prepared", "filtered"])
                .required(true),
        )
        .arg(Arg::with_name("num_workers").required(true))
        .get_matches();

    let num_rows = u32::from_str(matches.value_of("num_rows").unwrap()).unwrap();
    let field_size = usize::from_str(matches.value_of("field_size").unwrap()).unwrap();
    let select_mix = f64::from_str(matches.value_of("select_mix").unwrap()).unwrap();
    let num_statements_per_transaction =
        usize::from_str(matches.value_of("num_statements_per_transaction").unwrap()).unwrap();
    let skew = f64::from_str(matches.value_of("skew").unwrap()).unwrap();
    let optimization =
        OptimizationLevel::from_str(matches.value_of("optimization").unwrap()).unwrap();
    let num_workers = usize::from_str(matches.value_of("num_workers").unwrap()).unwrap();

    let dibs = Arc::new(ycsb::dibs(optimization));

    let db = Arc::new(ArrowYCSBDatabase::new(num_rows, field_size));

    let mut workers: Vec<Box<dyn Worker + Send>> = vec![];

    for worker_id in 0..num_workers {
        if skew == 0.0 {
            workers.push(Box::new(StandardWorker::new(
                worker_id,
                Arc::clone(&dibs),
                ycsb::uniform_generator(
                    num_rows,
                    field_size,
                    select_mix,
                    num_statements_per_transaction,
                ),
                ArrowYCSBConnection::new(Arc::clone(&db)),
            )));
        } else {
            workers.push(Box::new(StandardWorker::new(
                worker_id,
                Arc::clone(&dibs),
                ycsb::zipf_generator(
                    num_rows,
                    field_size,
                    select_mix,
                    num_statements_per_transaction,
                    skew,
                ),
                ArrowYCSBConnection::new(Arc::clone(&db)),
            )));
        }
    }

    runner::run(workers);
}
