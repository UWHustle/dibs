use clap::{App, Arg};
use dibs::OptimizationLevel;
use dibs_experiments::benchmarks::ycsb;
use dibs_experiments::benchmarks::ycsb::YCSBGenerator;
use dibs_experiments::runner;
use dibs_experiments::systems::arrow::{ArrowYCSBConnection, ArrowYCSBDatabase};
use dibs_experiments::worker::{SharedState, StandardWorker, Worker};
use std::str::FromStr;
use std::sync::Arc;

fn main() {
    let matches = App::new("YCSB on Arrow")
        .arg(Arg::with_name("num_rows").required(true))
        .arg(Arg::with_name("num_fields").required(true))
        .arg(Arg::with_name("field_size").required(true))
        .arg(Arg::with_name("select_mix").required(true))
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
    let field_size = usize::from_str(matches.value_of("field_size").unwrap()).unwrap();
    let select_mix = f64::from_str(matches.value_of("select_mix").unwrap()).unwrap();
    let num_workers = usize::from_str(matches.value_of("num_workers").unwrap()).unwrap();

    let dibs = ycsb::dibs(optimization);
    let shared_state = Arc::new(SharedState::new(dibs));

    let db = Arc::new(ArrowYCSBDatabase::new(num_rows, field_size));

    let mut workers: Vec<Box<dyn Worker + Send>> = vec![];

    for _ in 0..num_workers {
        workers.push(Box::new(StandardWorker::new(
            Arc::clone(&shared_state),
            YCSBGenerator::new(num_rows, field_size, select_mix),
            ArrowYCSBConnection::new(Arc::clone(&db)),
        )));
    }

    runner::run(workers, shared_state);
}
