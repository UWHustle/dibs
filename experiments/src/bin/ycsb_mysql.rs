use clap::{App, Arg};
use dibs::OptimizationLevel;
use dibs_experiments::benchmarks::ycsb;
use dibs_experiments::systems::mysql::MySQLYCSBConnection;
use dibs_experiments::worker::{SharedState, StandardWorker, Worker};
use dibs_experiments::{runner, systems};
use std::str::FromStr;
use std::sync::Arc;

fn main() {
    let matches = App::new("YCSB on MySQL")
        .arg(Arg::with_name("num_rows").required(true))
        .arg(Arg::with_name("field_size").required(true))
        .arg(Arg::with_name("select_mix").required(true))
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
    let skew = f64::from_str(matches.value_of("skew").unwrap()).unwrap();
    let optimization =
        OptimizationLevel::from_str(matches.value_of("optimization").unwrap()).unwrap();
    let num_workers = usize::from_str(matches.value_of("num_workers").unwrap()).unwrap();

    let dibs = ycsb::dibs(optimization);
    let shared_state = Arc::new(SharedState::new(dibs));

    systems::mysql::load_ycsb(num_rows, field_size);

    let mut workers: Vec<Box<dyn Worker + Send>> = vec![];

    for _ in 0..num_workers {
        if skew == 0.0 {
            workers.push(Box::new(StandardWorker::new(
                Arc::clone(&shared_state),
                ycsb::uniform_generator(num_rows, field_size, select_mix),
                MySQLYCSBConnection::new(),
            )));
        } else {
            workers.push(Box::new(StandardWorker::new(
                Arc::clone(&shared_state),
                ycsb::zipf_generator(num_rows, field_size, select_mix, skew),
                MySQLYCSBConnection::new(),
            )));
        }
    }

    runner::run(workers, shared_state);
}
