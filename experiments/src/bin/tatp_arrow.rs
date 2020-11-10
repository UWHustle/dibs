use clap::{App, Arg};
use dibs::OptimizationLevel;
use dibs_experiments::arrow_server::{ArrowTATPConnection, ArrowTATPDatabase};
use dibs_experiments::tatp::TATPGenerator;
use dibs_experiments::worker::{SharedState, Worker};
use dibs_experiments::{runner, tatp};
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

    let dibs = tatp::dibs(optimization);
    let shared_state = Arc::new(SharedState::new(dibs));

    let db = Arc::new(ArrowTATPDatabase::new(num_rows));

    let workers = (0..num_workers)
        .map(|_| {
            Worker::new(
                Arc::clone(&shared_state),
                TATPGenerator::new(num_rows),
                ArrowTATPConnection::new(Arc::clone(&db)),
            )
        })
        .collect();

    runner::run(workers, shared_state);
}
