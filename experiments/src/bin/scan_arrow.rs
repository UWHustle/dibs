use clap::{App, Arg};
use dibs::OptimizationLevel;
use dibs_experiments::arrow_server::{ArrowScanConnection, ArrowScanDatabase};
use dibs_experiments::scan::ScanGenerator;
use dibs_experiments::worker::{SharedState, Worker};
use dibs_experiments::{runner, scan};
use std::str::FromStr;
use std::sync::Arc;

fn main() {
    let matches = App::new("Scans on Arrow")
        .arg(Arg::with_name("num_rows").required(true))
        .arg(Arg::with_name("select_mix").required(true))
        .arg(Arg::with_name("update_mix").required(true))
        .arg(Arg::with_name("range").required(true))
        .arg(Arg::with_name("num_conjuncts").required(true))
        .arg(
            Arg::with_name("optimization")
                .possible_values(&["ungrouped", "grouped", "prepared", "filtered"])
                .required(true),
        )
        .arg(Arg::with_name("num_workers").required(true))
        .get_matches();

    let num_rows = u32::from_str(matches.value_of("num_rows").unwrap()).unwrap();
    let select_mix = f64::from_str(matches.value_of("select_mix").unwrap()).unwrap();
    let range = u8::from_str(matches.value_of("range").unwrap()).unwrap();
    let num_conjuncts = usize::from_str(matches.value_of("num_conjuncts").unwrap()).unwrap();
    let optimization =
        OptimizationLevel::from_str(matches.value_of("optimization").unwrap()).unwrap();
    let num_workers = usize::from_str(matches.value_of("num_workers").unwrap()).unwrap();

    let dibs = scan::dibs(num_conjuncts, optimization);
    let shared_state = Arc::new(SharedState::new(dibs));

    let db = Arc::new(ArrowScanDatabase::new(num_rows));

    let workers = (0..num_workers)
        .map(|_| {
            Worker::new(
                Arc::clone(&shared_state),
                ScanGenerator::new(select_mix, range),
                ArrowScanConnection::new(Arc::clone(&db)),
            )
        })
        .collect();

    runner::run(workers, shared_state);
}
