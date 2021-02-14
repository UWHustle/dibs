use clap::{App, Arg};
use dibs::OptimizationLevel;
use dibs_experiments::benchmarks::scan;
use dibs_experiments::benchmarks::scan::ScanGenerator;
use dibs_experiments::runner;
use dibs_experiments::systems::arrow::scan::{ArrowScanConnection, ArrowScanDatabase};
use dibs_experiments::worker::{StandardWorker, Worker};
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
        .arg(Arg::with_name("blowup_limit").required(true))
        .arg(Arg::with_name("num_workers").required(true))
        .get_matches();

    let num_rows = u32::from_str(matches.value_of("num_rows").unwrap()).unwrap();
    let select_mix = f64::from_str(matches.value_of("select_mix").unwrap()).unwrap();
    let range = u8::from_str(matches.value_of("range").unwrap()).unwrap();
    let num_conjuncts = usize::from_str(matches.value_of("num_conjuncts").unwrap()).unwrap();
    let optimization =
        OptimizationLevel::from_str(matches.value_of("optimization").unwrap()).unwrap();
    let blowup_limit = usize::from_str(matches.value_of("blowup_limit").unwrap()).unwrap();
    let num_workers = usize::from_str(matches.value_of("num_workers").unwrap()).unwrap();

    let dibs = Arc::new(scan::dibs(num_conjuncts, optimization, blowup_limit));

    let db = Arc::new(ArrowScanDatabase::new(num_rows));

    let mut workers: Vec<Box<dyn Worker + Send>> = vec![];

    for worker_id in 0..num_workers {
        workers.push(Box::new(StandardWorker::new(
            worker_id,
            Some(Arc::clone(&dibs)),
            ScanGenerator::new(select_mix, range),
            ArrowScanConnection::new(Arc::clone(&db)),
        )))
    }

    runner::run(workers);
}
