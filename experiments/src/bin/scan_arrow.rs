use clap::{App, Arg};
use dibs_experiments::scan::{ScanConfig, ScanClient};
use dibs_experiments::{scan, OptimizationLevel, runner};
use std::str::FromStr;
use std::sync::Arc;
use dibs_experiments::arrow::ArrowScanServer;

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

    let config = ScanConfig::new(num_rows, select_mix, range, num_conjuncts);
    let dibs = Arc::new(scan::dibs(num_conjuncts, optimization));
    let server = Arc::new(ArrowScanServer::new(&config));

    let clients = (0..num_workers)
        .map(|_| ScanClient::new(config.clone(), Arc::clone(&dibs), Arc::clone(&server)))
        .collect();

    runner::run(clients);
}
