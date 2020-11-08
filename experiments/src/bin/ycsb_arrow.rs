use clap::{App, Arg};
use dibs_experiments::arrow_server::{ArrowYCSBConnection, ArrowYCSBDatabase};
use dibs_experiments::ycsb::{YCSBClient, YCSBConfig};
use dibs_experiments::{runner, ycsb, OptimizationLevel};
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
    let num_fields = usize::from_str(matches.value_of("num_fields").unwrap()).unwrap();
    let field_size = usize::from_str(matches.value_of("field_size").unwrap()).unwrap();
    let select_mix = f64::from_str(matches.value_of("select_mix").unwrap()).unwrap();
    let num_workers = usize::from_str(matches.value_of("num_workers").unwrap()).unwrap();

    let config = YCSBConfig::new(num_rows, num_fields, field_size, select_mix);
    let dibs = Arc::new(ycsb::dibs(num_fields, optimization));
    let db = Arc::new(ArrowYCSBDatabase::new(num_rows, num_fields, field_size));

    let clients = (0..num_workers)
        .map(|_| {
            YCSBClient::new(
                config.clone(),
                Arc::clone(&dibs),
                ArrowYCSBConnection::new(Arc::clone(&db)),
            )
        })
        .collect();

    runner::run(clients);
}
