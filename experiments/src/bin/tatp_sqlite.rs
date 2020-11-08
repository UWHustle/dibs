use clap::{App, Arg};
use dibs_experiments::sqlite_server::SQLiteTATPConnection;
use dibs_experiments::tatp::{TATPClient, TATPConfig};
use dibs_experiments::{runner, sqlite_server, tatp, OptimizationLevel};
use std::str::FromStr;
use std::sync::Arc;

fn main() {
    let matches = App::new("TATP on SQLite")
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

    let config = TATPConfig::new(num_rows);
    let dibs = Arc::new(tatp::dibs(optimization));

    sqlite_server::load_tatp("tatp.sqlite", num_rows);

    let clients = (0..num_workers)
        .map(|_| {
            TATPClient::new(
                config.clone(),
                Arc::clone(&dibs),
                SQLiteTATPConnection::new("tatp.sqlite"),
            )
        })
        .collect();

    runner::run(clients);
}
