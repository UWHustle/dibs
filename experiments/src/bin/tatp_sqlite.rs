use clap::{App, Arg};
use dibs::OptimizationLevel;
use dibs_experiments::benchmarks::tatp;
use dibs_experiments::benchmarks::tatp::TATPGenerator;
use dibs_experiments::systems::sqlite::SQLiteTATPConnection;
use dibs_experiments::worker::{
    GroupCommitWorker, ReadOnlyGenerator, ReceivingGenerator, StandardWorker, Worker,
};
use dibs_experiments::{runner, systems};
use std::str::FromStr;
use std::sync::{mpsc, Arc};

fn main() {
    let matches = App::new("TATP on SQLite")
        .arg(Arg::with_name("num_rows").required(true))
        .arg(Arg::with_name("num_transactions_per_group").required(true))
        .arg(
            Arg::with_name("optimization")
                .possible_values(&["ungrouped", "grouped", "prepared", "filtered"])
                .required(true),
        )
        .arg(Arg::with_name("num_workers").required(true))
        .get_matches();

    let num_rows = u32::from_str(matches.value_of("num_rows").unwrap()).unwrap();
    let num_transactions_per_group =
        usize::from_str(matches.value_of("num_transactions_per_group").unwrap()).unwrap();
    let optimization =
        OptimizationLevel::from_str(matches.value_of("optimization").unwrap()).unwrap();
    let num_workers = usize::from_str(matches.value_of("num_workers").unwrap()).unwrap();

    let dibs = Arc::new(tatp::dibs(optimization));

    systems::sqlite::load_tatp("tatp.sqlite", num_rows);

    let (sender, receiver) = mpsc::sync_channel(0);

    let mut workers: Vec<Box<dyn Worker + Send>> = vec![Box::new(GroupCommitWorker::new(
        0,
        Some(dibs),
        ReceivingGenerator::new(TATPGenerator::new(num_rows), receiver),
        SQLiteTATPConnection::new("tatp.sqlite"),
        num_transactions_per_group,
    ))];

    for worker_id in 1..num_workers {
        let generator: ReadOnlyGenerator<TATPGenerator, SQLiteTATPConnection> =
            ReadOnlyGenerator::new(TATPGenerator::new(num_rows), sender.clone());

        workers.push(Box::new(StandardWorker::new(
            worker_id,
            None,
            generator,
            SQLiteTATPConnection::new("tatp.sqlite"),
        )))
    }

    runner::run(workers);
}
