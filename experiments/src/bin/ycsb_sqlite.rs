use clap::{App, Arg};
use dibs::OptimizationLevel;
use dibs_experiments::benchmarks::ycsb;
use dibs_experiments::benchmarks::ycsb::YCSBGenerator;
use dibs_experiments::systems;
use dibs_experiments::systems::sqlite::SQLiteYCSBConnection;
use dibs_experiments::worker::{
    GroupCommitWorker, ReadOnlyGenerator, ReceivingGenerator, SharedState, Worker,
};
use std::str::FromStr;
use std::sync::{mpsc, Arc};

fn main() {
    let matches = App::new("YCSB on SQLite")
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
    let num_transactions_per_group =
        usize::from_str(matches.value_of("num_transactions_per_group").unwrap()).unwrap();
    let optimization =
        OptimizationLevel::from_str(matches.value_of("optimization").unwrap()).unwrap();
    let field_size = usize::from_str(matches.value_of("field_size").unwrap()).unwrap();
    let select_mix = f64::from_str(matches.value_of("select_mix").unwrap()).unwrap();
    let num_workers = usize::from_str(matches.value_of("num_workers").unwrap()).unwrap();

    let dibs = ycsb::dibs(optimization);
    let shared_state = Arc::new(SharedState::new(dibs));

    systems::sqlite::load_ycsb("ycsb.sqlite", num_rows, field_size);

    let (sender, receiver) = mpsc::sync_channel(0);

    let mut workers: Vec<Box<dyn Worker + Send>> = vec![Box::new(GroupCommitWorker::new(
        Arc::clone(&shared_state),
        ReceivingGenerator::new(
            YCSBGenerator::new(num_rows, field_size, select_mix),
            receiver,
        ),
        SQLiteYCSBConnection::new("ycsb.sqlite"),
        num_transactions_per_group,
    ))];

    for _ in 1..num_workers {
        let generator: ReadOnlyGenerator<YCSBGenerator, SQLiteYCSBConnection> =
            ReadOnlyGenerator::new(
                YCSBGenerator::new(num_rows, field_size, select_mix),
                sender.clone(),
            );

        workers.push(Box::new(GroupCommitWorker::new(
            Arc::clone(&shared_state),
            generator,
            SQLiteYCSBConnection::new("ycsb.sqlite"),
            num_transactions_per_group,
        )))
    }
}
