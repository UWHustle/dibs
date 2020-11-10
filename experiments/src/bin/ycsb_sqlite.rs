use clap::{App, Arg};
use dibs::OptimizationLevel;
use dibs_experiments::benchmarks::ycsb;
use dibs_experiments::benchmarks::ycsb::YCSBGenerator;
use dibs_experiments::systems::sqlite::SQLiteYCSBConnection;
use dibs_experiments::worker::{
    GroupCommitWorker, ReadOnlyGenerator, ReceivingGenerator, SharedState, Worker,
};
use dibs_experiments::{runner, systems};
use rand::distributions::Distribution;
use std::str::FromStr;
use std::sync::{mpsc, Arc};

fn make_workers<F, D>(
    num_transactions_per_group: usize,
    num_workers: usize,
    shared_state: &Arc<SharedState>,
    make_generator: F,
) -> Vec<Box<dyn Worker + Send>>
where
    F: Fn() -> YCSBGenerator<D>,
    D: 'static + Distribution<usize> + Send,
{
    let (sender, receiver) = mpsc::sync_channel(0);

    let mut workers: Vec<Box<dyn Worker + Send>> = vec![Box::new(GroupCommitWorker::new(
        Arc::clone(shared_state),
        ReceivingGenerator::new(make_generator(), receiver),
        SQLiteYCSBConnection::new("ycsb.sqlite"),
        num_transactions_per_group,
    ))];

    for _ in 1..num_workers {
        let generator: ReadOnlyGenerator<YCSBGenerator<D>, SQLiteYCSBConnection> =
            ReadOnlyGenerator::new(make_generator(), sender.clone());

        workers.push(Box::new(GroupCommitWorker::new(
            Arc::clone(&shared_state),
            generator,
            SQLiteYCSBConnection::new("ycsb.sqlite"),
            num_transactions_per_group,
        )));
    }

    workers
}

fn main() {
    let matches = App::new("YCSB on SQLite")
        .arg(Arg::with_name("num_rows").required(true))
        .arg(Arg::with_name("num_transactions_per_group").required(true))
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
    let num_transactions_per_group =
        usize::from_str(matches.value_of("num_transactions_per_group").unwrap()).unwrap();
    let field_size = usize::from_str(matches.value_of("field_size").unwrap()).unwrap();
    let select_mix = f64::from_str(matches.value_of("select_mix").unwrap()).unwrap();
    let skew = f64::from_str(matches.value_of("skew").unwrap()).unwrap();
    let optimization =
        OptimizationLevel::from_str(matches.value_of("optimization").unwrap()).unwrap();
    let num_workers = usize::from_str(matches.value_of("num_workers").unwrap()).unwrap();

    let dibs = ycsb::dibs(optimization);
    let shared_state = Arc::new(SharedState::new(dibs));

    systems::sqlite::load_ycsb("ycsb.sqlite", num_rows, field_size);

    let workers = if skew == 0.0 {
        make_workers(
            num_transactions_per_group,
            num_workers,
            &shared_state,
            || ycsb::uniform_generator(num_rows, field_size, select_mix),
        )
    } else {
        make_workers(
            num_transactions_per_group,
            num_workers,
            &shared_state,
            || ycsb::zipf_generator(num_rows, field_size, select_mix, skew),
        )
    };

    runner::run(workers, shared_state);
}
