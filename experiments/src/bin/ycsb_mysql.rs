use clap::{App, Arg};
use dibs::OptimizationLevel;
use dibs_experiments::benchmarks::ycsb;
use dibs_experiments::systems::mysql::{IsolationMechanism, MySQLYCSBConnection};
use dibs_experiments::worker::{StandardWorker, Worker};
use dibs_experiments::{runner, systems};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

fn main() {
    let matches = App::new("YCSB on MySQL")
        .arg(Arg::with_name("num_rows").required(true))
        .arg(Arg::with_name("field_size").required(true))
        .arg(Arg::with_name("select_mix").required(true))
        .arg(Arg::with_name("num_statements_per_transaction").required(true))
        .arg(Arg::with_name("skew").required(true))
        .arg(Arg::with_name("isolation").required(true))
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
    let num_statements_per_transaction =
        usize::from_str(matches.value_of("num_statements_per_transaction").unwrap()).unwrap();
    let skew = f64::from_str(matches.value_of("skew").unwrap()).unwrap();
    let isolation = IsolationMechanism::from_str(matches.value_of("isolation").unwrap()).unwrap();
    let optimization =
        OptimizationLevel::from_str(matches.value_of("optimization").unwrap()).unwrap();
    let num_workers = usize::from_str(matches.value_of("num_workers").unwrap()).unwrap();

    let dibs = Arc::new(ycsb::dibs(optimization));
    let global_latencies = Arc::new(Mutex::new(vec![]));

    systems::mysql::load_ycsb(num_rows, field_size);

    let mut workers: Vec<Box<dyn Worker + Send>> = vec![];

    for worker_id in 0..num_workers {
        let dibs = match isolation {
            IsolationMechanism::DibsSerializable => Some(Arc::clone(&dibs)),
            IsolationMechanism::MySQLSerializable | IsolationMechanism::MySQLReadUncommitted => {
                None
            }
        };

        workers.push(if skew == 0.0 {
            Box::new(StandardWorker::new(
                worker_id,
                dibs,
                ycsb::uniform_generator(
                    num_rows,
                    field_size,
                    select_mix,
                    num_statements_per_transaction,
                ),
                MySQLYCSBConnection::new(isolation, Arc::clone(&global_latencies)),
            ))
        } else {
            Box::new(StandardWorker::new(
                worker_id,
                dibs,
                ycsb::zipf_generator(
                    num_rows,
                    field_size,
                    select_mix,
                    num_statements_per_transaction,
                    skew,
                ),
                MySQLYCSBConnection::new(isolation, Arc::clone(&global_latencies)),
            ))
        });
    }

    runner::run(workers);

    let mut latencies = global_latencies.lock().unwrap();
    latencies.sort_unstable();

    if latencies.len() > 0 {
        println!(
            "99th percentile latency: {} µs",
            latencies[latencies.len() * 99 / 100].as_micros()
        );
    }
}
