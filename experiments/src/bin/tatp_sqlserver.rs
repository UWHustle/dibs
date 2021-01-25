use clap::{App, Arg};
use dibs_experiments::benchmarks::tatp_sp::TATPSPGenerator;
use dibs_experiments::worker::{StandardWorker, Worker};
use dibs_experiments::{runner, systems};
use dibs_experiments::systems::odbc::{alloc_env, free_env};
use dibs_experiments::systems::sqlserver::SQLServerTATPConnection;
use std::str::FromStr;

fn main() {
    let matches = App::new("TATP on SQL Server")
        .arg(Arg::with_name("num_rows").required(true))
        .arg(Arg::with_name("num_workers").required(true))
        .get_matches();

    let num_rows = u32::from_str(matches.value_of("num_rows").unwrap()).unwrap();
    let num_workers = usize::from_str(matches.value_of("num_workers").unwrap()).unwrap();

    let env = unsafe { alloc_env() };

    unsafe {
        systems::sqlserver::load_tatp(env, num_rows);
    }

    {
        let mut workers: Vec<Box<dyn Worker + Send>> = Vec::with_capacity(num_workers);

        for worker_id in 0..num_workers {
            workers.push(Box::new(StandardWorker::new(
                worker_id,
                None,
                TATPSPGenerator::new(num_rows),
                SQLServerTATPConnection::new(env),
            )));
        }

        runner::run(workers);
    }

    unsafe {
        free_env(env);
    }
}
