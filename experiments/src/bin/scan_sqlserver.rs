use clap::{App, Arg};
use dibs_experiments::benchmarks::scan::ScanGenerator;
use dibs_experiments::systems::odbc::alloc_env;
use dibs_experiments::systems::sqlserver::SQLServerScanConnection;
use dibs_experiments::worker::{StandardWorker, Worker};
use dibs_experiments::{runner, systems};
use std::str::FromStr;
use std::sync::Arc;

fn main() {
    let matches = App::new("Scans on SQL Server")
        .arg(Arg::with_name("select_mix").required(true))
        .arg(Arg::with_name("range").required(true))
        .arg(Arg::with_name("num_conjuncts").required(true))
        .arg(Arg::with_name("num_workers").required(true))
        .get_matches();

    let select_mix = f64::from_str(matches.value_of("select_mix").unwrap()).unwrap();
    let range = u8::from_str(matches.value_of("range").unwrap()).unwrap();
    let num_conjuncts = usize::from_str(matches.value_of("num_conjuncts").unwrap()).unwrap();
    let num_workers = usize::from_str(matches.value_of("num_workers").unwrap()).unwrap();

    let env = unsafe { alloc_env().unwrap() };

    unsafe {
        systems::sqlserver::load_scan(env, num_conjuncts).unwrap();
    }

    {
        let mut workers: Vec<Box<dyn Worker + Send>> = Vec::with_capacity(num_workers);

        for worker_id in 0..num_workers {
            workers.push(Box::new(StandardWorker::new(
                worker_id,
                None,
                ScanGenerator::new(select_mix, range, num_conjuncts),
                SQLServerScanConnection::new(env, num_conjuncts).unwrap(),
            )));
        }

        runner::run(workers);
    }
}
