#![feature(array_map)]
#![feature(test)]

use crate::arrowdb::ArrowTATPServer;
use crate::tatp::{TATPClient, TATPConfig};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

mod arrowdb;
mod tatp;

extern crate test;

pub trait Server {
    fn begin_transaction(&self);
    fn commit_transaction(&self);
}

fn main() {
    let config = TATPConfig::new(
        10_000,
        [0.35, 0.10, 0.35, 0.02, 0.14, 0.02, 0.02, 0.00, 0.00, 0.00],
        0,
        1,
    );

    let num_workers = 1;

    let dibs = Arc::new(tatp::dibs(&config));
    let server = Arc::new(ArrowTATPServer::new(&config));
    let transaction_counter = Arc::new(AtomicUsize::new(0));
    let terminate = Arc::new(AtomicBool::new(false));

    let handles = core_affinity::get_core_ids()
        .unwrap()
        .into_iter()
        .take(num_workers)
        .map(|id| {
            let client = TATPClient::new(
                config.clone(),
                Arc::clone(&dibs),
                Arc::clone(&server),
                Arc::clone(&transaction_counter),
                Arc::clone(&terminate.clone()),
            );

            thread::spawn(move || {
                core_affinity::set_for_current(id);

                client.run();
            })
        })
        .collect::<Vec<_>>();

    let duration = Duration::from_secs(30);

    thread::sleep(duration);

    terminate.store(true, Ordering::Relaxed);

    for handle in handles {
        handle.join().unwrap();
    }

    println!(
        "{}",
        transaction_counter.load(Ordering::Relaxed) / duration.as_secs() as usize
    );
}
