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
    let config = TATPConfig::new(1_000_000);
    let server = Arc::new(ArrowTATPServer::new(&config));
    let transaction_counter = Arc::new(AtomicUsize::new(0));
    let terminate = Arc::new(AtomicBool::new(false));

    let threads = (0..2)
        .map(|_| {
            let client = TATPClient::new(
                config.clone(),
                Arc::clone(&server),
                Arc::clone(&transaction_counter),
                Arc::clone(&terminate.clone()),
            );

            thread::spawn(move || {
                client.run();
            })
        })
        .collect::<Vec<_>>();

    thread::sleep(Duration::from_secs(5));

    terminate.store(true, Ordering::Relaxed);

    for t in threads {
        t.join().unwrap();
    }

    println!("{}", transaction_counter.load(Ordering::Relaxed) / 5);
}
