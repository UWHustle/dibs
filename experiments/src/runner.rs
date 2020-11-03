use crate::Client;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub fn run<C>(clients: Vec<C>)
where
    C: 'static + Client + Send,
{
    let warmup_duration = Duration::from_secs(5);
    let measurement_duration = Duration::from_secs(10);

    let transaction_counter = Arc::new(AtomicUsize::new(0));
    let terminate = Arc::new(AtomicBool::new(false));

    let handles = core_affinity::get_core_ids()
        .unwrap()
        .into_iter()
        .cycle()
        .zip(clients)
        .map(|(id, client)| {
            let transaction_counter = Arc::clone(&transaction_counter);
            let terminate = Arc::clone(&terminate);

            thread::spawn(move || {
                core_affinity::set_for_current(id);

                while !terminate.load(Ordering::Relaxed) {
                    let transaction_id = transaction_counter.fetch_add(1, Ordering::Relaxed);
                    client.execute_transaction(transaction_id);
                }
            })
        })
        .collect::<Vec<_>>();

    thread::sleep(warmup_duration);

    let start = transaction_counter.load(Ordering::Relaxed);

    thread::sleep(measurement_duration);

    let stop = transaction_counter.load(Ordering::Relaxed);

    terminate.store(true, Ordering::Relaxed);

    for handle in handles {
        handle.join().unwrap();
    }

    println!(
        "{}",
        (stop - start) / measurement_duration.as_secs() as usize
    );
}
