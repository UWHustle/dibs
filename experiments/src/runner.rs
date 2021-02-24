use crate::worker::Worker;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub fn run(workers: Vec<Box<dyn Worker + Send>>) {
    let warmup_duration = Duration::from_secs(10);
    let measurement_duration = Duration::from_secs(60);

    let commit_counters = (0..workers.len())
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect::<Vec<_>>();
    let terminate = Arc::new(AtomicBool::new(false));

    let handles = core_affinity::get_core_ids()
        .unwrap()
        .into_iter()
        .cycle()
        .zip(workers)
        .zip(&commit_counters)
        .map(|((core_id, mut worker), commits)| {
            let commits = Arc::clone(&commits);
            let terminate = Arc::clone(&terminate);

            thread::spawn(move || {
                core_affinity::set_for_current(core_id);
                worker.run(commits, terminate);
            })
        })
        .collect::<Vec<_>>();

    thread::sleep(warmup_duration);

    let start = commit_counters
        .iter()
        .map(|commits| commits.load(Ordering::Relaxed))
        .sum::<usize>();

    thread::sleep(measurement_duration);

    let stop = commit_counters
        .iter()
        .map(|commits| commits.load(Ordering::Relaxed))
        .sum::<usize>();

    terminate.store(true, Ordering::Relaxed);

    for handle in handles {
        handle.join().unwrap();
    }

    println!(
        "{}",
        (stop - start) / measurement_duration.as_secs() as usize
    );
}
