use crate::worker::{SharedState, Worker};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub fn run(workers: Vec<Box<dyn Worker + Send>>, shared_state: Arc<SharedState>) {
    let warmup_duration = Duration::from_secs(10);
    let measurement_duration = Duration::from_secs(60);

    let handles = core_affinity::get_core_ids()
        .unwrap()
        .into_iter()
        .cycle()
        .zip(workers)
        .map(|(core_id, mut worker)| {
            thread::spawn(move || {
                core_affinity::set_for_current(core_id);
                worker.run();
            })
        })
        .collect::<Vec<_>>();

    thread::sleep(warmup_duration);

    let start = shared_state.get_commit_count();

    thread::sleep(measurement_duration);

    let stop = shared_state.get_commit_count();

    shared_state.terminate();

    for handle in handles {
        handle.join().unwrap();
    }

    println!(
        "{}",
        (stop - start) / measurement_duration.as_secs() as usize
    );
}
