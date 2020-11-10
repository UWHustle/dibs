use crate::worker::{SharedState, Worker};
use crate::{Generator, Procedure};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub fn run<G, P, C>(workers: Vec<Worker<G, P, C>>, shared_state: Arc<SharedState>)
where
    G: 'static + Generator<P> + Send,
    P: 'static + Procedure<C> + Send,
    C: 'static + Send,
{
    let warmup_duration = Duration::from_secs(5);
    let measurement_duration = Duration::from_secs(10);

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
