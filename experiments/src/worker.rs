use crate::{Generator, Procedure};
use dibs::Dibs;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

pub struct SharedState {
    group_counter: AtomicUsize,
    begin_counter: AtomicUsize,
    commit_counter: AtomicUsize,
    is_terminated: AtomicBool,
    dibs: Dibs,
}

impl SharedState {
    pub fn new(dibs: Dibs) -> SharedState {
        SharedState {
            group_counter: AtomicUsize::new(0),
            begin_counter: AtomicUsize::new(0),
            commit_counter: AtomicUsize::new(0),
            is_terminated: AtomicBool::new(false),
            dibs,
        }
    }

    pub fn get_commit_count(&self) -> usize {
        self.commit_counter.load(Ordering::Acquire)
    }

    pub fn terminate(&self) {
        self.is_terminated.store(true, Ordering::Release);
    }
}

pub struct Worker<G, P, C> {
    shared_state: Arc<SharedState>,
    generator: G,
    connection: C,
    _procedure: PhantomData<P>,
}

impl<G, P, C> Worker<G, P, C> {
    pub fn new(shared_state: Arc<SharedState>, generator: G, connection: C) -> Worker<G, P, C> {
        Worker {
            shared_state,
            generator,
            connection,
            _procedure: PhantomData::default(),
        }
    }
}

impl<G, P, C> Worker<G, P, C>
where
    G: Generator<P>,
    P: Procedure<C>,
{
    pub fn run(&mut self) {
        while !self.shared_state.is_terminated.load(Ordering::Acquire) {
            let group_id = self
                .shared_state
                .group_counter
                .fetch_add(1, Ordering::Relaxed);

            let transaction_id = self
                .shared_state
                .begin_counter
                .fetch_add(1, Ordering::Relaxed);

            let procedure = self.generator.next();

            procedure
                .execute(
                    group_id,
                    transaction_id,
                    &self.shared_state.dibs,
                    &mut self.connection,
                )
                .unwrap();

            self.shared_state
                .commit_counter
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}
