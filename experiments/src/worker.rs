use crate::{Connection, Generator, Procedure};
use dibs::Dibs;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError};
use std::sync::Arc;

pub struct SharedState {
    group_counter: AtomicUsize,
    begin_counter: AtomicUsize,
    commit_counter: AtomicUsize,
    terminate_flag: AtomicBool,
    dibs: Dibs,
}

impl SharedState {
    pub fn new(dibs: Dibs) -> SharedState {
        SharedState {
            group_counter: AtomicUsize::new(0),
            begin_counter: AtomicUsize::new(0),
            commit_counter: AtomicUsize::new(0),
            terminate_flag: AtomicBool::new(false),
            dibs,
        }
    }

    pub fn get_commit_count(&self) -> usize {
        self.commit_counter.load(Ordering::Acquire)
    }

    pub fn terminate(&self) {
        self.terminate_flag.store(true, Ordering::Release);
    }

    fn group_id(&self) -> usize {
        self.group_counter.fetch_add(1, Ordering::Relaxed)
    }

    fn transaction_id(&self) -> usize {
        self.begin_counter.fetch_add(1, Ordering::Relaxed)
    }

    fn increment_commit_count(&self, val: usize) {
        self.commit_counter.fetch_add(val, Ordering::Relaxed);
    }

    fn is_terminated(&self) -> bool {
        self.terminate_flag.load(Ordering::Acquire)
    }
}

pub struct ReadOnlyGenerator<G, C>
where
    G: Generator,
{
    inner: G,
    sender: SyncSender<G::Item>,
    _phantom: PhantomData<C>,
}

impl<G, C> ReadOnlyGenerator<G, C>
where
    G: Generator,
{
    pub fn new(inner: G, sender: SyncSender<G::Item>) -> ReadOnlyGenerator<G, C> {
        ReadOnlyGenerator {
            inner,
            sender,
            _phantom: PhantomData::default(),
        }
    }
}

impl<G, C> Generator for ReadOnlyGenerator<G, C>
where
    G: Generator,
    G::Item: Procedure<C>,
{
    type Item = G::Item;

    fn next(&self) -> G::Item {
        loop {
            let procedure = self.inner.next();

            if procedure.is_read_only() {
                break procedure;
            }

            self.sender
                .send(procedure)
                .expect("cannot send procedure (channel closed)");
        }
    }
}

pub struct ReceivingGenerator<G>
where
    G: Generator,
{
    inner: G,
    receiver: Receiver<G::Item>,
}

impl<G> ReceivingGenerator<G>
where
    G: Generator,
{
    pub fn new(inner: G, receiver: Receiver<G::Item>) -> ReceivingGenerator<G> {
        ReceivingGenerator { inner, receiver }
    }
}

impl<G> Generator for ReceivingGenerator<G>
where
    G: Generator,
{
    type Item = G::Item;

    fn next(&self) -> G::Item {
        match self.receiver.try_recv() {
            Ok(procedure) => procedure,
            Err(e) => match e {
                TryRecvError::Empty => self.inner.next(),
                TryRecvError::Disconnected => panic!("cannot receive procedure (channel closed)"),
            },
        }
    }
}

pub trait Worker {
    fn run(&mut self);
}

pub struct StandardWorker<G, C> {
    shared_state: Arc<SharedState>,
    generator: G,
    connection: C,
}

impl<G, C> StandardWorker<G, C> {
    pub fn new(
        shared_state: Arc<SharedState>,
        generator: G,
        connection: C,
    ) -> StandardWorker<G, C> {
        StandardWorker {
            shared_state,
            generator,
            connection,
        }
    }
}

impl<G, C> Worker for StandardWorker<G, C>
where
    G: Generator,
    G::Item: Procedure<C>,
    C: Connection,
{
    fn run(&mut self) {
        while !self.shared_state.is_terminated() {
            let group_id = self.shared_state.group_id();
            let transaction_id = self.shared_state.transaction_id();

            let procedure = self.generator.next();

            self.connection.begin();

            procedure
                .execute(
                    group_id,
                    transaction_id,
                    &self.shared_state.dibs,
                    &mut self.connection,
                )
                .unwrap();

            self.connection.commit();

            self.shared_state.increment_commit_count(1);
        }
    }
}

unsafe impl<G, C> Send for StandardWorker<G, C> {}

pub struct GroupCommitWorker<G, C> {
    shared_state: Arc<SharedState>,
    generator: G,
    connection: C,
    num_transactions_per_group: usize,
}

impl<G, C> GroupCommitWorker<G, C> {
    pub fn new(
        shared_state: Arc<SharedState>,
        generator: G,
        connection: C,
        num_transactions_per_group: usize,
    ) -> GroupCommitWorker<G, C> {
        GroupCommitWorker {
            shared_state,
            generator,
            connection,
            num_transactions_per_group,
        }
    }
}

impl<G, C> Worker for GroupCommitWorker<G, C>
where
    G: Generator,
    G::Item: Procedure<C>,
    C: Connection,
{
    fn run(&mut self) {
        while !self.shared_state.is_terminated() {
            let group_id = self.shared_state.group_id();
            let mut group_guards = vec![];
            let mut i = 0;

            self.connection.begin();

            while i < self.num_transactions_per_group {
                let transaction_id = self.shared_state.transaction_id();

                let procedure = self.generator.next();

                self.connection.savepoint();

                match procedure.execute(
                    group_id,
                    transaction_id,
                    &self.shared_state.dibs,
                    &mut self.connection,
                ) {
                    Ok(mut guards) => {
                        group_guards.append(&mut guards);
                        i += 1;
                    },
                    Err(_) => {
                        self.connection.rollback();
                        self.connection.commit();

                        group_guards.clear();

                        self.shared_state.increment_commit_count(i);
                        i = 0;

                        self.connection.begin();
                        self.connection.savepoint();
                    }
                }
            }

            self.connection.commit();

            self.shared_state.increment_commit_count(i);
        }
    }
}
