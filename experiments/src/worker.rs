use crate::{Connection, Generator, Procedure};
use dibs::{Dibs, Transaction};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError};
use std::sync::Arc;

struct State {
    group_counter: usize,
    transaction_counter: usize,
    dibs: Arc<Dibs>,
}

impl State {
    fn new(worker_id: usize, dibs: Arc<Dibs>) -> State {
        assert!(worker_id < 1024);
        let counter = worker_id * usize::max_value() / 1024;

        State {
            group_counter: counter,
            transaction_counter: counter,
            dibs,
        }
    }

    fn group_id(&mut self) -> usize {
        State::fetch_inc(&mut self.group_counter)
    }

    fn transaction_id(&mut self) -> usize {
        State::fetch_inc(&mut self.transaction_counter)
    }

    fn fetch_inc(x: &mut usize) -> usize {
        let prev = *x;
        *x += 1;
        prev
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
    fn run(&mut self, commits: Arc<AtomicUsize>, terminate: Arc<AtomicBool>);
}

pub struct StandardWorker<G, C> {
    state: State,
    generator: G,
    connection: C,
}

impl<G, C> StandardWorker<G, C> {
    pub fn new(
        worker_id: usize,
        dibs: Arc<Dibs>,
        generator: G,
        connection: C,
    ) -> StandardWorker<G, C> {
        StandardWorker {
            state: State::new(worker_id, dibs),
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
    fn run(&mut self, commits: Arc<AtomicUsize>, terminate: Arc<AtomicBool>) {
        while !terminate.load(Ordering::Relaxed) {
            let mut transaction =
                Transaction::new(self.state.group_id(), self.state.transaction_id());

            let procedure = self.generator.next();

            self.connection.begin();

            loop {
                let result =
                    procedure.execute(&self.state.dibs, &mut transaction, &mut self.connection);

                if result.is_ok() {
                    break;
                }
            }

            self.connection.commit();

            transaction.commit();

            commits.fetch_add(1, Ordering::Relaxed);
        }
    }
}

unsafe impl<G, C> Send for StandardWorker<G, C> {}

pub struct GroupCommitWorker<G, C> {
    state: State,
    generator: G,
    connection: C,
    num_transactions_per_group: usize,
}

impl<G, C> GroupCommitWorker<G, C> {
    pub fn new(
        worker_id: usize,
        dibs: Arc<Dibs>,
        generator: G,
        connection: C,
        num_transactions_per_group: usize,
    ) -> GroupCommitWorker<G, C> {
        GroupCommitWorker {
            state: State::new(worker_id, dibs),
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
    fn run(&mut self, commits: Arc<AtomicUsize>, terminate: Arc<AtomicBool>) {
        while !terminate.load(Ordering::Relaxed) {
            let mut transactions = vec![];

            let group_id = self.state.group_id();
            let mut i = 0;

            self.connection.begin();

            while i < self.num_transactions_per_group {
                transactions.push(Transaction::new(group_id, self.state.transaction_id()));

                let procedure = self.generator.next();

                self.connection.savepoint();

                match procedure.execute(
                    &self.state.dibs,
                    transactions.last_mut().unwrap(),
                    &mut self.connection,
                ) {
                    Ok(_) => {
                        i += 1;
                    }
                    Err(_) => {
                        self.connection.rollback();
                        self.connection.commit();

                        for transaction in transactions.drain(..) {
                            transaction.commit();
                        }

                        commits.fetch_add(i, Ordering::Relaxed);
                        i = 0;

                        self.connection.begin();
                        self.connection.savepoint();
                    }
                }
            }

            self.connection.commit();

            for transaction in transactions.drain(..) {
                transaction.commit();
            }

            commits.fetch_add(i, Ordering::Relaxed);
        }
    }
}
