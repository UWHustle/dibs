use dibs::{AcquireError, Dibs, OptimizationLevel, RequestGuard};

pub mod arrow_server;
pub mod runner;
pub mod scan;
pub mod sqlite_server;
pub mod tatp;
pub mod worker;
pub mod ycsb;

pub trait Procedure<C> {
    fn is_read_only(&self) -> bool;
    fn execute(
        &self,
        group_id: usize,
        transaction_id: usize,
        dibs: &Dibs,
        connection: &mut C,
    ) -> Result<Vec<RequestGuard>, AcquireError>;
}

pub trait Generator {
    type Item;
    fn next(&self) -> Self::Item;
}

pub trait Statement {
    fn is_read_only(&self) -> bool;
}

pub trait Connection {
    fn begin(&mut self);
    fn commit(&mut self);
    fn rollback(&mut self);
    fn savepoint(&mut self);
}
