use dibs::{AcquireError, Dibs, OptimizationLevel, RequestGuard};

pub mod benchmarks;
pub mod runner;
pub mod systems;
pub mod worker;

pub trait Procedure<C> {
    fn is_read_only(&self) -> bool;
    fn execute(
        &self,
        group_id: usize,
        transaction_id: usize,
        dibs: &Dibs,
        connection: &mut C,
        guards: &mut Vec<RequestGuard>,
    ) -> Result<(), AcquireError>;
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
