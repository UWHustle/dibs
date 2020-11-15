use dibs::{AcquireError, Dibs, OptimizationLevel, Transaction};
use std::sync::Arc;

pub mod benchmarks;
pub mod runner;
pub mod systems;
pub mod worker;

pub trait Procedure<C> {
    fn is_read_only(&self) -> bool;
    fn execute(
        &self,
        dibs: &Option<Arc<Dibs>>,
        transaction: &mut Transaction,
        connection: &mut C,
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
