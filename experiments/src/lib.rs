#![feature(cstring_from_vec_with_nul)]

use dibs::{AcquireError, Dibs, OptimizationLevel, Transaction};
use std::sync::Arc;

pub mod benchmarks;
pub mod runner;
pub mod systems;
pub mod worker;

pub trait AccessType {
    fn is_read_only(&self) -> bool;
}

pub trait Procedure<C> {
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
