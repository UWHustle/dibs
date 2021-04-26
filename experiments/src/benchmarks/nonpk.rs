use crate::{AccessType, Generator, Procedure};
use dibs::predicate::{Predicate, Value};
use dibs::{AcquireError, Dibs, OptimizationLevel, RequestTemplate, Transaction};
use rand::{thread_rng, Rng};
use std::sync::Arc;
use std::time::Duration;
use fnv::FnvHashSet;

pub trait NonPKConnection {
    fn get_pk(&self, non_pk_v: u32) -> u32;
    fn update(&self, pk_v: u32, field_v: u32);
}

pub enum NonPKProcedure {
    UpdateWithPK { pk_v: u32, field_v: u32 },
    UpdateWithNonPK { non_pk_v: u32, field_v: u32 },
}

impl AccessType for NonPKProcedure {
    fn is_read_only(&self) -> bool {
        false
    }
}

impl<C> Procedure<C> for NonPKProcedure
where
    C: NonPKConnection,
{
    fn execute(
        &self,
        dibs: &Option<Arc<Dibs>>,
        transaction: &mut Transaction,
        connection: &mut C,
    ) -> Result<(), AcquireError> {
        match self {
            &NonPKProcedure::UpdateWithPK { pk_v, field_v } => {
                if let Some(d) = dibs {
                    d.acquire(transaction, 0, vec![Value::Integer(pk_v as usize)])?;
                }

                connection.update(pk_v, field_v);
            }
            &NonPKProcedure::UpdateWithNonPK { non_pk_v, field_v } => {
                if let Some(d) = dibs {
                    d.acquire(transaction, 1, vec![Value::Integer(non_pk_v as usize)])?;
                }

                let pk_v = connection.get_pk(non_pk_v);

                if let Some(d) = dibs {
                    d.acquire(transaction, 0, vec![Value::Integer(pk_v as usize)])?;
                }

                connection.update(pk_v, field_v);
            }
        }

        Ok(())
    }
}

pub struct NonPKGenerator {
    num_rows: u32,
    non_pk: f64,
}

impl NonPKGenerator {
    pub fn new(num_rows: u32, non_pk: f64) -> Self {
        assert!(non_pk >= 0.0 && non_pk <= 1.0);
        NonPKGenerator { num_rows, non_pk }
    }
}

impl Generator for NonPKGenerator {
    type Item = NonPKProcedure;

    fn next(&self) -> NonPKProcedure {
        let mut rng = thread_rng();

        let transaction_type = rng.gen::<f64>();

        let k_v = rng.gen_range(0, self.num_rows);
        let field_v = rng.gen();

        if transaction_type < self.non_pk {
            NonPKProcedure::UpdateWithNonPK {
                non_pk_v: k_v,
                field_v,
            }
        } else {
            NonPKProcedure::UpdateWithPK { pk_v: k_v, field_v }
        }
    }
}

pub fn dibs(filter_magnitude: usize) -> Dibs {
    let filters = match filter_magnitude {
        1 => [None],
        _ => [Some(0)],
    };

    let templates = vec![
        // (0) Update using PK
        RequestTemplate::new(
            0,
            [0].iter().copied().collect(),
            [2].iter().copied().collect(),
            Predicate::equality(0, 0),
        ),
        // (1) Get PK
        RequestTemplate::new(
            0,
            (0..=1).collect(),
            FnvHashSet::default(),
            Predicate::equality(1, 0),
        ),
    ];

    Dibs::new(
        &filters,
        &templates,
        match filter_magnitude {
            1 => OptimizationLevel::Prepared,
            _ => OptimizationLevel::Filtered,
        },
        usize::max_value(),
        Duration::from_micros(10),
        filter_magnitude,
    )
}
