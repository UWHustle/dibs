use crate::{Generator, OptimizationLevel, Procedure};
use dibs::predicate::{ComparisonOperator, Predicate, Value};
use dibs::{AcquireError, Dibs, RequestGuard, RequestTemplate};
use rand::{thread_rng, Rng};
use std::collections::HashSet;
use std::time::Duration;

const NUM_FIELDS: usize = 10;

pub trait YCSBConnection {
    /// Get user.
    /// ```sql
    /// SELECT field
    /// FROM users
    /// WHERE id = ?;
    /// ```
    fn select_user(&mut self, field: usize, user_id: u32) -> Vec<u8>;

    /// Update user.
    /// ```sql
    /// UPDATE users
    /// SET field = ?
    /// WHERE id = ?;
    /// ```
    fn update_user(&mut self, field: usize, data: &[u8], user_id: u32);
}

pub enum YCSBProcedure {
    SelectUser {
        field: usize,
        user_id: u32,
    },
    UpdateUser {
        field: usize,
        data: Vec<u8>,
        user_id: u32,
    },
}

impl<C: YCSBConnection> Procedure<C> for YCSBProcedure {
    fn is_read_only(&self) -> bool {
        match self {
            YCSBProcedure::SelectUser { .. } => true,
            YCSBProcedure::UpdateUser { .. } => false,
        }
    }

    fn execute(
        &self,
        group_id: usize,
        transaction_id: usize,
        dibs: &Dibs,
        connection: &mut C,
    ) -> Result<Vec<RequestGuard>, AcquireError> {
        match self {
            YCSBProcedure::SelectUser { field, user_id } => {
                let guard = dibs.acquire(
                    group_id,
                    transaction_id,
                    *field,
                    vec![Value::Integer(*user_id as usize)],
                )?;

                connection.select_user(*field, *user_id);

                Ok(vec![guard])
            }
            YCSBProcedure::UpdateUser {
                field,
                data,
                user_id,
            } => {
                let guard = dibs.acquire(
                    group_id,
                    transaction_id,
                    NUM_FIELDS + *field,
                    vec![Value::Integer(*user_id as usize)],
                )?;

                connection.update_user(*field, data, *user_id);

                Ok(vec![guard])
            }
        }
    }
}

pub struct YCSBGenerator {
    num_rows: u32,
    field_size: usize,
    select_mix: f64,
}

impl YCSBGenerator {
    pub fn new(num_rows: u32, field_size: usize, select_mix: f64) -> YCSBGenerator {
        YCSBGenerator {
            num_rows,
            field_size,
            select_mix,
        }
    }
}

impl Generator for YCSBGenerator {
    type Item = YCSBProcedure;

    fn next(&self) -> YCSBProcedure {
        let mut rng = thread_rng();

        let transaction_type = rng.gen::<f64>();
        let field = rng.gen_range(0, NUM_FIELDS);
        let user_id = rng.gen_range(0, self.num_rows);

        if transaction_type < self.select_mix {
            YCSBProcedure::SelectUser { field, user_id }
        } else {
            let data = (0..self.field_size).map(|_| rng.gen()).collect::<Vec<_>>();
            YCSBProcedure::UpdateUser {
                field,
                data,
                user_id,
            }
        }
    }
}

pub fn dibs(num_fields: usize, optimization: OptimizationLevel) -> Dibs {
    let filters = match optimization {
        OptimizationLevel::Filtered => &[Some(0)],
        _ => &[None],
    };

    let templates = (0..num_fields)
        .map(|field| {
            // (0..num_fields) Get user.
            RequestTemplate::new(
                0,
                [field].iter().cloned().collect(),
                HashSet::new(),
                Predicate::comparison(ComparisonOperator::Eq, 0, 0),
            )
        })
        .chain((0..num_fields).map(|field| {
            // (num_fields..2*num_fields) Update user.
            RequestTemplate::new(
                0,
                HashSet::new(),
                [field].iter().cloned().collect(),
                Predicate::comparison(ComparisonOperator::Eq, 0, 0),
            )
        }))
        .collect::<Vec<_>>();

    Dibs::new(filters, &templates, optimization, Duration::from_secs(60))
}
