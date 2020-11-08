use crate::{Client, DibsConnector, OptimizationLevel};
use dibs::predicate::{ComparisonOperator, Predicate, Value};
use dibs::{Dibs, RequestTemplate};
use rand::Rng;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

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

#[derive(Clone)]
pub struct YCSBConfig {
    num_rows: u32,
    num_fields: usize,
    field_size: usize,
    select_mix: f64,
}

impl YCSBConfig {
    pub fn new(num_rows: u32, num_fields: usize, field_size: usize, select_mix: f64) -> YCSBConfig {
        assert!(select_mix >= 0.0 && select_mix <= 1.0);

        YCSBConfig {
            num_rows,
            num_fields,
            field_size,
            select_mix,
        }
    }
}

pub fn dibs(num_fields: usize, optimization: OptimizationLevel) -> DibsConnector {
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

    let dibs = Dibs::new(
        filters,
        &templates,
        optimization != OptimizationLevel::Ungrouped,
    );

    DibsConnector::new(dibs, optimization, templates, Duration::from_secs(60))
}

pub struct YCSBClient<C> {
    config: YCSBConfig,
    dibs: Arc<DibsConnector>,
    conn: C,
}

impl<C> YCSBClient<C> {
    pub fn new(config: YCSBConfig, dibs: Arc<DibsConnector>, conn: C) -> YCSBClient<C> {
        YCSBClient { config, dibs, conn }
    }
}

impl<C> Client for YCSBClient<C>
where
    C: YCSBConnection,
{
    fn process(&mut self, transaction_id: usize) {
        let mut rng = rand::thread_rng();

        let transaction_type = rng.gen::<f64>();
        let field = rng.gen_range(0, self.config.num_fields);
        let user_id = rng.gen_range(0, self.config.num_rows);

        if transaction_type < self.config.select_mix {
            // Select user.
            let _guard = self.dibs.acquire(
                transaction_id,
                field,
                vec![Value::Integer(user_id as usize)],
            );

            self.conn.select_user(field, user_id);
        } else {
            // Update user.
            let data = (0..self.config.field_size)
                .map(|_| rng.gen())
                .collect::<Vec<_>>();

            let _guard = self.dibs.acquire(
                transaction_id,
                self.config.num_fields + field,
                vec![Value::Integer(user_id as usize)],
            );

            self.conn.update_user(field, &data, user_id);
        }
    }
}

unsafe impl<C> Send for YCSBClient<C> {}
