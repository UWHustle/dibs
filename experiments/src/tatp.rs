use crate::{Client, DibsConnector, OptimizationLevel};
use dibs::predicate::{ComparisonOperator, Predicate, Value};
use dibs::{Dibs, RequestTemplate};
use rand::rngs::ThreadRng;
use rand::Rng;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

pub trait TATPConnection {
    /// Get subscriber data by ID.
    /// ```sql
    /// SELECT *
    /// FROM subscriber
    /// WHERE s_id = ?;
    /// ```
    fn get_subscriber_data(&mut self, s_id: u32) -> ([bool; 10], [u8; 10], [u8; 10], u32, u32);

    /// Get new destination.
    /// ```sql
    /// SELECT cf.numberx
    /// FROM special_facility AS sf, call_forwarding AS cf
    /// WHERE
    ///     (sf.s_id = ?
    ///         AND sf.sf_type = ?
    ///         AND sf.is_active = 1)
    ///     AND (cf.s_id = sf.s_id
    ///         AND cf.sf_type = sf.sf_type)
    ///     AND (cf.start_time <= ?
    ///         AND ? < cf.end_time);
    /// ```
    fn get_new_destination(
        &mut self,
        s_id: u32,
        sf_type: u8,
        start_time: u8,
        end_time: u8,
    ) -> Vec<String>;

    /// Get access data.
    /// ```sql
    /// SELECT data1, data2, data3, data4
    /// FROM access_info
    /// WHERE s_id = ? AND ai_type = ?;
    /// ```
    fn get_access_data(&mut self, s_id: u32, ai_type: u8) -> Option<(u8, u8, String, String)>;

    /// Update subscriber bit.
    /// ```sql
    /// UPDATE subscriber
    /// SET bit_1 = ?
    /// WHERE s_id = ?;
    /// ```
    fn update_subscriber_bit(&mut self, bit_1: bool, s_id: u32);

    /// Update special facility data.
    /// ```sql
    /// UPDATE special_facility
    /// SET data_a = ?
    /// WHERE s_id = ? AND sf_type = ?;
    fn update_special_facility_data(&mut self, data_a: u8, s_id: u32, sf_type: u8);

    /// Update subscriber location.
    /// ```sql
    /// UPDATE subscriber
    /// SET vlr_location = ?
    /// WHERE s_id = ?;
    /// ```
    fn update_subscriber_location(&mut self, vlr_location: u32, s_id: u32);

    /// Get special facility types.
    /// ```sql
    /// SELECT sf_type
    /// FROM special_facility
    /// WHERE s_id = ?;
    /// ```
    fn get_special_facility_types(&mut self, s_id: u32) -> Vec<u8>;

    /// Insert call forwarding.
    /// ```sql
    /// INSERT INTO call_forwarding
    /// VALUES (?, ?, ?, ?, ?);
    /// ```
    fn insert_call_forwarding(
        &mut self,
        s_id: u32,
        sf_type: u8,
        start_time: u8,
        end_time: u8,
        numberx: &str,
    );

    /// Delete call forwarding.
    /// ```sql
    /// DELETE FROM call_forwarding
    /// WHERE s_id = ? AND sf_type = ? AND start_time = ?;
    /// ```
    fn delete_call_forwarding(&mut self, s_id: u32, sf_type: u8, start_time: u8);
}

#[derive(Clone)]
pub struct TATPConfig {
    num_rows: u32,
    a_val: u32,
}

impl TATPConfig {
    pub fn new(num_rows: u32) -> TATPConfig {
        let a_val = if num_rows <= 1000000 {
            65535
        } else if num_rows <= 10000000 {
            1048575
        } else {
            2097151
        };

        TATPConfig { num_rows, a_val }
    }

    pub fn get_num_rows(&self) -> u32 {
        self.num_rows
    }

    fn gen_s_id(&self, rng: &mut ThreadRng) -> u32 {
        (rng.gen_range(0, self.a_val + 1) | rng.gen_range(1, self.num_rows + 1)) % self.num_rows + 1
    }

    fn gen_numberx(&self, rng: &mut ThreadRng) -> String {
        let mut numberx = vec![0; 15];
        let s = rng.gen_range(1, self.num_rows + 1).to_string();
        numberx[(15 - s.len())..].copy_from_slice(s.as_bytes());
        String::from_utf8(numberx).unwrap()
    }
}

pub fn dibs(optimization: OptimizationLevel) -> DibsConnector {
    let filters = match optimization {
        OptimizationLevel::Filtered => &[Some(0), Some(0), Some(0), Some(0)],
        _ => &[None, None, None, None],
    };

    let templates = vec![
        // (0) Get subscriber data.
        RequestTemplate::new(
            0,
            (0..33).collect(),
            HashSet::new(),
            Predicate::comparison(ComparisonOperator::Eq, 0, 0),
        ),
        // (1) Get new destination (special facility).
        RequestTemplate::new(
            2,
            (0..3).collect(),
            HashSet::new(),
            Predicate::conjunction(vec![
                Predicate::comparison(ComparisonOperator::Eq, 0, 0),
                Predicate::comparison(ComparisonOperator::Eq, 1, 1),
            ]),
        ),
        // (2) Get new destination (call forwarding).
        RequestTemplate::new(
            3,
            (0..5).collect(),
            HashSet::new(),
            Predicate::conjunction(vec![
                Predicate::comparison(ComparisonOperator::Eq, 0, 0),
                Predicate::comparison(ComparisonOperator::Eq, 1, 1),
                Predicate::comparison(ComparisonOperator::Le, 2, 2),
                Predicate::comparison(ComparisonOperator::Gt, 3, 3),
            ]),
        ),
        // (3) Get access data.
        RequestTemplate::new(
            1,
            (0..6).collect(),
            HashSet::new(),
            Predicate::conjunction(vec![
                Predicate::comparison(ComparisonOperator::Eq, 0, 0),
                Predicate::comparison(ComparisonOperator::Eq, 1, 1),
            ]),
        ),
        // (4) Update subscriber bit.
        RequestTemplate::new(
            0,
            [0].iter().cloned().collect(),
            [2].iter().cloned().collect(),
            Predicate::comparison(ComparisonOperator::Eq, 0, 0),
        ),
        // (5) Update special facility data.
        RequestTemplate::new(
            2,
            (0..2).collect(),
            [4].iter().cloned().collect(),
            Predicate::conjunction(vec![
                Predicate::comparison(ComparisonOperator::Eq, 0, 0),
                Predicate::comparison(ComparisonOperator::Eq, 1, 1),
            ]),
        ),
        // (6) Update subscriber location.
        RequestTemplate::new(
            0,
            [0].iter().cloned().collect(),
            [32].iter().cloned().collect(),
            Predicate::comparison(ComparisonOperator::Eq, 0, 0),
        ),
        // (7) Get special facility types.
        RequestTemplate::new(
            2,
            (0..2).collect(),
            HashSet::new(),
            Predicate::comparison(ComparisonOperator::Eq, 0, 0),
        ),
        // (8) Insert/delete call forwarding.
        RequestTemplate::new(
            3,
            HashSet::new(),
            (0..5).collect(),
            Predicate::conjunction(vec![
                Predicate::comparison(ComparisonOperator::Eq, 0, 0),
                Predicate::comparison(ComparisonOperator::Eq, 1, 1),
                Predicate::comparison(ComparisonOperator::Eq, 2, 2),
            ]),
        ),
    ];

    let dibs = Dibs::new(
        filters,
        &templates,
        optimization != OptimizationLevel::Ungrouped,
    );

    DibsConnector::new(dibs, optimization, templates, Duration::from_secs(60))
}

pub fn uppercase_alphabetic_string(len: usize, rng: &mut ThreadRng) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    (0..len)
        .map(|_| CHARSET[rng.gen_range(0, CHARSET.len())] as char)
        .collect()
}

pub struct TATPClient<C> {
    config: TATPConfig,
    dibs: Arc<DibsConnector>,
    conn: C,
}

impl<C> TATPClient<C> {
    pub fn new(config: TATPConfig, dibs: Arc<DibsConnector>, conn: C) -> TATPClient<C> {
        TATPClient { config, dibs, conn }
    }
}

impl<C> Client for TATPClient<C>
where
    C: TATPConnection,
{
    fn process(&mut self, transaction_id: usize) {
        let mut rng = rand::thread_rng();

        let transaction_type = rng.gen::<f64>();
        let s_id = self.config.gen_s_id(&mut rng);

        if transaction_type < 0.35 {
            // GET_SUBSCRIBER_DATA
            // Probability: 35%
            let _guard = self
                .dibs
                .acquire(transaction_id, 0, vec![Value::Integer(s_id as usize)]);

            self.conn.get_subscriber_data(s_id);
        } else if transaction_type < 0.45 {
            // GET_NEW_DESTINATION
            // Probability: 10%
            let sf_type = rng.gen_range(1, 5);
            let start_time = rng.gen_range(0, 3) * 8;
            let end_time = rng.gen_range(1, 25);

            let _guard_sf = self.dibs.acquire(
                transaction_id,
                1,
                vec![
                    Value::Integer(s_id as usize),
                    Value::Integer(sf_type as usize),
                ],
            );

            let _guard_cf = self.dibs.acquire(
                transaction_id,
                2,
                vec![
                    Value::Integer(s_id as usize),
                    Value::Integer(sf_type as usize),
                    Value::Integer(start_time as usize),
                    Value::Integer(end_time as usize),
                ],
            );

            self.conn
                .get_new_destination(s_id, sf_type, start_time, end_time);
        } else if transaction_type < 0.80 {
            // GET_ACCESS_DATA
            // Probability: 35%
            let ai_type = rng.gen_range(1, 5);

            let _guard = self.dibs.acquire(
                transaction_id,
                3,
                vec![
                    Value::Integer(s_id as usize),
                    Value::Integer(ai_type as usize),
                ],
            );

            self.conn.get_access_data(s_id, ai_type);
        } else if transaction_type < 0.82 {
            // UPDATE_SUBSCRIBER_DATA
            // Probability: 2%
            let bit_1 = rng.gen();
            let data_a = rng.gen();
            let sf_type = rng.gen_range(1, 5);

            let _guard_s =
                self.dibs
                    .acquire(transaction_id, 4, vec![Value::Integer(s_id as usize)]);

            let _guard_sf = self.dibs.acquire(
                transaction_id,
                5,
                vec![
                    Value::Integer(s_id as usize),
                    Value::Integer(sf_type as usize),
                ],
            );

            self.conn.update_subscriber_bit(bit_1, s_id);
            self.conn
                .update_special_facility_data(data_a, s_id, sf_type);
        } else if transaction_type < 0.96 {
            // UPDATE_LOCATION
            // Probability: 14%
            let vlr_location = rng.gen();

            let _guard = self
                .dibs
                .acquire(transaction_id, 6, vec![Value::Integer(s_id as usize)]);

            self.conn.update_subscriber_location(vlr_location, s_id);
        } else if transaction_type < 0.98 {
            // INSERT_CALL_FORWARDING
            // Probability: 2%
            let sf_type = rng.gen_range(1, 5);
            let start_time = rng.gen_range(0, 3) * 8;
            let end_time = rng.gen_range(1, 25);
            let numberx = self.config.gen_numberx(&mut rng);

            let _guard_sf =
                self.dibs
                    .acquire(transaction_id, 7, vec![Value::Integer(s_id as usize)]);

            let _guard_cf = self.dibs.acquire(
                transaction_id,
                8,
                vec![
                    Value::Integer(s_id as usize),
                    Value::Integer(sf_type as usize),
                    Value::Integer(start_time as usize),
                ],
            );

            self.conn.get_special_facility_types(s_id);
            self.conn
                .insert_call_forwarding(s_id, sf_type, start_time, end_time, &numberx);
        } else {
            // DELETE_CALL_FORWARDING
            // Probability: 2%
            let sf_type = rng.gen_range(1, 5);
            let start_time = rng.gen_range(0, 3) * 8;

            let _guard = self.dibs.acquire(
                transaction_id,
                8,
                vec![
                    Value::Integer(s_id as usize),
                    Value::Integer(sf_type as usize),
                    Value::Integer(start_time as usize),
                ],
            );

            self.conn.delete_call_forwarding(s_id, sf_type, start_time);
        }
    }
}

unsafe impl<C> Send for TATPClient<C> {}
