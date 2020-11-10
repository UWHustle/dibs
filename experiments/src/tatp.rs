use crate::{Generator, Procedure};
use dibs::predicate::{ComparisonOperator, Predicate, Value};
use dibs::{AcquireError, Dibs, OptimizationLevel, RequestGuard, RequestTemplate};
use rand::rngs::ThreadRng;
use rand::{thread_rng, Rng};
use std::collections::HashSet;
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

pub enum TATPProcedure {
    GetSubscriberData {
        s_id: u32,
    },
    GetNewDestination {
        s_id: u32,
        sf_type: u8,
        start_time: u8,
        end_time: u8,
    },
    GetAccessData {
        s_id: u32,
        ai_type: u8,
    },
    UpdateSubscriberData {
        bit_1: bool,
        s_id: u32,
        data_a: u8,
        sf_type: u8,
    },
    UpdateLocation {
        vlr_location: u32,
        s_id: u32,
    },
    InsertCallForwarding {
        s_id: u32,
        sf_type: u8,
        start_time: u8,
        end_time: u8,
        numberx: String,
    },
    DeleteCallForwarding {
        s_id: u32,
        sf_type: u8,
        start_time: u8,
    },
}

impl<C: TATPConnection> Procedure<C> for TATPProcedure {
    fn is_read_only(&self) -> bool {
        match self {
            TATPProcedure::GetSubscriberData { .. }
            | TATPProcedure::GetNewDestination { .. }
            | TATPProcedure::GetAccessData { .. } => true,
            TATPProcedure::UpdateSubscriberData { .. }
            | TATPProcedure::UpdateLocation { .. }
            | TATPProcedure::InsertCallForwarding { .. }
            | TATPProcedure::DeleteCallForwarding { .. } => false,
        }
    }

    fn execute(
        &self,
        group_id: usize,
        transaction_id: usize,
        dibs: &Dibs,
        connection: &mut C,
    ) -> Result<Vec<RequestGuard>, AcquireError> {
        let mut guards = vec![];

        match self {
            TATPProcedure::GetSubscriberData { s_id } => {
                guards.push(dibs.acquire(
                    group_id,
                    transaction_id,
                    0,
                    vec![Value::Integer(*s_id as usize)],
                )?);

                connection.get_subscriber_data(*s_id);
            }

            TATPProcedure::GetNewDestination {
                s_id,
                sf_type,
                start_time,
                end_time,
            } => {
                guards.push(dibs.acquire(
                    group_id,
                    transaction_id,
                    1,
                    vec![
                        Value::Integer(*s_id as usize),
                        Value::Integer(*sf_type as usize),
                    ],
                )?);

                guards.push(dibs.acquire(
                    group_id,
                    transaction_id,
                    2,
                    vec![
                        Value::Integer(*s_id as usize),
                        Value::Integer(*sf_type as usize),
                        Value::Integer(*start_time as usize),
                        Value::Integer(*end_time as usize),
                    ],
                )?);

                connection.get_new_destination(*s_id, *sf_type, *start_time, *end_time);
            }

            TATPProcedure::GetAccessData { s_id, ai_type } => {
                guards.push(dibs.acquire(
                    group_id,
                    transaction_id,
                    3,
                    vec![
                        Value::Integer(*s_id as usize),
                        Value::Integer(*ai_type as usize),
                    ],
                )?);

                connection.get_access_data(*s_id, *ai_type);
            }

            TATPProcedure::UpdateSubscriberData {
                bit_1,
                s_id,
                data_a,
                sf_type,
            } => {
                guards.push(dibs.acquire(
                    group_id,
                    transaction_id,
                    4,
                    vec![Value::Integer(*s_id as usize)],
                )?);

                guards.push(dibs.acquire(
                    group_id,
                    transaction_id,
                    5,
                    vec![
                        Value::Integer(*s_id as usize),
                        Value::Integer(*sf_type as usize),
                    ],
                )?);

                connection.update_subscriber_bit(*bit_1, *s_id);
                connection.update_special_facility_data(*data_a, *s_id, *sf_type);
            }
            TATPProcedure::UpdateLocation { vlr_location, s_id } => {
                guards.push(dibs.acquire(
                    group_id,
                    transaction_id,
                    6,
                    vec![Value::Integer(*s_id as usize)],
                )?);

                connection.update_subscriber_location(*vlr_location, *s_id);
            }
            TATPProcedure::InsertCallForwarding {
                s_id,
                sf_type,
                start_time,
                end_time,
                numberx,
            } => {
                guards.push(dibs.acquire(
                    group_id,
                    transaction_id,
                    7,
                    vec![Value::Integer(*s_id as usize)],
                )?);

                guards.push(dibs.acquire(
                    group_id,
                    transaction_id,
                    8,
                    vec![
                        Value::Integer(*s_id as usize),
                        Value::Integer(*sf_type as usize),
                        Value::Integer(*start_time as usize),
                    ],
                )?);

                connection.get_special_facility_types(*s_id);
                connection.insert_call_forwarding(*s_id, *sf_type, *start_time, *end_time, numberx);
            }
            TATPProcedure::DeleteCallForwarding {
                s_id,
                sf_type,
                start_time,
            } => {
                guards.push(dibs.acquire(
                    group_id,
                    transaction_id,
                    8,
                    vec![
                        Value::Integer(*s_id as usize),
                        Value::Integer(*sf_type as usize),
                        Value::Integer(*start_time as usize),
                    ],
                )?);

                connection.delete_call_forwarding(*s_id, *sf_type, *start_time);
            }
        }

        Ok(guards)
    }
}

pub struct TATPGenerator {
    num_rows: u32,
    a_val: u32,
}

impl TATPGenerator {
    pub fn new(num_rows: u32) -> TATPGenerator {
        let a_val = if num_rows <= 1000000 {
            65535
        } else if num_rows <= 10000000 {
            1048575
        } else {
            2097151
        };

        TATPGenerator { num_rows, a_val }
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

impl Generator<TATPProcedure> for TATPGenerator {
    fn next(&self) -> TATPProcedure {
        let mut rng = thread_rng();

        let transaction_type = rng.gen::<f64>();
        let s_id = self.gen_s_id(&mut rng);

        if transaction_type < 0.35 {
            TATPProcedure::GetSubscriberData { s_id }
        } else if transaction_type < 0.45 {
            let sf_type = rng.gen_range(1, 5);
            let start_time = rng.gen_range(0, 3) * 8;
            let end_time = rng.gen_range(1, 25);

            TATPProcedure::GetNewDestination {
                s_id,
                sf_type,
                start_time,
                end_time,
            }
        } else if transaction_type < 0.80 {
            let ai_type = rng.gen_range(1, 5);

            TATPProcedure::GetAccessData { s_id, ai_type }
        } else if transaction_type < 0.82 {
            let bit_1 = rng.gen();
            let data_a = rng.gen();
            let sf_type = rng.gen_range(1, 5);

            TATPProcedure::UpdateSubscriberData {
                bit_1,
                s_id,
                data_a,
                sf_type,
            }
        } else if transaction_type < 0.96 {
            let vlr_location = rng.gen();

            TATPProcedure::UpdateLocation { vlr_location, s_id }
        } else if transaction_type < 0.98 {
            let sf_type = rng.gen_range(1, 5);
            let start_time = rng.gen_range(0, 3) * 8;
            let end_time = rng.gen_range(1, 25);
            let numberx = self.gen_numberx(&mut rng);

            TATPProcedure::InsertCallForwarding {
                s_id,
                sf_type,
                start_time,
                end_time,
                numberx,
            }
        } else {
            let sf_type = rng.gen_range(1, 5);
            let start_time = rng.gen_range(0, 3) * 8;

            TATPProcedure::DeleteCallForwarding {
                s_id,
                sf_type,
                start_time,
            }
        }
    }
}

pub fn dibs(optimization: OptimizationLevel) -> Dibs {
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

    Dibs::new(filters, &templates, optimization, Duration::from_secs(60))
}

pub fn uppercase_alphabetic_string(len: usize, rng: &mut ThreadRng) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    (0..len)
        .map(|_| CHARSET[rng.gen_range(0, CHARSET.len())] as char)
        .collect()
}
