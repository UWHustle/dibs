use crate::Server;
use dibs::predicate::{ComparisonOperator, Predicate, Value};
use dibs::{Dibs, Request, RequestGuard, RequestTemplate, RequestVariant};
use rand::rngs::ThreadRng;
use rand::Rng;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub trait TATPServer {
    /// Get subscriber data by ID.
    /// ```sql
    /// SELECT *
    /// FROM subscriber
    /// WHERE s_id = ?;
    /// ```
    fn get_subscriber_data(&self, s_id: u32) -> ([bool; 10], [u8; 10], [u8; 10], u32, u32);

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
        &self,
        s_id: u32,
        sf_type: u8,
        start_time: u8,
        end_time: u8,
    ) -> Vec<[u8; 15]>;

    /// Get access data.
    /// ```sql
    /// SELECT data1, data2, data3, data4
    /// FROM access_info
    /// WHERE s_id = ? AND ai_type = ?;
    /// ```
    fn get_access_data(&self, s_id: u32, ai_type: u8) -> Option<(u8, u8, [u8; 3], [u8; 5])>;

    /// Update subscriber bit.
    /// ```sql
    /// UPDATE subscriber
    /// SET bit_1 = ?
    /// WHERE s_id = ?;
    /// ```
    fn update_subscriber_bit(&self, bit_1: bool, s_id: u32);

    /// Update special facility data.
    /// ```sql
    /// UPDATE special_facility
    /// SET data_a = ?
    /// WHERE s_id = ? AND sf_type = ?
    fn update_special_facility_data(&self, data_a: u8, s_id: u32, sf_type: u8);

    /// Update subscriber location.
    /// ```sql
    /// UPDATE subscriber
    /// SET vlr_location = ?
    /// WHERE s_id = ?;
    /// ```
    fn update_subscriber_location(&self, vlr_location: u32, s_id: u32);

    /// Get special facility types.
    /// ```sql
    /// SELECT sf_type
    /// FROM special_facility
    /// WHERE s_id = ?;
    /// ```
    fn get_special_facility_types(&self, s_id: u32) -> Vec<u8>;

    /// Insert call forwarding.
    /// ```sql
    /// INSERT INTO call_forwarding
    /// VALUES (?, ?, ?, ?, ?);
    /// ```
    fn insert_call_forwarding(
        &self,
        s_id: u32,
        sf_type: u8,
        start_time: u8,
        end_time: u8,
        numberx: [u8; 15],
    );

    /// Delete call forwarding.
    /// ```sql
    /// DELETE FROM call_forwarding
    /// WHERE s_id = ? AND sf_type = ? AND start_time = ?;
    /// ```
    fn delete_call_forwarding(&self, s_id: u32, sf_type: u8, start_time: u8);

    /// Get subscriber data scan (not part of official TATP).
    /// ```sql
    /// SELECT *
    /// FROM subscriber
    /// WHERE ((byte2_1 BETWEEN ? AND ?) OR (byte2_1 BETWEEN ? AND ?))
    ///     AND ((byte2_2 BETWEEN ? AND ?) OR (byte2_2 BETWEEN ? AND ?))
    ///     AND ((byte2_3 BETWEEN ? AND ?) OR (byte2_3 BETWEEN ? AND ?))
    ///     AND ((byte2_4 BETWEEN ? AND ?) OR (byte2_4 BETWEEN ? AND ?))
    ///     AND ((byte2_5 BETWEEN ? AND ?) OR (byte2_5 BETWEEN ? AND ?))
    ///     AND ((byte2_6 BETWEEN ? AND ?) OR (byte2_6 BETWEEN ? AND ?))
    ///     AND ((byte2_7 BETWEEN ? AND ?) OR (byte2_7 BETWEEN ? AND ?))
    ///     AND ((byte2_8 BETWEEN ? AND ?) OR (byte2_8 BETWEEN ? AND ?))
    ///     AND ((byte2_9 BETWEEN ? AND ?) OR (byte2_9 BETWEEN ? AND ?))
    ///     AND ((byte2_10 BETWEEN ? AND ?) OR (byte2_10 BETWEEN ? AND ?))
    /// ```
    fn get_subscriber_data_scan(
        &self,
        byte2: [(u8, u8, u8, u8); 10],
    ) -> Vec<([bool; 10], [u8; 10], [u8; 10], u32, u32)>;

    /// Update subscriber bit scan (not part of official TATP).
    /// ```sql
    /// UPDATE subscriber
    /// SET bit_1 = ?
    /// WHERE ((byte2_1 BETWEEN ? AND ?) OR (byte2_1 BETWEEN ? AND ?))
    ///     AND ((byte2_2 BETWEEN ? AND ?) OR (byte2_2 BETWEEN ? AND ?))
    ///     AND ((byte2_3 BETWEEN ? AND ?) OR (byte2_3 BETWEEN ? AND ?))
    ///     AND ((byte2_4 BETWEEN ? AND ?) OR (byte2_4 BETWEEN ? AND ?))
    ///     AND ((byte2_5 BETWEEN ? AND ?) OR (byte2_5 BETWEEN ? AND ?))
    ///     AND ((byte2_6 BETWEEN ? AND ?) OR (byte2_6 BETWEEN ? AND ?))
    ///     AND ((byte2_7 BETWEEN ? AND ?) OR (byte2_7 BETWEEN ? AND ?))
    ///     AND ((byte2_8 BETWEEN ? AND ?) OR (byte2_8 BETWEEN ? AND ?))
    ///     AND ((byte2_9 BETWEEN ? AND ?) OR (byte2_9 BETWEEN ? AND ?))
    ///     AND ((byte2_10 BETWEEN ? AND ?) OR (byte2_10 BETWEEN ? AND ?))
    /// ```
    fn update_subscriber_bit_scan(&self, bit_1: bool, byte2: [(u8, u8, u8, u8); 10]);

    /// Update subscriber location scan (not part of official TATP).
    /// ```sql
    /// UPDATE subscriber
    /// SET vlr_location = ?
    /// WHERE ((byte2_1 BETWEEN ? AND ?) OR (byte2_1 BETWEEN ? AND ?))
    ///     AND ((byte2_2 BETWEEN ? AND ?) OR (byte2_2 BETWEEN ? AND ?))
    ///     AND ((byte2_3 BETWEEN ? AND ?) OR (byte2_3 BETWEEN ? AND ?))
    ///     AND ((byte2_4 BETWEEN ? AND ?) OR (byte2_4 BETWEEN ? AND ?))
    ///     AND ((byte2_5 BETWEEN ? AND ?) OR (byte2_5 BETWEEN ? AND ?))
    ///     AND ((byte2_6 BETWEEN ? AND ?) OR (byte2_6 BETWEEN ? AND ?))
    ///     AND ((byte2_7 BETWEEN ? AND ?) OR (byte2_7 BETWEEN ? AND ?))
    ///     AND ((byte2_8 BETWEEN ? AND ?) OR (byte2_8 BETWEEN ? AND ?))
    ///     AND ((byte2_9 BETWEEN ? AND ?) OR (byte2_9 BETWEEN ? AND ?))
    ///     AND ((byte2_10 BETWEEN ? AND ?) OR (byte2_10 BETWEEN ? AND ?))
    /// ```
    fn update_subscriber_location_scan(&self, vlr_location: u32, byte2: [(u8, u8, u8, u8); 10]);
}

fn scan_predicate(num_conjuncts: usize) -> Predicate {
    assert!(num_conjuncts <= 10);

    Predicate::conjunction(
        (0..num_conjuncts)
            .map(|i| {
                Predicate::disjunction(vec![
                    Predicate::conjunction(vec![
                        Predicate::comparison(ComparisonOperator::Ge, i + 21, i * 4),
                        Predicate::comparison(ComparisonOperator::Le, i + 21, i * 4 + 1),
                    ]),
                    Predicate::conjunction(vec![
                        Predicate::comparison(ComparisonOperator::Ge, i + 21, i * 4 + 2),
                        Predicate::comparison(ComparisonOperator::Le, i + 21, i * 4 + 3),
                    ]),
                ])
            })
            .collect(),
    )
}

pub struct TATPDibs {
    templates: [RequestTemplate; 12],
    inner: Dibs,
}

impl TATPDibs {
    pub fn new(config: &TATPConfig) -> TATPDibs {
        let filters = &[Some(0), Some(0), Some(0), Some(0)];

        let templates = [
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
            // (9) Get subscriber data scan.
            RequestTemplate::new(
                0,
                (0..33).collect(),
                HashSet::new(),
                scan_predicate(config.num_conjuncts),
            ),
            // (10) Update subscriber bit scan.
            RequestTemplate::new(
                0,
                (21..31).collect(),
                [2].iter().cloned().collect(),
                scan_predicate(config.num_conjuncts),
            ),
            // (11) Update subscriber location scan.
            RequestTemplate::new(
                0,
                (21..31).collect(),
                [32].iter().cloned().collect(),
                scan_predicate(config.num_conjuncts),
            ),
        ];

        let inner = Dibs::new(filters, &templates);

        TATPDibs { templates, inner }
    }

    pub fn acquire(&self, request: Request, timeout: Duration) -> Option<RequestGuard> {
        self.inner.acquire(request, timeout)
    }
}

#[derive(Clone)]
pub struct TATPConfig {
    num_rows: u32,
    a_val: u32,
    mix: Vec<f64>,
    scan_range: u8,
    num_conjuncts: usize,
}

impl TATPConfig {
    pub fn new(num_rows: u32, mix: [f64; 10], scan_range: u8, num_conjuncts: usize) -> TATPConfig {
        let a_val = if num_rows <= 1000000 {
            65535
        } else if num_rows <= 10000000 {
            1048575
        } else {
            2097151
        };

        let mix = mix
            .iter()
            .scan(0.0, |state, &x| {
                *state = *state + x;
                Some(*state)
            })
            .collect::<Vec<_>>();

        assert_eq!(*mix.last().unwrap(), 1.0);

        TATPConfig {
            num_rows,
            a_val,
            mix,
            scan_range,
            num_conjuncts,
        }
    }

    pub fn get_num_rows(&self) -> u32 {
        self.num_rows
    }

    fn gen_s_id(&self, rng: &mut ThreadRng) -> u32 {
        (rng.gen_range(0, self.a_val + 1) | rng.gen_range(1, self.num_rows + 1)) % self.num_rows + 1
    }

    fn gen_numberx(&self, rng: &mut ThreadRng) -> [u8; 15] {
        let mut numberx = [0; 15];
        let s = rng.gen_range(1, self.num_rows + 1).to_string();
        numberx[(15 - s.len())..].copy_from_slice(s.as_bytes());
        numberx
    }

    fn gen_byte2(&self, rng: &mut ThreadRng) -> [(u8, u8, u8, u8); 10] {
        let mut arguments = [(0, 0, 0, 0); 10];

        for argument in &mut arguments {
            argument.0 = rng.gen_range(0, u8::max_value() - self.scan_range);
            argument.1 = argument.0 + self.scan_range;
            argument.2 = rng.gen_range(0, u8::max_value() - self.scan_range);
            argument.3 = argument.2 + self.scan_range;
        }

        arguments
    }
}

fn byte2_to_arguments(byte2: &[(u8, u8, u8, u8); 10]) -> Vec<Value> {
    let mut arguments = Vec::with_capacity(40);

    for b in byte2 {
        for v in &[b.0, b.1, b.2, b.3] {
            arguments.push(Value::Integer(*v as usize))
        }
    }

    arguments
}

pub struct TATPClient<S> {
    config: TATPConfig,
    dibs: Arc<TATPDibs>,
    server: Arc<S>,
    transaction_counter: Arc<AtomicUsize>,
    terminate: Arc<AtomicBool>,
    ad_hoc: bool,
    timeout: Duration,
}

impl<S> TATPClient<S> {
    pub fn new(
        config: TATPConfig,
        dibs: Arc<TATPDibs>,
        server: Arc<S>,
        transaction_counter: Arc<AtomicUsize>,
        terminate: Arc<AtomicBool>,
        ad_hoc: bool,
    ) -> TATPClient<S> {
        TATPClient {
            config,
            dibs,
            server,
            transaction_counter,
            terminate,
            ad_hoc,
            timeout: Duration::from_secs(100),
        }
    }
}

impl<S> TATPClient<S>
where
    S: Server + TATPServer,
{
    pub fn run(&self) {
        let mut rng = rand::thread_rng();

        while !self.terminate.load(Ordering::Relaxed) {
            let transaction_id = self.transaction_counter.fetch_add(1, Ordering::Relaxed);

            let transaction_type = rng.gen::<f64>();
            let s_id = self.config.gen_s_id(&mut rng);

            if transaction_type < self.config.mix[0] {
                // GET_SUBSCRIBER_DATA
                let _guard = self.acquire(transaction_id, 0, vec![Value::Integer(s_id as usize)]);
                self.server.get_subscriber_data(s_id);
            } else if transaction_type < self.config.mix[1] {
                // GET_NEW_DESTINATION
                let sf_type = rng.gen_range(1, 5);
                let start_time = rng.gen_range(0, 3) * 8;
                let end_time = rng.gen_range(1, 25);

                let _guard_sf = self.acquire(
                    transaction_id,
                    1,
                    vec![
                        Value::Integer(s_id as usize),
                        Value::Integer(sf_type as usize),
                    ],
                );

                let _guard_cf = self.acquire(
                    transaction_id,
                    2,
                    vec![
                        Value::Integer(s_id as usize),
                        Value::Integer(sf_type as usize),
                        Value::Integer(start_time as usize),
                        Value::Integer(end_time as usize),
                    ],
                );

                self.server
                    .get_new_destination(s_id, sf_type, start_time, end_time);
            } else if transaction_type < self.config.mix[2] {
                // GET_ACCESS_DATA
                let ai_type = rng.gen_range(1, 5);

                let _guard = self.acquire(
                    transaction_id,
                    3,
                    vec![
                        Value::Integer(s_id as usize),
                        Value::Integer(ai_type as usize),
                    ],
                );

                self.server.get_access_data(s_id, ai_type);
            } else if transaction_type < self.config.mix[3] {
                // UPDATE_SUBSCRIBER_DATA
                let bit_1 = rng.gen();
                let data_a = rng.gen();
                let sf_type = rng.gen_range(1, 5);

                let _guard_s = self.acquire(transaction_id, 4, vec![Value::Integer(s_id as usize)]);

                let _guard_sf = self.acquire(
                    transaction_id,
                    5,
                    vec![
                        Value::Integer(s_id as usize),
                        Value::Integer(sf_type as usize),
                    ],
                );

                self.server.update_subscriber_bit(bit_1, s_id);
                self.server
                    .update_special_facility_data(data_a, s_id, sf_type);
            } else if transaction_type < self.config.mix[4] {
                // UPDATE_LOCATION
                let vlr_location = rng.gen();

                let _guard = self.acquire(transaction_id, 6, vec![Value::Integer(s_id as usize)]);

                self.server.update_subscriber_location(vlr_location, s_id);
            } else if transaction_type < self.config.mix[5] {
                // INSERT_CALL_FORWARDING
                let sf_type = rng.gen_range(1, 5);
                let start_time = rng.gen_range(0, 3) * 8;
                let end_time = rng.gen_range(1, 25);
                let numberx = self.config.gen_numberx(&mut rng);

                let _guard_sf =
                    self.acquire(transaction_id, 7, vec![Value::Integer(s_id as usize)]);

                let _guard_cf = self.acquire(
                    transaction_id,
                    8,
                    vec![
                        Value::Integer(s_id as usize),
                        Value::Integer(sf_type as usize),
                        Value::Integer(start_time as usize),
                    ],
                );

                self.server.get_special_facility_types(s_id);
                self.server
                    .insert_call_forwarding(s_id, sf_type, start_time, end_time, numberx);
            } else if transaction_type < self.config.mix[6] {
                // DELETE_CALL_FORWARDING
                let sf_type = rng.gen_range(1, 5);
                let start_time = rng.gen_range(0, 3) * 8;

                let _guard = self.acquire(
                    transaction_id,
                    8,
                    vec![
                        Value::Integer(s_id as usize),
                        Value::Integer(sf_type as usize),
                        Value::Integer(start_time as usize),
                    ],
                );

                self.server
                    .delete_call_forwarding(s_id, sf_type, start_time);
            } else if transaction_type < self.config.mix[7] {
                // GET_SUBSCRIBER_DATA_SCAN
                let byte2 = self.config.gen_byte2(&mut rng);

                let _guard = self.acquire(transaction_id, 9, byte2_to_arguments(&byte2));

                self.server.get_subscriber_data_scan(byte2);
            } else if transaction_type < self.config.mix[8] {
                // UPDATE_SUBSCRIBER_DATA_SCAN
                let bit_1 = rng.gen();
                let data_a = rng.gen();
                let sf_type = rng.gen_range(1, 5);
                let byte2 = self.config.gen_byte2(&mut rng);

                let _guard_s = self.acquire(transaction_id, 10, byte2_to_arguments(&byte2));

                let _guard_sf = self.acquire(
                    transaction_id,
                    5,
                    vec![
                        Value::Integer(s_id as usize),
                        Value::Integer(sf_type as usize),
                    ],
                );

                self.server.update_subscriber_bit_scan(bit_1, byte2);
                self.server
                    .update_special_facility_data(data_a, s_id, sf_type);
            } else {
                // UPDATE_LOCATION_SCAN
                let vlr_location = rng.gen();
                let byte2 = self.config.gen_byte2(&mut rng);

                let _guard = self.acquire(transaction_id, 11, byte2_to_arguments(&byte2));

                self.server
                    .update_subscriber_location_scan(vlr_location, byte2);
            }
        }
    }

    fn acquire(
        &self,
        transaction_id: usize,
        template_id: usize,
        arguments: Vec<Value>,
    ) -> RequestGuard {
        let request_variant = if self.ad_hoc {
            RequestVariant::AdHoc(self.dibs.templates[template_id].clone())
        } else {
            RequestVariant::Prepared(template_id)
        };

        let request = Request::new(transaction_id, request_variant, arguments);

        self.dibs.acquire(request, self.timeout).unwrap()
    }
}
