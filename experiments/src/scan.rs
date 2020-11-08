use crate::{Client, DibsConnector, OptimizationLevel};
use dibs::predicate::{ComparisonOperator, Predicate, Value};
use dibs::{Dibs, RequestTemplate};
use rand::rngs::ThreadRng;
use rand::Rng;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

pub trait ScanServer {
    /// Get subscriber data scan.
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

    /// Update subscriber location scan.
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

#[derive(Clone)]
pub struct ScanConfig {
    num_rows: u32,
    select_mix: f64,
    range: u8,
    num_conjuncts: usize,
}

impl ScanConfig {
    pub fn new(num_rows: u32, select_mix: f64, range: u8, num_conjuncts: usize) -> ScanConfig {
        assert!(select_mix >= 0.0 && select_mix <= 1.0);

        ScanConfig {
            num_rows,
            select_mix,
            range,
            num_conjuncts,
        }
    }

    pub fn get_num_rows(&self) -> u32 {
        self.num_rows
    }

    fn gen_byte2(&self, rng: &mut ThreadRng) -> [(u8, u8, u8, u8); 10] {
        let mut arguments = [(0, 0, 0, 0); 10];

        for argument in &mut arguments {
            argument.0 = rng.gen_range(0, u8::max_value() - self.range);
            argument.1 = argument.0 + self.range;
            argument.2 = rng.gen_range(0, u8::max_value() - self.range);
            argument.3 = argument.2 + self.range;
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

pub fn dibs(num_conjuncts: usize, optimization: OptimizationLevel) -> DibsConnector {
    let templates = vec![
        // (0) Get subscriber data scan.
        RequestTemplate::new(
            0,
            (0..33).collect(),
            HashSet::new(),
            scan_predicate(num_conjuncts),
        ),
        // (1) Update subscriber location scan.
        RequestTemplate::new(
            0,
            (21..31).collect(),
            [32].iter().cloned().collect(),
            scan_predicate(num_conjuncts),
        ),
    ];

    let dibs = Dibs::new(
        &[None],
        &templates,
        optimization != OptimizationLevel::Ungrouped,
    );

    DibsConnector::new(dibs, optimization, templates, Duration::from_secs(60))
}

pub struct ScanClient<S> {
    config: ScanConfig,
    dibs: Arc<DibsConnector>,
    server: Arc<S>,
}

impl<S> ScanClient<S> {
    pub fn new(config: ScanConfig, dibs: Arc<DibsConnector>, server: Arc<S>) -> ScanClient<S> {
        ScanClient {
            config,
            dibs,
            server,
        }
    }
}

impl<S> Client for ScanClient<S>
where
    S: ScanServer,
{
    fn process(&mut self, transaction_id: usize) {
        let mut rng = rand::thread_rng();

        let transaction_type = rng.gen::<f64>();

        if transaction_type < self.config.select_mix {
            // Get subscriber data scan.
            let byte2 = self.config.gen_byte2(&mut rng);

            let _guard = self
                .dibs
                .acquire(transaction_id, 0, byte2_to_arguments(&byte2));

            self.server.get_subscriber_data_scan(byte2);
        } else {
            // Update subscriber location scan.
            let vlr_location = rng.gen();
            let byte2 = self.config.gen_byte2(&mut rng);

            let _guard = self
                .dibs
                .acquire(transaction_id, 1, byte2_to_arguments(&byte2));

            self.server
                .update_subscriber_location_scan(vlr_location, byte2);
        }
    }
}
