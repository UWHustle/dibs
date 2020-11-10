use crate::{Generator, Procedure};
use dibs::predicate::{ComparisonOperator, Predicate, Value};
use dibs::{AcquireError, Dibs, OptimizationLevel, RequestGuard, RequestTemplate};
use rand::rngs::ThreadRng;
use rand::{thread_rng, Rng};
use std::collections::HashSet;
use std::time::Duration;

pub trait ScanConnection {
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

pub enum ScanProcedure {
    GetSubscriberDataScan {
        byte2: [(u8, u8, u8, u8); 10],
    },
    UpdateSubscriberLocationScan {
        vlr_location: u32,
        byte2: [(u8, u8, u8, u8); 10],
    },
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

impl<C: ScanConnection> Procedure<C> for ScanProcedure {
    fn is_read_only(&self) -> bool {
        match self {
            ScanProcedure::GetSubscriberDataScan { .. } => true,
            ScanProcedure::UpdateSubscriberLocationScan { .. } => false,
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
            ScanProcedure::GetSubscriberDataScan { byte2 } => {
                let guard =
                    dibs.acquire(group_id, transaction_id, 0, byte2_to_arguments(&byte2))?;

                connection.get_subscriber_data_scan(*byte2);

                Ok(vec![guard])
            }
            ScanProcedure::UpdateSubscriberLocationScan {
                vlr_location,
                byte2,
            } => {
                let guard =
                    dibs.acquire(group_id, transaction_id, 1, byte2_to_arguments(&byte2))?;

                connection.update_subscriber_location_scan(*vlr_location, *byte2);

                Ok(vec![guard])
            }
        }
    }
}

pub struct ScanGenerator {
    select_mix: f64,
    range: u8,
}

impl ScanGenerator {
    pub fn new(select_mix: f64, range: u8) -> ScanGenerator {
        ScanGenerator { select_mix, range }
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

impl Generator for ScanGenerator {
    type Item = ScanProcedure;

    fn next(&self) -> ScanProcedure {
        let mut rng = thread_rng();

        let transaction_type = rng.gen::<f64>();

        if transaction_type < self.select_mix {
            let byte2 = self.gen_byte2(&mut rng);

            ScanProcedure::GetSubscriberDataScan { byte2 }
        } else {
            let vlr_location = rng.gen();
            let byte2 = self.gen_byte2(&mut rng);

            ScanProcedure::UpdateSubscriberLocationScan {
                vlr_location,
                byte2,
            }
        }
    }
}

pub fn dibs(num_conjuncts: usize, optimization: OptimizationLevel) -> Dibs {
    let scan_predicate = Predicate::conjunction(
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
    );

    let templates = vec![
        // (0) Get subscriber data scan.
        RequestTemplate::new(0, (0..33).collect(), HashSet::new(), scan_predicate.clone()),
        // (1) Update subscriber location scan.
        RequestTemplate::new(
            0,
            (21..31).collect(),
            [32].iter().cloned().collect(),
            scan_predicate,
        ),
    ];

    Dibs::new(&[None], &templates, optimization, Duration::from_secs(60))
}
