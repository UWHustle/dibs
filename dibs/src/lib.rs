use crate::predicate::{ComparisonOperator, Connective, Predicate, Value};
use fnv::{FnvHashMap, FnvHashSet};
use rand::Rng;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, WaitTimeoutResult};
use std::time::Duration;

pub mod predicate;
mod solver;
mod union_find;

const FILTER_MAGNITUDE: usize = 1024;

#[derive(Clone)]
pub struct RequestTemplate {
    table: usize,
    read_columns: FnvHashSet<usize>,
    write_columns: FnvHashSet<usize>,
    predicate: Predicate,
}

impl RequestTemplate {
    pub fn new(
        table: usize,
        read_columns: FnvHashSet<usize>,
        write_columns: FnvHashSet<usize>,
        predicate: Predicate,
    ) -> RequestTemplate {
        RequestTemplate {
            table,
            read_columns,
            write_columns,
            predicate,
        }
    }
}

pub enum RequestVariant {
    AdHoc(RequestTemplate),
    Prepared(usize),
}

pub struct Request {
    group_id: usize,
    transaction_id: usize,
    variant: RequestVariant,
    arguments: Vec<Value>,
    completed: (Mutex<bool>, Condvar),
}

impl Request {
    pub fn new(
        group_id: usize,
        transaction_id: usize,
        variant: RequestVariant,
        arguments: Vec<Value>,
    ) -> Request {
        Request {
            group_id,
            transaction_id,
            variant,
            arguments,
            completed: (Mutex::new(false), Condvar::new()),
        }
    }

    pub fn complete(&self) {
        let (lock, cvar) = &self.completed;
        *lock.lock().unwrap() = true;
        cvar.notify_all();
    }

    pub fn await_completion(&self, timeout: Duration) -> WaitTimeoutResult {
        let (lock, cvar) = &self.completed;
        cvar.wait_timeout_while(lock.lock().unwrap(), timeout, |completed| !*completed)
            .unwrap()
            .1
    }
}

struct PreparedRequest {
    template: RequestTemplate,
    filter: Option<usize>,
    conflicts: Vec<Option<Predicate>>,
}

type RequestBucket = Arc<spin::Mutex<FnvHashMap<usize, Arc<Request>>>>;

pub struct RequestGuard {
    id: usize,
    buckets: Vec<RequestBucket>,
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        for bucket in &self.buckets {
            bucket
                .lock()
                .remove(&self.id)
                .expect(&format!("no request with id {}", self.id))
                .complete();
        }
    }
}

fn potential_conflict(p: &RequestTemplate, q: &RequestTemplate) -> bool {
    p.table == q.table
        && (!p.read_columns.is_disjoint(&q.write_columns)
            || !p.write_columns.is_disjoint(&q.read_columns)
            || !p.write_columns.is_disjoint(&q.write_columns))
}

fn prepare_filter(template: &RequestTemplate, column: usize) -> Option<usize> {
    match &template.predicate {
        Predicate::Comparison(comparison)
            if comparison.operator == ComparisonOperator::Eq && comparison.left == column =>
        {
            Some(comparison.right)
        }
        Predicate::Connective(_connective @ Connective::Conjunction, operands) => {
            operands.iter().find_map(|operand| match operand {
                Predicate::Comparison(comparison)
                    if comparison.operator == ComparisonOperator::Eq
                        && comparison.left == column =>
                {
                    Some(comparison.right)
                }
                _ => None,
            })
        }
        _ => None,
    }
}

fn prepare_conflicts(
    template: &RequestTemplate,
    other_templates: &[RequestTemplate],
) -> Vec<Option<Predicate>> {
    other_templates
        .iter()
        .map(|other_template| {
            if potential_conflict(template, other_template) {
                Some(solver::prepare(
                    &template.predicate,
                    &other_template.predicate,
                ))
            } else {
                None
            }
        })
        .collect()
}

#[derive(Debug)]
pub enum AcquireError {
    Timeout(usize),
    GroupConflict,
}

#[derive(Clone, Copy, PartialEq)]
pub enum OptimizationLevel {
    Ungrouped,
    Grouped,
    Prepared,
    Filtered,
}

impl FromStr for OptimizationLevel {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, ()> {
        match s {
            "ungrouped" => Ok(OptimizationLevel::Ungrouped),
            "grouped" => Ok(OptimizationLevel::Grouped),
            "prepared" => Ok(OptimizationLevel::Prepared),
            "filtered" => Ok(OptimizationLevel::Filtered),
            _ => Err(()),
        }
    }
}

pub struct Dibs {
    prepared_requests: Vec<PreparedRequest>,
    inflight_requests: Vec<Vec<RequestBucket>>,
    optimization: OptimizationLevel,
    blowup_limit: usize,
    timeout: Duration,
    request_count: AtomicUsize,
}

impl Dibs {
    pub fn new(
        filters: &[Option<usize>],
        templates: &[RequestTemplate],
        optimization: OptimizationLevel,
        blowup_limit: usize,
        timeout: Duration,
    ) -> Dibs {
        let prepared_requests = templates
            .iter()
            .map(|template| PreparedRequest {
                template: template.clone(),
                filter: filters[template.table].and_then(|column| prepare_filter(template, column)),
                conflicts: prepare_conflicts(template, templates),
            })
            .collect();

        let inflight_requests = filters
            .iter()
            .map(|filter| {
                let num_partitions = match filter {
                    Some(_) => FILTER_MAGNITUDE,
                    None => 1,
                };

                (0..num_partitions)
                    .map(|_| Arc::new(spin::Mutex::new(FnvHashMap::default())))
                    .collect()
            })
            .collect();

        Dibs {
            prepared_requests,
            inflight_requests,
            optimization,
            blowup_limit,
            timeout,
            request_count: AtomicUsize::new(0),
        }
    }

    pub fn acquire(
        &self,
        group_id: usize,
        transaction_id: usize,
        template_id: usize,
        arguments: Vec<Value>,
    ) -> Result<RequestGuard, AcquireError> {
        let request_variant = match self.optimization {
            OptimizationLevel::Ungrouped | OptimizationLevel::Grouped => {
                RequestVariant::AdHoc(self.prepared_requests[template_id].template.clone())
            }
            _ => RequestVariant::Prepared(template_id),
        };

        let request = Arc::new(Request::new(
            group_id,
            transaction_id,
            request_variant,
            arguments,
        ));

        let request_id = self.request_count.fetch_add(1, Ordering::Relaxed);

        let mut conflicting_requests = vec![];

        let buckets = match &request.variant {
            RequestVariant::AdHoc(template) => {
                let buckets = &self.inflight_requests[template.table];
                for bucket in buckets {
                    conflicting_requests
                        .extend(self.solve_ad_hoc(request_id, &request, template, bucket));
                }

                buckets.clone()
            }

            &RequestVariant::Prepared(prepared_id) => {
                let prepared_request = &self.prepared_requests[prepared_id];
                let buckets = &self.inflight_requests[prepared_request.template.table];

                match prepared_request.filter {
                    Some(filter) => {
                        let bucket_index = match request.arguments[filter] {
                            Value::Integer(v) => v % buckets.len(),
                            _ => panic!("filtering on non-integer columns is not yet supported"),
                        };

                        let bucket = &buckets[bucket_index];

                        conflicting_requests =
                            self.solve_prepared(request_id, &request, prepared_id, bucket);

                        vec![Arc::clone(bucket)]
                    }

                    None => {
                        for bucket in buckets {
                            conflicting_requests.extend(self.solve_prepared(
                                request_id,
                                &request,
                                prepared_id,
                                bucket,
                            ));
                        }

                        buckets.clone()
                    }
                }
            }
        };

        let guard = RequestGuard {
            id: request_id,
            buckets,
        };

        let timeout = self.timeout.mul_f32(rand::thread_rng().gen_range(0.8, 1.2));

        for conflicting_request in &conflicting_requests {
            if conflicting_request.group_id == request.group_id {
                return Err(AcquireError::GroupConflict);
            }

            if conflicting_request.await_completion(timeout).timed_out() {
                return Err(AcquireError::Timeout(conflicting_request.transaction_id));
            }
        }

        Ok(guard)
    }

    fn solve_ad_hoc(
        &self,
        request_id: usize,
        request: &Arc<Request>,
        template: &RequestTemplate,
        bucket: &RequestBucket,
    ) -> Vec<Arc<Request>> {
        let mut other_requests = vec![];

        {
            let mut bucket_guard = bucket.lock();
            other_requests.extend(bucket_guard.values().cloned());
            bucket_guard.insert(request_id, Arc::clone(request));
        }

        other_requests.retain(|other_request| {
            other_request.transaction_id != request.transaction_id && {
                let other_template = match &other_request.variant {
                    RequestVariant::AdHoc(t) => t,
                    &RequestVariant::Prepared(id) => &self.prepared_requests[id].template,
                };

                potential_conflict(template, other_template)
                    && match self.optimization {
                        OptimizationLevel::Ungrouped => solver::solve_dnf(
                            &template.predicate,
                            &request.arguments,
                            &other_template.predicate,
                            &other_request.arguments,
                            self.blowup_limit,
                        ),
                        _ => solver::solve_clustered(
                            &template.predicate,
                            &request.arguments,
                            &other_template.predicate,
                            &other_request.arguments,
                            self.blowup_limit,
                        ),
                    }
            }
        });

        other_requests
    }

    fn solve_prepared(
        &self,
        request_id: usize,
        request: &Arc<Request>,
        prepared_id: usize,
        bucket: &RequestBucket,
    ) -> Vec<Arc<Request>> {
        let mut other_requests = vec![];

        {
            let mut bucket_guard = bucket.lock();
            other_requests.extend(bucket_guard.values().cloned());
            bucket_guard.insert(request_id, Arc::clone(request));
        };

        other_requests.retain(|other_request| {
            other_request.transaction_id != request.transaction_id
                && match &other_request.variant {
                    RequestVariant::AdHoc(other_template) => {
                        potential_conflict(
                            &self.prepared_requests[prepared_id].template,
                            other_template,
                        ) && solver::solve_clustered(
                            &self.prepared_requests[prepared_id].template.predicate,
                            &request.arguments,
                            &other_template.predicate,
                            &other_request.arguments,
                            self.blowup_limit,
                        )
                    }
                    &RequestVariant::Prepared(other_prepared_id) => {
                        match &self.prepared_requests[prepared_id].conflicts[other_prepared_id] {
                            Some(conflict) => solver::evaluate(
                                conflict,
                                &request.arguments,
                                &other_request.arguments,
                            ),
                            None => false,
                        }
                    }
                }
        });

        other_requests
    }
}
