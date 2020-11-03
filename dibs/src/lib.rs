use crate::predicate::{ComparisonOperator, Connective, Predicate, Value};
use fnv::FnvHashMap;
use std::collections::HashSet;
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
    read_columns: HashSet<usize>,
    write_columns: HashSet<usize>,
    predicate: Predicate,
}

impl RequestTemplate {
    pub fn new(
        table: usize,
        read_columns: HashSet<usize>,
        write_columns: HashSet<usize>,
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
    transaction_id: usize,
    variant: RequestVariant,
    arguments: Vec<Value>,
    completed: (Mutex<bool>, Condvar),
}

impl Request {
    pub fn new(transaction_id: usize, variant: RequestVariant, arguments: Vec<Value>) -> Request {
        Request {
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

type RequestBucket = Arc<Mutex<FnvHashMap<usize, Arc<Request>>>>;

pub struct RequestGuard {
    id: usize,
    buckets: Vec<RequestBucket>,
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        for bucket in &self.buckets {
            bucket
                .lock()
                .unwrap()
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

pub struct Dibs {
    prepared_requests: Vec<PreparedRequest>,
    inflight_requests: Vec<Vec<RequestBucket>>,
    request_count: AtomicUsize,
    grouped_solve: bool,
}

impl Dibs {
    pub fn new(
        filters: &[Option<usize>],
        templates: &[RequestTemplate],
        grouped_solve: bool,
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
                    .map(|_| Arc::new(Mutex::new(FnvHashMap::default())))
                    .collect()
            })
            .collect();

        Dibs {
            prepared_requests,
            inflight_requests,
            request_count: AtomicUsize::new(0),
            grouped_solve,
        }
    }

    pub fn acquire(&self, request: Request, timeout: Duration) -> Option<RequestGuard> {
        let request = Arc::new(request);
        let request_id = self.request_count.fetch_add(1, Ordering::Relaxed);

        let mut conflicting_requests = vec![];

        let buckets = match &request.variant {
            RequestVariant::AdHoc(template) => {
                let buckets = &self.inflight_requests[template.table];
                for bucket in buckets {
                    self.solve_ad_hoc(
                        request_id,
                        &request,
                        template,
                        bucket,
                        &mut conflicting_requests,
                    );
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

                        self.solve_prepared(
                            request_id,
                            &request,
                            prepared_id,
                            bucket,
                            &mut conflicting_requests,
                        );

                        vec![Arc::clone(bucket)]
                    }

                    None => {
                        for bucket in buckets {
                            self.solve_prepared(
                                request_id,
                                &request,
                                prepared_id,
                                bucket,
                                &mut conflicting_requests,
                            );
                        }

                        buckets.clone()
                    }
                }
            }
        };

        for conflicting_request in &conflicting_requests {
            if conflicting_request.await_completion(timeout).timed_out() {
                return None;
            }
        }

        Some(RequestGuard {
            id: request_id,
            buckets,
        })
    }

    fn solve_ad_hoc(
        &self,
        request_id: usize,
        request: &Arc<Request>,
        template: &RequestTemplate,
        bucket: &RequestBucket,
        conflicting_requests: &mut Vec<Arc<Request>>,
    ) {
        let mut bucket_guard = bucket.lock().unwrap();

        for other_request in bucket_guard.values() {
            if other_request.transaction_id != request.transaction_id {
                let other_template = match &other_request.variant {
                    RequestVariant::AdHoc(t) => t,
                    &RequestVariant::Prepared(id) => &self.prepared_requests[id].template,
                };

                if potential_conflict(template, other_template) {
                    if self.grouped_solve {
                        if solver::solve_clustered(
                            &template.predicate,
                            &request.arguments,
                            &other_template.predicate,
                            &other_request.arguments,
                        ) {
                            conflicting_requests.push(Arc::clone(other_request));
                        }
                    } else {
                        if solver::solve_dnf(
                            &template.predicate,
                            &request.arguments,
                            &other_template.predicate,
                            &other_request.arguments,
                        ) {
                            conflicting_requests.push(Arc::clone(other_request));
                        }
                    }
                }
            }
        }

        bucket_guard.insert(request_id, Arc::clone(request));
    }

    fn solve_prepared(
        &self,
        request_id: usize,
        request: &Arc<Request>,
        prepared_id: usize,
        bucket: &RequestBucket,
        conflicting_requests: &mut Vec<Arc<Request>>,
    ) {
        let mut bucket_guard = bucket.lock().unwrap();

        for other_request in bucket_guard.values() {
            if other_request.transaction_id != request.transaction_id {
                match &other_request.variant {
                    RequestVariant::AdHoc(other_template) => {
                        if potential_conflict(
                            &self.prepared_requests[prepared_id].template,
                            other_template,
                        ) && solver::solve_clustered(
                            &self.prepared_requests[prepared_id].template.predicate,
                            &request.arguments,
                            &other_template.predicate,
                            &other_request.arguments,
                        ) {
                            conflicting_requests.push(Arc::clone(other_request));
                        }
                    }

                    &RequestVariant::Prepared(other_prepared_id) => {
                        if let Some(conflict) =
                            &self.prepared_requests[prepared_id].conflicts[other_prepared_id]
                        {
                            if solver::evaluate(
                                conflict,
                                &request.arguments,
                                &other_request.arguments,
                            ) {
                                conflicting_requests.push(Arc::clone(other_request));
                            }
                        }
                    }
                }
            }
        }

        bucket_guard.insert(request_id, Arc::clone(request));
    }
}

#[cfg(test)]
mod tests {
    use crate::predicate::{ComparisonOperator, Predicate, Value};
    use crate::solver;

    #[test]
    fn it_works() {
        let p = Predicate::conjunction(vec![
            Predicate::comparison(ComparisonOperator::Eq, 0, 0),
            Predicate::comparison(ComparisonOperator::Eq, 1, 1),
        ]);

        let _p_args = &[Value::Integer(0), Value::Integer(1)];

        let q = Predicate::disjunction(vec![
            Predicate::comparison(ComparisonOperator::Eq, 0, 2),
            Predicate::comparison(ComparisonOperator::Eq, 1, 3),
        ]);

        let _q_args = &[Value::Integer(0), Value::Integer(1)];

        println!("Predicate P:\n{}\n", p);

        println!("Predicate Q:\n{}\n", q);

        println!("{}", solver::prepare(&p, &q));
    }
}
