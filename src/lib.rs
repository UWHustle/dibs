use crate::notification::Notification;
use crate::predicate::{ComparisonOperator, Connective, Predicate, Value};
use fnv::{FnvHashMap, FnvHashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

mod notification;
pub mod predicate;
mod solver;
mod union_find;

const FILTER_MAGNITUDE: usize = 512;

#[derive(Clone)]
pub struct RequestTemplate {
    table: usize,
    read_columns: FnvHashSet<usize>,
    write_columns: FnvHashSet<usize>,
    predicate: Predicate,
}

pub enum RequestVariant {
    AdHoc(RequestTemplate),
    Prepared(usize),
}

pub struct Request {
    variant: RequestVariant,
    arguments: Vec<Value>,
    notification: Arc<Notification>,
}

impl Request {
    pub fn new(variant: RequestVariant, arguments: Vec<Value>) -> Request {
        Request {
            variant,
            arguments,
            notification: Arc::new(Notification::new()),
        }
    }

    pub fn complete(&self) {
        self.notification.notify();
    }
}

struct PreparedRequest {
    template: RequestTemplate,
    filter: Option<usize>,
    conflicts: Vec<Option<Predicate>>,
}

struct TableRequestGroup {
    filtered: Vec<Mutex<FnvHashMap<usize, Request>>>,
    residual: Mutex<FnvHashMap<usize, Request>>,
    residual_count: AtomicUsize,
}

pub struct RequestGuard<'a> {
    id: usize,
    bucket: &'a Mutex<FnvHashMap<usize, Request>>,
    residual_count: Option<&'a AtomicUsize>,
}

impl<'a> Drop for RequestGuard<'a> {
    fn drop(&mut self) {
        self.bucket
            .lock()
            .unwrap()
            .remove(&self.id)
            .expect(&format!("no request with id {}", self.id))
            .complete();

        if let Some(residual_count) = self.residual_count {
            residual_count.fetch_sub(1, Ordering::Release);
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
    inflight_requests: Vec<TableRequestGroup>,
    request_count: AtomicUsize,
}

impl Dibs {
    pub fn new(filters: &[Option<usize>], templates: &[RequestTemplate]) -> Dibs {
        let prepared_requests = templates
            .iter()
            .map(|template| PreparedRequest {
                template: template.clone(),
                filter: filters[template.table].and_then(|column| prepare_filter(template, column)),
                conflicts: prepare_conflicts(template, templates),
            })
            .collect();

        let inflight_requests = (0..filters.len())
            .map(|_| TableRequestGroup {
                filtered: (0..FILTER_MAGNITUDE)
                    .map(|_| Mutex::new(FnvHashMap::default()))
                    .collect(),
                residual: Mutex::new(FnvHashMap::default()),
                residual_count: AtomicUsize::new(0),
            })
            .collect();

        Dibs {
            prepared_requests,
            inflight_requests,
            request_count: AtomicUsize::new(0),
        }
    }

    pub fn acquire(&self, request: Request) -> RequestGuard {
        let request_id = self.request_count.fetch_add(1, Ordering::Relaxed);
        let mut notifications = vec![];

        let request_guard = match &request.variant {
            RequestVariant::AdHoc(template) => {
                let table_request_group = &self.inflight_requests[template.table];

                let mut guards = Vec::with_capacity(table_request_group.filtered.len());

                for bucket in &table_request_group.filtered {
                    let bucket_guard = bucket.lock().unwrap();

                    notifications.extend(self.solve_ad_hoc(
                        template,
                        &request.arguments,
                        bucket_guard.values(),
                    ));

                    guards.push(bucket_guard);
                }

                let mut residual_guard = table_request_group.residual.lock().unwrap();

                notifications.extend(self.solve_ad_hoc(
                    template,
                    &request.arguments,
                    residual_guard.values(),
                ));

                table_request_group
                    .residual_count
                    .fetch_add(1, Ordering::Release);

                residual_guard.insert(request_id, request);

                RequestGuard {
                    id: request_id,
                    bucket: &table_request_group.residual,
                    residual_count: Some(&table_request_group.residual_count),
                }
            }

            RequestVariant::Prepared(id) => {
                let prepared_request = &self.prepared_requests[*id];
                let table_request_group = &self.inflight_requests[prepared_request.template.table];

                if let Some(filter) = prepared_request.filter {
                    let bucket = &table_request_group.filtered[filter];
                    let mut bucket_guard = bucket.lock().unwrap();

                    notifications.extend(self.solve_prepared(
                        *id,
                        &request.arguments,
                        bucket_guard.values(),
                    ));

                    if table_request_group.residual_count.load(Ordering::Acquire) > 0 {
                        notifications.extend(self.solve_prepared(
                            *id,
                            &request.arguments,
                            table_request_group.residual.lock().unwrap().values(),
                        ));
                    }

                    bucket_guard.insert(*id, request);

                    RequestGuard {
                        id: request_id,
                        bucket,
                        residual_count: None,
                    }
                } else {
                    let mut guards = Vec::with_capacity(table_request_group.filtered.len());

                    for bucket in &table_request_group.filtered {
                        let bucket_guard = bucket.lock().unwrap();

                        notifications.extend(self.solve_prepared(
                            *id,
                            &request.arguments,
                            bucket_guard.values(),
                        ));

                        guards.push(bucket_guard);
                    }

                    let mut residual_guard = table_request_group.residual.lock().unwrap();

                    notifications.extend(self.solve_prepared(
                        *id,
                        &request.arguments,
                        residual_guard.values(),
                    ));

                    table_request_group
                        .residual_count
                        .fetch_add(1, Ordering::Release);

                    residual_guard.insert(request_id, request);

                    RequestGuard {
                        id: request_id,
                        bucket: &table_request_group.residual,
                        residual_count: Some(&table_request_group.residual_count),
                    }
                }
            }
        };

        for notification in &notifications {
            notification.wait();
        }

        request_guard
    }

    fn solve_ad_hoc<'a>(
        &'a self,
        template: &'a RequestTemplate,
        arguments: &'a [Value],
        other_requests: impl Iterator<Item = &'a Request> + 'a,
    ) -> impl Iterator<Item = Arc<Notification>> + 'a {
        other_requests.filter_map(move |other_request| {
            let other_template = match &other_request.variant {
                RequestVariant::AdHoc(t) => t,
                RequestVariant::Prepared(id) => &self.prepared_requests[*id].template,
            };

            if potential_conflict(template, other_template)
                && solver::solve_clustered(
                    &template.predicate,
                    arguments,
                    &other_template.predicate,
                    &other_request.arguments,
                )
            {
                Some(other_request.notification.clone())
            } else {
                None
            }
        })
    }

    fn solve_prepared<'a>(
        &'a self,
        id: usize,
        arguments: &'a [Value],
        other_requests: impl Iterator<Item = &'a Request> + 'a,
    ) -> impl Iterator<Item = Arc<Notification>> + 'a {
        other_requests.filter_map(move |other_request| match &other_request.variant {
            RequestVariant::AdHoc(other_template) => {
                if potential_conflict(&self.prepared_requests[id].template, other_template)
                    && solver::solve_clustered(
                        &self.prepared_requests[id].template.predicate,
                        arguments,
                        &other_template.predicate,
                        &other_request.arguments,
                    )
                {
                    Some(other_request.notification.clone())
                } else {
                    None
                }
            }
            RequestVariant::Prepared(other_id) => self.prepared_requests[id].conflicts[*other_id]
                .as_ref()
                .and_then(|conflict| {
                    if solver::evaluate(conflict, &arguments, &other_request.arguments) {
                        Some(other_request.notification.clone())
                    } else {
                        None
                    }
                }),
        })
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

        let p_args = &[Value::Integer(0), Value::Integer(1)];

        let q = Predicate::disjunction(vec![
            Predicate::comparison(ComparisonOperator::Eq, 0, 2),
            Predicate::comparison(ComparisonOperator::Eq, 1, 3),
        ]);

        let q_args = &[Value::Integer(0), Value::Integer(1)];

        println!("Predicate P:\n{}\n", p);

        println!("Predicate Q:\n{}\n", q);

        println!("{}", solver::prepare(&p, &q));
    }
}
