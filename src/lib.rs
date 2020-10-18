use crate::predicate::{ComparisonOperator, Connective, Predicate, Value};
use crate::request::{
    AdHocClusteredSolvable, AdHocDnfSolvable, PreparedSolvable, Request, RequestBucket, Solvable,
};
use fnv::FnvHashMap;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

mod notification;
pub mod predicate;
mod request;
mod solver;
mod union_find;

trait AdHocSolvable {
    fn dibs_new(predicate: Predicate, arguments: Vec<Value>) -> Self;
}

impl AdHocSolvable for AdHocDnfSolvable {
    fn dibs_new(predicate: Predicate, arguments: Vec<Value>) -> Self {
        AdHocDnfSolvable::new(predicate, arguments)
    }
}

impl AdHocSolvable for AdHocClusteredSolvable {
    fn dibs_new(predicate: Predicate, arguments: Vec<Value>) -> Self {
        AdHocClusteredSolvable::new(predicate, arguments)
    }
}

pub struct RequestGuard<'a, T>
where
    T: Solvable,
{
    id: usize,
    bucket: &'a Mutex<RequestBucket<T>>,
}

impl<T> RequestGuard<'_, T>
where
    T: Solvable,
{
    fn new(id: usize, bucket: &Mutex<RequestBucket<T>>) -> RequestGuard<T> {
        RequestGuard { id, bucket }
    }
}

impl<T> Drop for RequestGuard<'_, T>
where
    T: Solvable,
{
    fn drop(&mut self) {
        self.bucket.lock().unwrap().remove(&self.id);
    }
}

struct DibsAdHoc<T>
where
    T: Solvable,
{
    requests: Vec<Mutex<RequestBucket<T>>>,
    counter: AtomicUsize,
}

impl<T> DibsAdHoc<T>
where
    T: Solvable + AdHocSolvable,
{
    fn new(num_tables: usize) -> DibsAdHoc<T> {
        DibsAdHoc {
            requests: (0..num_tables)
                .map(|_| Mutex::new(RequestBucket::new()))
                .collect(),
            counter: AtomicUsize::new(0),
        }
    }

    fn acquire(
        &self,
        table: usize,
        predicate: Predicate,
        arguments: Vec<Value>,
    ) -> RequestGuard<T> {
        let bucket = self
            .requests
            .get(table)
            .expect(&format!("unrecognized table id: {}", table));

        let id = self.counter.fetch_add(1, Ordering::Relaxed);
        let request = Request::new(T::dibs_new(predicate, arguments));
        let notifications = bucket.lock().unwrap().insert(id, request);

        for notification in &notifications {
            notification.wait();
        }

        RequestGuard::new(id, bucket)
    }
}

pub struct DibsAdHocDnf(DibsAdHoc<AdHocDnfSolvable>);

impl DibsAdHocDnf {
    pub fn new(num_tables: usize) -> DibsAdHocDnf {
        DibsAdHocDnf(DibsAdHoc::new(num_tables))
    }

    pub fn acquire(
        &self,
        table: usize,
        predicate: Predicate,
        arguments: Vec<Value>,
    ) -> RequestGuard<AdHocDnfSolvable> {
        self.0.acquire(table, predicate, arguments)
    }
}

pub struct DibsAdHocClustered(DibsAdHoc<AdHocClusteredSolvable>);

impl DibsAdHocClustered {
    pub fn new(num_tables: usize) -> DibsAdHocClustered {
        DibsAdHocClustered(DibsAdHoc::new(num_tables))
    }

    pub fn acquire(
        &self,
        table: usize,
        predicate: Predicate,
        arguments: Vec<Value>,
    ) -> RequestGuard<AdHocClusteredSolvable> {
        self.0.acquire(table, predicate, arguments)
    }
}

pub struct RequestTemplate {
    table: usize,
    read_columns: HashSet<usize>,
    write_columns: HashSet<usize>,
    predicate: Predicate,
}

struct PreparedRequest {
    table: usize,
    filter: Option<usize>,
    conflicts: FnvHashMap<usize, Predicate>,
}

fn find_conflicts(
    template: &RequestTemplate,
    other_templates: &[RequestTemplate],
) -> FnvHashMap<usize, Predicate> {
    FnvHashMap::from_iter(
        other_templates
            .iter()
            .enumerate()
            .filter_map(|(i, other_template)| {
                if template.table == other_template.table
                    && (!template
                        .read_columns
                        .is_disjoint(&other_template.write_columns)
                        || !template
                            .write_columns
                            .is_disjoint(&other_template.read_columns)
                        || !template
                            .write_columns
                            .is_disjoint(&other_template.write_columns))
                {
                    Some((
                        i,
                        solver::prepare(&template.predicate, &other_template.predicate),
                    ))
                } else {
                    None
                }
            }),
    )
}

pub struct DibsPreparedUnfiltered<'a> {
    prepared_requests: Vec<PreparedRequest>,
    requests: Vec<Mutex<RequestBucket<PreparedSolvable<'a>>>>,
    counter: AtomicUsize,
}

impl<'a> DibsPreparedUnfiltered<'a> {
    pub fn new(num_tables: usize, templates: &[RequestTemplate]) -> DibsPreparedUnfiltered<'a> {
        DibsPreparedUnfiltered {
            prepared_requests: templates
                .iter()
                .map(|template| PreparedRequest {
                    table: template.table,
                    filter: None,
                    conflicts: find_conflicts(template, templates),
                })
                .collect(),
            requests: (0..num_tables)
                .map(|_| Mutex::new(RequestBucket::new()))
                .collect(),
            counter: AtomicUsize::new(0),
        }
    }

    pub fn acquire(
        &'a self,
        id: usize,
        arguments: Vec<Value>,
    ) -> RequestGuard<PreparedSolvable<'a>> {
        let prepared_request = self
            .prepared_requests
            .get(id)
            .expect(&format!("unrecognized request id: {}", id));

        let bucket = self.requests.get(prepared_request.table).expect(&format!(
            "unrecognized table id: {}",
            prepared_request.table
        ));

        let request_id = self.counter.fetch_add(1, Ordering::Relaxed);
        let request = Request::new(PreparedSolvable::new(
            request_id,
            &prepared_request.conflicts,
            arguments,
        ));
        let notifications = bucket.lock().unwrap().insert(id, request);

        for notification in &notifications {
            notification.wait();
        }

        RequestGuard::new(id, bucket)
    }
}

struct RequestFilter<'a> {
    filtered: Vec<Mutex<RequestBucket<PreparedSolvable<'a>>>>,
    residual: Mutex<RequestBucket<PreparedSolvable<'a>>>,
}

impl<'a> RequestFilter<'a> {
    fn new() -> RequestFilter<'a> {
        RequestFilter {
            filtered: (0..512).map(|_| Mutex::new(RequestBucket::new())).collect(),
            residual: Mutex::new(RequestBucket::new()),
        }
    }
}

pub struct DibsPreparedFiltered<'a> {
    prepared_requests: Vec<PreparedRequest>,
    requests: Vec<RequestFilter<'a>>,
    counter: AtomicUsize,
}

impl<'a> DibsPreparedFiltered<'a> {
    pub fn new(filters: Vec<usize>, templates: &[RequestTemplate]) -> DibsPreparedFiltered<'a> {
        DibsPreparedFiltered {
            prepared_requests: templates
                .iter()
                .map(|template| PreparedRequest {
                    table: template.table,
                    filter: match &template.predicate {
                        Predicate::Comparison(comparison)
                            if comparison.operator == ComparisonOperator::Eq
                                && comparison.left == filters[template.table] =>
                        {
                            Some(comparison.right)
                        }
                        Predicate::Connective(_connective @ Connective::Conjunction, operands) => {
                            operands.iter().find_map(|operand| match operand {
                                Predicate::Comparison(comparison)
                                    if comparison.operator == ComparisonOperator::Eq
                                        && comparison.left == filters[template.table] =>
                                {
                                    Some(comparison.right)
                                }
                                _ => None,
                            })
                        }
                        _ => None,
                    },
                    conflicts: find_conflicts(template, templates),
                })
                .collect(),
            requests: (0..filters.len()).map(|_| RequestFilter::new()).collect(),
            counter: AtomicUsize::new(0),
        }
    }

    pub fn acquire(
        &'a self,
        id: usize,
        arguments: Vec<Value>,
    ) -> RequestGuard<PreparedSolvable<'a>> {
        let prepared_request = self
            .prepared_requests
            .get(id)
            .expect(&format!("unrecognized request id: {}", id));

        let bucket = match &prepared_request.filter {
            Some(filter) => {
                let hash = match &arguments[*filter] {
                    Value::Integer(i) => *i % 512,
                    _ => panic!(),
                };

                &self.requests[prepared_request.table].filtered[hash]
            }
            None => &self.requests[prepared_request.table].residual,
        };

        let request_id = self.counter.fetch_add(1, Ordering::Relaxed);
        let request = Request::new(PreparedSolvable::new(
            request_id,
            &prepared_request.conflicts,
            arguments,
        ));
        let notifications = bucket.lock().unwrap().insert(id, request);

        for notification in &notifications {
            notification.wait();
        }

        RequestGuard::new(id, bucket)
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
