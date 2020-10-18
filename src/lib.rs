use crate::predicate::{Predicate, Value};
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

pub struct PreparedRequest {
    table: usize,
    read_columns: HashSet<usize>,
    write_columns: HashSet<usize>,
    predicate: Predicate,
}

pub struct DibsPreparedUnfiltered<'a> {
    requests: Vec<Mutex<RequestBucket<PreparedSolvable<'a>>>>,
    counter: AtomicUsize,
    conflicts: Vec<(usize, FnvHashMap<usize, Predicate>)>,
}

impl<'a> DibsPreparedUnfiltered<'a> {
    pub fn new(
        num_tables: usize,
        prepared_requests: &[PreparedRequest],
    ) -> DibsPreparedUnfiltered<'a> {
        DibsPreparedUnfiltered {
            requests: (0..num_tables)
                .map(|_| Mutex::new(RequestBucket::new()))
                .collect(),
            counter: AtomicUsize::new(0),
            conflicts: prepared_requests
                .iter()
                .map(|left| {
                    (
                        left.table,
                        FnvHashMap::from_iter(prepared_requests.iter().enumerate().filter_map(
                            |(j, right)| {
                                if left.table == right.table
                                    && (!left.read_columns.is_disjoint(&right.write_columns)
                                        || !left.write_columns.is_disjoint(&right.read_columns)
                                        || !left.write_columns.is_disjoint(&right.write_columns))
                                {
                                    Some((j, solver::prepare(&left.predicate, &right.predicate)))
                                } else {
                                    None
                                }
                            },
                        )),
                    )
                })
                .collect(),
        }
    }

    pub fn acquire(
        &'a self,
        id: usize,
        arguments: Vec<Value>,
    ) -> RequestGuard<PreparedSolvable<'a>> {
        let (table, conflicts) = &self
            .conflicts
            .get(id)
            .expect(&format!("unrecognized request id: {}", id));

        let bucket = self
            .requests
            .get(*table)
            .expect(&format!("unrecognized table id: {}", table));

        let request_id = self.counter.fetch_add(1, Ordering::Relaxed);
        let request = Request::new(PreparedSolvable::new(request_id, conflicts, arguments));
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
