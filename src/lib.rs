use crate::notification::Notification;
use crate::predicate::{Predicate, Value};
use fnv::FnvHashMap;
use std::iter::FromIterator;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

mod notification;
pub mod predicate;
mod solver;
mod union_find;

struct AdHocRequest {
    predicate: Predicate,
    arguments: Vec<Value>,
    completed: Arc<Notification>,
}

impl AdHocRequest {
    fn new(predicate: Predicate, arguments: Vec<Value>) -> AdHocRequest {
        AdHocRequest {
            predicate,
            arguments,
            completed: Arc::new(Notification::new()),
        }
    }
}

struct RequestBucket {
    requests: FnvHashMap<usize, AdHocRequest>,
}

impl RequestBucket {
    fn new() -> RequestBucket {
        RequestBucket {
            requests: FnvHashMap::default(),
        }
    }

    fn insert(
        &mut self,
        id: usize,
        request: AdHocRequest,
        cluster: bool,
    ) -> Vec<Arc<Notification>> {
        let notifications = self
            .requests
            .iter()
            .filter(|&(other_id, other_request)| {
                assert_ne!(id, *other_id, "two requests with the same id {}", id);

                if cluster {
                    solver::solve_clustered(
                        &request.predicate,
                        &request.arguments,
                        &other_request.predicate,
                        &other_request.arguments,
                    )
                } else {
                    solver::solve_dnf(
                        &request.predicate,
                        &request.arguments,
                        &other_request.predicate,
                        &other_request.arguments,
                    )
                }
            })
            .map(|(_, other_request)| other_request.completed.clone())
            .collect();

        self.requests.insert(id, request);

        notifications
    }

    fn remove(&mut self, id: &usize) {
        self.requests
            .remove(id)
            .expect(&format!("no request with id {}", id))
            .completed
            .notify();
    }
}

pub struct AdHocRequestGuard<'a> {
    id: usize,
    bucket: &'a Mutex<RequestBucket>,
}

impl AdHocRequestGuard<'_> {
    fn new(id: usize, bucket: &Mutex<RequestBucket>) -> AdHocRequestGuard {
        AdHocRequestGuard { id, bucket }
    }
}

impl Drop for AdHocRequestGuard<'_> {
    fn drop(&mut self) {
        self.bucket.lock().unwrap().remove(&self.id);
    }
}

pub struct Dibs {
    ad_hoc_requests: FnvHashMap<usize, Mutex<RequestBucket>>,
    counter: AtomicUsize,
}

impl Dibs {
    pub fn new(tables: &[usize]) -> Dibs {
        Dibs {
            ad_hoc_requests: FnvHashMap::from_iter(
                tables
                    .iter()
                    .map(|&table| (table, Mutex::new(RequestBucket::new()))),
            ),
            counter: AtomicUsize::new(0),
        }
    }
}

impl Dibs {
    pub fn ad_hoc_acquire(
        &self,
        table: usize,
        predicate: Predicate,
        arguments: Vec<Value>,
    ) -> Result<AdHocRequestGuard, String> {
        let bucket = self
            .ad_hoc_requests
            .get(&table)
            .ok_or(format!("unrecognized table id: {}", table))?;

        let id = self.counter.fetch_add(1, Ordering::Relaxed);
        let request = AdHocRequest::new(predicate, arguments);

        let notifications = bucket.lock().unwrap().insert(id, request, false);

        for notification in &notifications {
            notification.wait();
        }

        Ok(AdHocRequestGuard::new(id, bucket))
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

        let q = Predicate::conjunction(vec![
            Predicate::comparison(ComparisonOperator::Eq, 0, 0),
            Predicate::comparison(ComparisonOperator::Eq, 1, 1),
        ]);

        let q_args = &[Value::Integer(0), Value::Integer(1)];

        println!("Predicate P:\n{}\n", p);

        println!("Predicate Q:\n{}\n", q);

        println!("{:?}", solver::solve_clustered(&p, p_args, &q, q_args));
    }
}
