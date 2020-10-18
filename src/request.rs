use crate::notification::Notification;
use crate::predicate::{Predicate, Value};
use crate::solver;
use fnv::FnvHashMap;
use std::sync::Arc;

pub trait Solvable {
    fn solve(&self, other: &Self) -> bool;
}

pub struct AdHocDnfSolvable {
    predicate: Predicate,
    arguments: Vec<Value>,
}

impl AdHocDnfSolvable {
    pub fn new(predicate: Predicate, arguments: Vec<Value>) -> AdHocDnfSolvable {
        AdHocDnfSolvable {
            predicate,
            arguments,
        }
    }
}

impl Solvable for AdHocDnfSolvable {
    fn solve(&self, other: &Self) -> bool {
        solver::solve_dnf(
            &self.predicate,
            &self.arguments,
            &other.predicate,
            &other.arguments,
        )
    }
}

pub struct AdHocClusteredSolvable {
    predicate: Predicate,
    arguments: Vec<Value>,
}

impl AdHocClusteredSolvable {
    pub fn new(predicate: Predicate, arguments: Vec<Value>) -> AdHocClusteredSolvable {
        AdHocClusteredSolvable {
            predicate,
            arguments,
        }
    }
}

impl Solvable for AdHocClusteredSolvable {
    fn solve(&self, other: &Self) -> bool {
        solver::solve_clustered(
            &self.predicate,
            &self.arguments,
            &other.predicate,
            &other.arguments,
        )
    }
}

pub struct PreparedSolvable<'a> {
    id: usize,
    conflicts: &'a FnvHashMap<usize, Predicate>,
    arguments: Vec<Value>,
}

impl<'a> PreparedSolvable<'a> {
    pub fn new(
        id: usize,
        conflicts: &'a FnvHashMap<usize, Predicate>,
        arguments: Vec<Value>,
    ) -> PreparedSolvable {
        PreparedSolvable {
            id,
            conflicts,
            arguments,
        }
    }
}

impl<'a> Solvable for PreparedSolvable<'a> {
    fn solve(&self, other: &Self) -> bool {
        self.conflicts
            .get(&other.id)
            .map(|conflict| solver::evaluate(conflict, &self.arguments, &other.arguments))
            .unwrap_or(false)
    }
}

pub struct Request<T>
where
    T: Solvable,
{
    solvable: T,
    notification: Arc<Notification>,
}

impl<T> Request<T>
where
    T: Solvable,
{
    pub fn new(solvable: T) -> Request<T> {
        Request {
            solvable,
            notification: Arc::new(Notification::new()),
        }
    }

    pub fn solve(&self, other: &Request<T>) -> Option<Arc<Notification>> {
        if self.solvable.solve(&other.solvable) {
            Some(self.notification.clone())
        } else {
            None
        }
    }

    pub fn complete(&self) {
        self.notification.notify();
    }
}

pub struct RequestBucket<T>
where
    T: Solvable,
{
    requests: FnvHashMap<usize, Request<T>>,
}

impl<T> RequestBucket<T>
where
    T: Solvable,
{
    pub fn new() -> RequestBucket<T> {
        RequestBucket {
            requests: FnvHashMap::default(),
        }
    }

    pub fn insert(&mut self, id: usize, request: Request<T>) -> Vec<Arc<Notification>> {
        let notifications = self
            .requests
            .iter()
            .filter_map(|(other_id, other_request)| {
                assert_ne!(id, *other_id, "two requests with the same id {}", id);
                request.solve(other_request)
            })
            .collect();

        self.requests.insert(id, request);

        notifications
    }

    pub fn remove(&mut self, id: &usize) {
        self.requests
            .remove(id)
            .expect(&format!("no request with id {}", id))
            .complete();
    }
}
