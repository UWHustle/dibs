// use crate::notification::Notification;
// use crate::predicate::{Predicate, Value};
// use crate::solver;
// use fnv::FnvHashMap;
// use std::sync::Arc;
//
// pub enum RequestVariant {
//     AdHoc(Predicate),
//     Prepared(usize),
// }
//
// pub struct Request {
//     variant: RequestVariant,
//     arguments: Vec<Value>,
//     notification: Arc<Notification>,
// }
//
// impl Request {
//     pub fn new(variant: RequestVariant, arguments: Vec<Value>) -> Request {
//         Request {
//             variant,
//             arguments,
//             notification: Arc::new(Notification::new()),
//         }
//     }
//
//     pub fn complete(&self) {
//         self.notification.notify();
//     }
// }
//
// pub struct RequestBucket {
//     requests: FnvHashMap<usize, Request>,
//     cluster: bool,
// }
//
// impl RequestBucket {
//     pub fn new(cluster: bool) -> RequestBucket {
//         RequestBucket {
//             requests: FnvHashMap::default(),
//             cluster,
//         }
//     }
//
//     pub fn get_conflicts(&self, id: usize, request: Request) -> Vec<Arc<Notification>> {
//         self.requests
//             .iter()
//             .filter_map(|(other_id, other_request)| {
//                 assert_ne!(id, *other_id, "two requests with the same id {}", id);
//
//                 let p_args = &request.arguments;
//                 let q_args = &other_request.arguments;
//
//                 let result = match (&request.variant, &other_request.variant) {
//                     (RequestVariant::AdHoc(p), RequestVariant::AdHoc(q)) => {
//                         solver::solve_clustered(p, p_args, q, q_args)
//                     }
//                     (RequestVariant::AdHoc(p), RequestVariant::Prepared())
//                 };
//
//                 if result {
//                     Some(other_request.notification.clone())
//                 } else {
//                     None
//                 }
//             })
//             .collect()
//     }
//
//     pub fn insert(&mut self, id: usize, request: Request) -> Vec<Arc<Notification>> {
//         let notifications = self
//             .requests
//             .iter()
//             .filter_map(|(other_id, other_request)| {
//                 assert_ne!(id, *other_id, "two requests with the same id {}", id);
//                 request.solve(other_request)
//             })
//             .collect();
//
//         self.requests.insert(id, request);
//
//         notifications
//     }
//
//     pub fn remove(&mut self, id: &usize) {
//         self.requests
//             .remove(id)
//             .expect(&format!("no request with id {}", id))
//             .complete();
//     }
// }
