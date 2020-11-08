use dibs::predicate::Value;
use dibs::{Dibs, Request, RequestGuard, RequestTemplate, RequestVariant};
use std::str::FromStr;
use std::time::Duration;

pub mod arrow_server;
pub mod runner;
pub mod scan;
pub mod sqlite_server;
pub mod tatp;
pub mod ycsb;

pub trait Client {
    fn process(&mut self, transaction_id: usize);
}

#[derive(Clone, PartialEq)]
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

pub struct DibsConnector {
    dibs: Dibs,
    optimization: OptimizationLevel,
    templates: Vec<RequestTemplate>,
    timeout: Duration,
}

impl DibsConnector {
    fn new(
        dibs: Dibs,
        optimization: OptimizationLevel,
        templates: Vec<RequestTemplate>,
        timeout: Duration,
    ) -> DibsConnector {
        DibsConnector {
            dibs,
            optimization,
            templates,
            timeout,
        }
    }

    fn acquire(
        &self,
        transaction_id: usize,
        template_id: usize,
        arguments: Vec<Value>,
    ) -> RequestGuard {
        let request_variant = match self.optimization {
            OptimizationLevel::Ungrouped | OptimizationLevel::Grouped => {
                RequestVariant::AdHoc(self.templates[template_id].clone())
            }
            _ => RequestVariant::Prepared(template_id),
        };

        let request = Request::new(transaction_id, request_variant, arguments);

        self.dibs.acquire(request, self.timeout).unwrap()
    }
}
