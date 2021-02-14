use crate::benchmarks::seats;
use std::fmt::Debug;

#[derive(Debug)]
pub enum Error {
    UserAbort(String),
    InvalidOperation,
}

pub enum DeleteReservationVariant<'a> {
    CustomerId(i64),
    CustomerIdString(&'a str),
    FrequentFlyer(&'a str),
}

pub enum UpdateCustomerVariant<'a> {
    CustomerId(i64),
    CustomerIdString(&'a str),
}

pub struct AirportInfo {
    pub f_id: i64,
    pub seats_left: i64,
    pub al_name: String,
    pub depart_time: i64,
    pub depart_ap_code: String,
    pub depart_ap_name: String,
    pub depart_ap_city: String,
    pub depart_ap_co_id: i64,
    pub arrive_time: i64,
    pub arrive_ap_code: String,
    pub arrive_ap_name: String,
    pub arrive_ap_city: String,
    pub arrive_ap_co_id: i64,
}

pub trait SEATSConnection {
    fn delete_reservation(
        &self,
        variant: DeleteReservationVariant,
        f_id: i64,
    ) -> Result<(), seats::Error>;

    fn find_flights(
        &self,
        depart_aid: i64,
        arrive_aid: i64,
        start_timestamp: i64,
        end_timestamp: i64,
        distance: f64,
    ) -> Result<Vec<AirportInfo>, Error>;

    fn find_open_seats(&self, f_id: i64) -> Result<Vec<(i64, i64, f64)>, Error>;

    fn new_reservation(
        &self,
        r_id: i64,
        c_id: i64,
        f_id: i64,
        seat: i64,
        price: f64,
        iattrs: &[i64],
    ) -> Result<(), Error>;

    fn update_customer<S>(
        &self,
        variant: UpdateCustomerVariant,
        update_frequent_flyer: bool,
        iattr0: i64,
        iattr1: i64,
    ) -> Result<(), Error>
    where
        S: AsRef<str> + Debug;

    fn update_reservation(
        &self,
        r_id: i64,
        c_id: i64,
        f_id: i64,
        seat: i64,
        iattr_index: usize,
        iattr: i64,
    ) -> Result<(), Error>;
}
