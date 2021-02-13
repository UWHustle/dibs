use crate::benchmarks::seats;
use std::fmt::Debug;

#[derive(Debug)]
pub enum Error {
    SeatReserved { f_id: i64, seat: i64 },
    NonexistentReservation { c_id: i64, f_id: i64 },
    InvalidOperation,
}

pub trait SEATSConnection {
    fn delete_reservation<S1, S2>(
        &self,
        flight_id: i64,
        customer_id: i64,
        customer_id_string: Option<S1>,
        frequent_flyer_customer_id_string: Option<S2>,
        frequent_flyer_airline_id: i64,
    ) -> Result<(), seats::Error>
    where
        S1: AsRef<str> + Debug,
        S2: AsRef<str> + Debug;

    fn find_flights(
        &self,
        depart_airport_id: i64,
        arrive_airport_id: i64,
        start_timestamp: i64,
        end_timestamp: i64,
        distance: i64,
    ) -> Result<(), Error>;

    fn find_open_seats(&self, f_id: i64) -> Result<Vec<(i64, i64, f64)>, Error>;

    fn new_reservation(
        &self,
        reservation_id: i64,
        customer_id: i64,
        flight_id: i64,
        seat_num: i64,
        price: f64,
        attrs: &[i64],
    ) -> Result<(), Error>;

    fn update_customer<S>(
        &self,
        customer_id: i64,
        customer_id_string: Option<S>,
        update_frequent_flyer: i64,
        attr0: i64,
        attr1: i64,
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
