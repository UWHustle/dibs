use crate::benchmarks::seats;
use crate::benchmarks::seats::Error::UserAbort;
use dibs::predicate::{ComparisonOperator, Predicate};
use dibs::{AcquireError, Dibs, OptimizationLevel, RequestTemplate};
use fnv::FnvHashSet;
use std::fmt::Debug;
use std::time::Duration;

const AIRPORT_TABLE_ID: usize = 1;
const AIRPORT_DISTANCE_TABLE_ID: usize = 2;
const AIRLINE_TABLE_ID: usize = 3;
const CUSTOMER_TABLE_ID: usize = 4;
const FREQUENT_FLYER_TABLE_ID: usize = 5;
const FLIGHT_TABLE_ID: usize = 6;
const RESERVATION_TABLE_ID: usize = 7;

pub const GET_AIRPORT_INFO_TEMPLATE_ID: usize = 0;
pub const GET_NEARBY_AIRPORTS_TEMPLATE_ID: usize = 1;
pub const GET_AIRLINE_NAME_TEMPLATE_ID: usize = 2;
pub const GET_CUSTOMER_ID_FROM_STR_TEMPLATE_ID: usize = 3;
pub const GET_CUSTOMER_ATTRIBUTE_TEMPLATE_ID: usize = 4;
pub const GET_CUSTOMER_BASE_AIRPORT_TEMPLATE_ID: usize = 5;
pub const UPDATE_CUSTOMER_DELETE_RESERVATION_TEMPLATE_ID: usize = 6;
pub const UPDATE_CUSTOMER_NEW_RESERVATION_TEMPLATE_ID: usize = 7;
pub const UPDATE_CUSTOMER_IATTRS_TEMPLATE_ID: usize = 8;
pub const GET_AIRLINE_IDS_TEMPLATE_ID: usize = 9;
pub const DECREMENT_IATTR_TEMPLATE_ID: usize = 10;
pub const SET_IATTRS_NEW_RESERVATION_TEMPLATE_ID: usize = 11;
pub const SET_IATTRS_UPDATE_CUSTOMER_TEMPLATE_ID: usize = 12;
pub const GET_SEATS_LEFT_TEMPLATE_ID: usize = 13;
pub const GET_AIRLINE_AND_SEATS_LEFT_TEMPLATE_ID: usize = 14;
pub const GET_PRICE_TEMPLATE_ID: usize = 15;
pub const GET_FLIGHTS_TEMPLATE_ID: usize = 16;
pub const INCREMENT_DECREMENT_SEATS_LEFT_TEMPLATE_ID: usize = 17;
pub const SEAT_IS_RESERVED_TEMPLATE_ID: usize = 18;
pub const CUSTOMER_HAS_RESERVATION_ON_FLIGHT_TEMPLATE_ID: usize = 19;
pub const GET_RESERVED_SEATS_ON_FLIGHT_TEMPLATE_ID: usize = 20;
pub const GET_RESERVATION_INFO_TEMPLATE_ID: usize = 21;
pub const UPDATE_RESERVATION_TEMPLATE_ID: usize = 22;
pub const INSERT_REMOVE_TEMPLATE_ID: usize = 23;

#[derive(Debug)]
pub enum Error {
    UserAbort(String),
    InvalidOperation,
}

impl From<dibs::AcquireError> for Error {
    fn from(e: AcquireError) -> Self {
        match e {
            AcquireError::Timeout(id) => UserAbort(format!("conflict timeout with request {}", id)),
            AcquireError::GroupConflict => UserAbort("group conflict".to_string()),
        }
    }
}

pub enum DeleteReservationVariant {
    CustomerId(i64),
    CustomerIdString(String),
    FrequentFlyer(String),
}

pub enum UpdateCustomerVariant {
    CustomerId(i64),
    CustomerIdString(String),
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
        distance: i64,
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

    fn update_customer(
        &self,
        variant: UpdateCustomerVariant,
        update_ff: bool,
        iattr0: i64,
        iattr1: i64,
    ) -> Result<(), Error>;

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

pub fn dibs(optimization: OptimizationLevel) -> Dibs {
    let filters = match optimization {
        OptimizationLevel::Filtered => &[
            None,
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(2),
        ],
        _ => &[None; 8],
    };

    let templates = vec![
        // (0) get_airport_info
        RequestTemplate::new(
            AIRPORT_TABLE_ID,
            // id, code, name, city, co_id
            [0, 1, 2, 3, 5].iter().copied().collect(),
            FnvHashSet::default(),
            // id = ?
            Predicate::equality(0, 0),
        ),
        // (1) get_nearby_airports
        RequestTemplate::new(
            AIRPORT_DISTANCE_TABLE_ID,
            // id0, id1, distance
            (0..=2).collect(),
            FnvHashSet::default(),
            // id0 = ? AND distance <= ?
            Predicate::conjunction(vec![
                Predicate::equality(0, 0),
                Predicate::comparison(ComparisonOperator::Le, 2, 1),
            ]),
        ),
        // (2) get_airline_name
        RequestTemplate::new(
            AIRLINE_TABLE_ID,
            // id, name
            [0, 4].iter().copied().collect(),
            FnvHashSet::default(),
            // id = ?
            Predicate::equality(0, 0),
        ),
        // (3) get_customer_id_from_str
        RequestTemplate::new(
            CUSTOMER_TABLE_ID,
            // id, id_str
            [0, 1].iter().copied().collect(),
            FnvHashSet::default(),
            // id_str = ?
            Predicate::equality(1, 0),
        ),
        // (4) get_customer_attribute
        RequestTemplate::new(
            CUSTOMER_TABLE_ID,
            // id, iattr00
            [0, 24].iter().copied().collect(),
            FnvHashSet::default(),
            // id = ?
            Predicate::equality(0, 0),
        ),
        // (5) get_customer_base_airport
        RequestTemplate::new(
            CUSTOMER_TABLE_ID,
            // id, base_ap_id
            [0, 2].iter().copied().collect(),
            FnvHashSet::default(),
            // id = ?
            Predicate::equality(0, 0),
        ),
        // (6) update_customer_delete_reservation
        RequestTemplate::new(
            CUSTOMER_TABLE_ID,
            // id
            [0].iter().copied().collect(),
            // balance, iattr00, iattr10, iattr11
            [3, 24, 34, 35].iter().copied().collect(),
            // id = ?
            Predicate::equality(0, 0),
        ),
        // (7) update_customer_new_reservation
        RequestTemplate::new(
            CUSTOMER_TABLE_ID,
            // id
            [0].iter().copied().collect(),
            // iattr10, iattr11, iattr12, iattr13, iattr14, iattr15
            (34..=39).collect(),
            // id = ?
            Predicate::equality(0, 0),
        ),
        // (8) update_customer_iattrs
        RequestTemplate::new(
            CUSTOMER_TABLE_ID,
            // id
            [0].iter().copied().collect(),
            // iattr00, iattr01
            (24..=25).collect(),
            // id = ?
            Predicate::equality(0, 0),
        ),
        // (9) get_airline_ids
        RequestTemplate::new(
            FREQUENT_FLYER_TABLE_ID,
            // c_id, al_id
            (0..=1).collect(),
            FnvHashSet::default(),
            // c_id = ?
            Predicate::equality(0, 0),
        ),
        // (10) decrement_iattr
        RequestTemplate::new(
            FREQUENT_FLYER_TABLE_ID,
            // c_id, al_id
            (0..=1).collect(),
            // iattr10
            [17].iter().copied().collect(),
            // c_id = ? AND al_id = ?
            Predicate::conjunction(vec![Predicate::equality(0, 0), Predicate::equality(1, 1)]),
        ),
        // (11) set_iattrs_new_reservation
        RequestTemplate::new(
            FREQUENT_FLYER_TABLE_ID,
            // c_id, al_id
            (0..=1).collect(),
            // iattr10, iattr11, iattr12, iattr13, iattr14
            (17..=21).collect(),
            // c_id = ? AND al_id = ?
            Predicate::conjunction(vec![Predicate::equality(0, 0), Predicate::equality(1, 1)]),
        ),
        // (12) set_iattrs_update_customer
        RequestTemplate::new(
            FREQUENT_FLYER_TABLE_ID,
            // c_id
            [0].iter().copied().collect(),
            // iattr00, iattr01
            (7..=8).collect(),
            // c_id = ?
            Predicate::equality(0, 0),
        ),
        // (13) get_seats_left
        RequestTemplate::new(
            FLIGHT_TABLE_ID,
            // id, seats_left
            [0, 9].iter().copied().collect(),
            FnvHashSet::default(),
            // id = ?
            Predicate::equality(0, 0),
        ),
        // (14) get_airline_and_seats_left
        RequestTemplate::new(
            FLIGHT_TABLE_ID,
            // id, al_id, seats_left
            [0, 1, 9].iter().copied().collect(),
            FnvHashSet::default(),
            // id = ?
            Predicate::equality(0, 0),
        ),
        // (15) get_price
        RequestTemplate::new(
            FLIGHT_TABLE_ID,
            // id, base_price, seats_total, seats_left
            [0, 7, 8, 9].iter().copied().collect(),
            FnvHashSet::default(),
            // id = ?
            Predicate::equality(0, 0),
        ),
        // (16) get_flights
        RequestTemplate::new(
            FLIGHT_TABLE_ID,
            // id, al_id, depart_ap_id, depart_time, arrive_ap_id, arrive_time, seats_left
            [0, 1, 2, 3, 4, 5, 9].iter().copied().collect(),
            FnvHashSet::default(),
            // depart_ap_id = ? AND depart_time >= ? AND depart_time <= ? AND arrive_ap_id IN (?)
            Predicate::conjunction(vec![
                Predicate::equality(2, 0),
                Predicate::comparison(ComparisonOperator::Ge, 3, 1),
                Predicate::comparison(ComparisonOperator::Le, 3, 2),
                // IN predicates are not yet supported.
            ]),
        ),
        // (17) increment/decrement_seats_left
        RequestTemplate::new(
            FLIGHT_TABLE_ID,
            // id
            [0].iter().copied().collect(),
            // seats_left
            [9].iter().copied().collect(),
            // id = ?
            Predicate::equality(0, 0),
        ),
        // (18) seat_is_reserved
        RequestTemplate::new(
            RESERVATION_TABLE_ID,
            // f_id, seat
            [2, 3].iter().copied().collect(),
            FnvHashSet::default(),
            // f_id = ? AND seat = ?
            Predicate::conjunction(vec![Predicate::equality(2, 0), Predicate::equality(3, 1)]),
        ),
        // (19) customer_has_reservation_on_flight
        RequestTemplate::new(
            RESERVATION_TABLE_ID,
            // c_id, f_id
            [1, 2].iter().copied().collect(),
            FnvHashSet::default(),
            // c_id = ? AND f_id = ?
            Predicate::conjunction(vec![Predicate::equality(1, 0), Predicate::equality(2, 1)]),
        ),
        // (20) get_reserved_seats_on_flight
        RequestTemplate::new(
            RESERVATION_TABLE_ID,
            // f_id, seat
            [2, 3].iter().copied().collect(),
            FnvHashSet::default(),
            // f_id = ?
            Predicate::equality(2, 0),
        ),
        // (21) get_reservation_info
        RequestTemplate::new(
            RESERVATION_TABLE_ID,
            // id, c_id, f_id, price
            [0, 1, 2, 4].iter().copied().collect(),
            FnvHashSet::default(),
            // c_id = ? AND f_id = ?
            Predicate::conjunction(vec![Predicate::equality(1, 0), Predicate::equality(2, 1)]),
        ),
        // (22) update_reservation
        RequestTemplate::new(
            RESERVATION_TABLE_ID,
            // id, c_id, f_id
            (0..=2).collect(),
            // seat, iattrXX
            [3, 5, 6, 7, 8, 9, 10, 11, 12, 13].iter().copied().collect(),
            // id = ? AND c_id = ? AND f_id = ?
            Predicate::conjunction((0..=2).map(|i| Predicate::equality(i, i)).collect()),
        ),
        // (23) insert/remove
        RequestTemplate::new(
            RESERVATION_TABLE_ID,
            FnvHashSet::default(),
            // [all columns]
            (0..=13).collect(),
            // id = ? AND c_id = ? AND f_id = ?
            Predicate::conjunction((0..=2).map(|i| Predicate::equality(i, i)).collect()),
        ),
    ];

    Dibs::new(
        filters,
        &templates,
        optimization,
        usize::max_value(),
        Duration::from_millis(100),
    )
}
