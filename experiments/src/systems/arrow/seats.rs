use crate::benchmarks::seats;
use crate::benchmarks::seats::{
    AirportInfo, DeleteReservationVariant, SEATSConnection, UpdateCustomerVariant,
};
use crate::systems::arrow::{BooleanArrayMut, Float64ArrayMut, Int64ArrayMut};
use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::csv;
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
use dibs::predicate::Value;
use dibs::{Dibs, OptimizationLevel, Transaction};
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryFrom;
use std::fmt::Debug;
use std::fs::File;
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

const BLOCK_CAPACITY: usize = 1024;
const NUM_PARTITIONS: usize = 1024;

const COUNTRY_RECORDS: usize = 248;
const AIRLINE_RECORDS: usize = 1250;
const AIRPORT_RECORDS: usize = 286;
const AIRPORT_DISTANCE_RECORDS: usize = 40755;
const CUSTOMER_RECORDS: usize = 1000000;
const FLIGHT_RECORDS: usize = 763951;
const FREQUENT_FLYER_RECORDS: usize = 2162434;
const RESERVATION_RECORDS: usize = 1144313;

#[derive(Debug)]
enum Error {
    DuplicateKey(String),
    NonexistentKey(String),
}

#[allow(dead_code)]
struct Country {
    id: Int64Array,
    name: StringArray,
    code_2: StringArray,
    code_3: StringArray,
    pk_index: HashMap<i64, usize>,
}

impl Country {
    fn new<P>(path: P) -> Country
    where
        P: AsRef<Path>,
    {
        println!("Loading COUNTRY...");

        let schema = Schema::new(vec![
            Field::new("CO_ID", DataType::Int64, false),
            Field::new("CO_NAME", DataType::Utf8, false),
            Field::new("CO_CODE_2", DataType::Utf8, false),
            Field::new("CO_CODE_3", DataType::Utf8, false),
        ]);

        let file = File::open(path).unwrap();
        let mut csv = csv::Reader::new(
            file,
            Arc::new(schema),
            true,
            None,
            COUNTRY_RECORDS,
            None,
            None,
        );

        let batch = csv.next().unwrap().unwrap();

        let id = Int64Array::from(batch.column(0).data());

        let pk_index = id
            .iter()
            .enumerate()
            .map(|(row_index, id_v)| (id_v.unwrap(), row_index))
            .collect();

        Country {
            id,
            name: StringArray::from(batch.column(1).data()),
            code_2: StringArray::from(batch.column(2).data()),
            code_3: StringArray::from(batch.column(2).data()),
            pk_index,
        }
    }
}

#[allow(dead_code)]
struct Airport {
    id: Int64Array,
    code: StringArray,
    name: StringArray,
    city: StringArray,
    postal_code: StringArray,
    co_id: Int64Array,
    longitude: Float64Array,
    latitude: Float64Array,
    gmt_offset: Float64Array,
    wac: Int64Array,
    iattrs: Vec<Int64Array>,
    pk_index: HashMap<i64, usize>,
}

impl Airport {
    fn new<P>(path: P) -> Airport
    where
        P: AsRef<Path>,
    {
        println!("Loading AIRPORT...");

        let mut fields = vec![
            Field::new("AP_ID", DataType::Int64, false),
            Field::new("AP_CODE", DataType::Utf8, false),
            Field::new("AP_NAME", DataType::Utf8, false),
            Field::new("AP_CITY", DataType::Utf8, false),
            Field::new("AP_POSTAL_CODE", DataType::Utf8, true),
            Field::new("AP_CO_ID", DataType::Int64, false),
            Field::new("AP_LONGITUDE", DataType::Float64, true),
            Field::new("AP_LATITUDE", DataType::Float64, true),
            Field::new("AP_GMT_OFFSET", DataType::Float64, true),
            Field::new("AP_WAC", DataType::Int64, true),
        ];

        for i in 0..16 {
            fields.push(Field::new(
                &format!("AP_IATTR{}{}", i / 10, i % 10),
                DataType::Int64,
                true,
            ));
        }

        let schema = Schema::new(fields);

        let file = File::open(path).unwrap();
        let mut csv = csv::Reader::new(
            file,
            Arc::new(schema),
            true,
            None,
            AIRPORT_RECORDS,
            None,
            None,
        );

        let batch = csv.next().unwrap().unwrap();

        let id = Int64Array::from(batch.column(0).data());

        let pk_index = id
            .iter()
            .enumerate()
            .map(|(row_index, id_v)| (id_v.unwrap(), row_index))
            .collect();

        Airport {
            id,
            code: StringArray::from(batch.column(1).data()),
            name: StringArray::from(batch.column(2).data()),
            city: StringArray::from(batch.column(3).data()),
            postal_code: StringArray::from(batch.column(4).data()),
            co_id: Int64Array::from(batch.column(5).data()),
            longitude: Float64Array::from(batch.column(6).data()),
            latitude: Float64Array::from(batch.column(7).data()),
            gmt_offset: Float64Array::from(batch.column(8).data()),
            wac: Int64Array::from(batch.column(9).data()),
            iattrs: (10..26)
                .map(|i| Int64Array::from(batch.column(i).data()))
                .collect::<Vec<_>>(),
            pk_index,
        }
    }

    fn get_airport_info(&self, id: i64) -> (&str, &str, &str, i64) {
        let row_index = self.pk_index[&id];
        assert_eq!(self.id.value(row_index), id);

        (
            self.code.value(row_index),
            self.name.value(row_index),
            self.city.value(row_index),
            self.co_id.value(row_index),
        )
    }
}

#[allow(dead_code)]
struct AirportDistance {
    id0: Int64Array,
    id1: Int64Array,
    distance: Float64Array,
    pk_index: HashMap<i64, HashMap<i64, usize>>,
}

impl AirportDistance {
    fn new<P>(path: P) -> AirportDistance
    where
        P: AsRef<Path>,
    {
        println!("Loading AIRPORT_DISTANCE...");

        let schema = Schema::new(vec![
            Field::new("D_AP_ID0", DataType::Int64, false),
            Field::new("D_AP_ID1", DataType::Int64, false),
            Field::new("D_DISTANCE", DataType::Float64, false),
        ]);

        let file = File::open(path).unwrap();
        let mut csv = csv::Reader::new(
            file,
            Arc::new(schema),
            true,
            None,
            AIRPORT_DISTANCE_RECORDS,
            None,
            None,
        );

        let batch = csv.next().unwrap().unwrap();

        let id0 = Int64Array::from(batch.column(0).data());
        let id1 = Int64Array::from(batch.column(1).data());

        let mut pk_index = HashMap::new();

        for (row_index, (id0_v, id1_v)) in id0.iter().zip(id1.iter()).enumerate() {
            pk_index
                .entry(id0_v.unwrap())
                .or_insert(HashMap::new())
                .insert(id1_v.unwrap(), row_index);
        }

        AirportDistance {
            id0,
            id1,
            distance: Float64Array::from(batch.column(2).data()),
            pk_index,
        }
    }

    fn get_nearby_airports(&self, id0: i64, distance: f64) -> Vec<i64> {
        let mut connected_airports = self
            .pk_index
            .get(&id0)
            .map(|m_id1| {
                m_id1
                    .iter()
                    .filter_map(|(&id1, &row_index)| {
                        assert_eq!(self.id0.value(row_index), id0);
                        assert_eq!(self.id1.value(row_index), id1);

                        let other_distance = self.distance.value(row_index);
                        if other_distance <= distance {
                            Some((id1, distance))
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or(vec![]);

        connected_airports.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        connected_airports.iter().map(|&(id1, _)| id1).collect()
    }
}

#[allow(dead_code)]
struct Airline {
    id: Int64Array,
    iata_code: StringArray,
    icao_code: StringArray,
    call_sign: StringArray,
    name: StringArray,
    co_id: Int64Array,
    iattrs: Vec<Int64Array>,
    pk_index: HashMap<i64, usize>,
}

impl Airline {
    pub fn new<P>(path: P) -> Airline
    where
        P: AsRef<Path>,
    {
        println!("Loading AIRLINE...");

        let mut fields = vec![
            Field::new("AL_ID", DataType::Int64, false),
            Field::new("AL_IATA_CODE", DataType::Utf8, true),
            Field::new("AL_ICAO_CODE", DataType::Utf8, true),
            Field::new("AL_CALL_SIGN", DataType::Utf8, true),
            Field::new("AL_NAME", DataType::Utf8, false),
            Field::new("AL_CO_ID", DataType::Int64, false),
        ];

        for i in 0..16 {
            fields.push(Field::new(
                &format!("AL_IATTR{}{}", i / 10, i % 10),
                DataType::Int64,
                true,
            ));
        }

        let schema = Schema::new(fields);

        let file = File::open(path).unwrap();
        let mut csv = csv::Reader::new(
            file,
            Arc::new(schema),
            true,
            None,
            AIRLINE_RECORDS,
            None,
            None,
        );

        let batch = csv.next().unwrap().unwrap();

        let id = Int64Array::from(batch.column(0).data());

        let pk_index = id
            .iter()
            .enumerate()
            .map(|(row_index, id_v)| (id_v.unwrap(), row_index))
            .collect();

        Airline {
            id,
            iata_code: StringArray::from(batch.column(1).data()),
            icao_code: StringArray::from(batch.column(2).data()),
            call_sign: StringArray::from(batch.column(3).data()),
            name: StringArray::from(batch.column(4).data()),
            co_id: Int64Array::from(batch.column(5).data()),
            iattrs: (6..22)
                .map(|i| Int64Array::from(batch.column(i).data()))
                .collect::<Vec<_>>(),
            pk_index,
        }
    }

    fn get_airline_name(&self, id: i64) -> &str {
        let row_index = self.pk_index[&id];
        assert_eq!(self.id.value(row_index), id);

        self.name.value(row_index)
    }
}

#[allow(dead_code)]
struct Customer {
    id: Int64Array,
    id_str: StringArray,
    base_ap_id: Int64Array,
    balance: Float64ArrayMut,
    sattrs: Vec<StringArray>,
    iattrs: Vec<Int64ArrayMut>,
    pk_index: HashMap<i64, usize>,
    id_str_index: HashMap<String, usize>,
}

impl Customer {
    fn new<P>(path: P) -> Customer
    where
        P: AsRef<Path>,
    {
        println!("Loading CUSTOMER...");

        let mut fields = vec![
            Field::new("C_ID", DataType::Int64, false),
            Field::new("C_ID_STR", DataType::Utf8, false),
            Field::new("C_BASE_AP_ID", DataType::Int64, true),
            Field::new("C_BALANCE", DataType::Float64, false),
        ];

        for i in 0..20 {
            fields.push(Field::new(
                &format!("C_SATTR{}{}", i / 10, i % 10),
                DataType::Utf8,
                true,
            ));
        }

        for i in 0..20 {
            fields.push(Field::new(
                &format!("C_IATTR{}{}", i / 10, i % 10),
                DataType::Int64,
                true,
            ));
        }

        let schema = Schema::new(fields);

        let file = File::open(path).unwrap();
        let mut csv = csv::Reader::new(
            file,
            Arc::new(schema),
            true,
            None,
            CUSTOMER_RECORDS,
            None,
            None,
        );

        let batch = csv.next().unwrap().unwrap();

        let id = Int64Array::from(batch.column(0).data());
        let id_str = StringArray::from(batch.column(1).data());

        let pk_index = id
            .iter()
            .enumerate()
            .map(|(row_index, id_v)| (id_v.unwrap(), row_index))
            .collect();

        let id_str_index = (0..id_str.len())
            .map(|row_index| (id_str.value(row_index).to_string(), row_index))
            .collect();

        Customer {
            id,
            id_str,
            base_ap_id: Int64Array::from(batch.column(2).data()),
            balance: Float64ArrayMut(Float64Array::from(batch.column(3).data())),
            sattrs: (4..24)
                .map(|i| StringArray::from(batch.column(i).data()))
                .collect::<Vec<_>>(),
            iattrs: (24..44)
                .map(|i| Int64ArrayMut(Int64Array::from(batch.column(i).data())))
                .collect::<Vec<_>>(),
            pk_index,
            id_str_index,
        }
    }

    fn get_customer_id_from_str(&self, c_id_str: &str) -> Option<i64> {
        self.id_str_index.get(c_id_str).map(|&row_index| {
            assert_eq!(self.id_str.value(row_index), c_id_str);
            self.id.value(row_index)
        })
    }

    fn get_customer_attribute(&self, c_id: i64) -> Option<i64> {
        self.pk_index.get(&c_id).map(|&row_index| {
            assert_eq!(self.id.value(row_index), c_id);
            self.iattrs[0].0.value(row_index)
        })
    }

    fn update_customer_delete_reservation(&self, c_id: i64, balance: f64, iattr00: i64) {
        let row_index = self.pk_index[&c_id];
        assert_eq!(self.id.value(row_index), c_id);

        unsafe {
            self.balance
                .set(row_index, self.balance.0.value(row_index) + balance);
            self.iattrs[0].set(row_index, iattr00);
            self.iattrs[10].set(row_index, self.iattrs[10].0.value(row_index) - 1);
            self.iattrs[11].set(row_index, self.iattrs[11].0.value(row_index) - 1);
        }
    }

    fn update_customer_new_reservation(
        &self,
        c_id: i64,
        iattr12: i64,
        iattr13: i64,
        iattr14: i64,
        iattr15: i64,
    ) {
        if let Some(&row_index) = self.pk_index.get(&c_id) {
            assert_eq!(self.id.value(row_index), c_id);

            unsafe {
                self.iattrs[10].set(row_index, self.iattrs[10].0.value(row_index) + 1);
                self.iattrs[11].set(row_index, self.iattrs[11].0.value(row_index) + 1);
                self.iattrs[12].set(row_index, iattr12);
                self.iattrs[13].set(row_index, iattr13);
                self.iattrs[14].set(row_index, iattr14);
                self.iattrs[15].set(row_index, iattr15);
            }
        }
    }

    fn update_customer_iattrs(&self, c_id: i64, iattr00: i64, iattr01: i64) {
        if let Some(&row_index) = self.pk_index.get(&c_id) {
            assert_eq!(self.id.value(row_index), c_id);

            unsafe {
                self.iattrs[0].set(row_index, iattr00);
                self.iattrs[1].set(row_index, iattr01);
            }
        }
    }
}

#[allow(dead_code)]
struct FrequentFlyer {
    c_id: Int64Array,
    al_id: Int64Array,
    c_id_str: StringArray,
    sattrs: Vec<StringArray>,
    iattrs: Vec<Int64ArrayMut>,
    pk_index: HashMap<i64, HashMap<i64, usize>>,
}

impl FrequentFlyer {
    fn new<P>(path: P) -> FrequentFlyer
    where
        P: AsRef<Path>,
    {
        println!("Loading FREQUENT_FLYER...");

        let mut fields = vec![
            Field::new("FF_C_ID", DataType::Int64, false),
            Field::new("FF_AL_ID", DataType::Int64, false),
            Field::new("FF_C_ID_STR", DataType::Utf8, false),
        ];

        for i in 0..4 {
            fields.push(Field::new(&format!("FF_SATTR0{}", i), DataType::Utf8, true));
        }

        for i in 0..16 {
            fields.push(Field::new(
                &format!("FF_IATTR{}{}", i / 10, i % 10),
                DataType::Int64,
                true,
            ));
        }

        let schema = Schema::new(fields);

        let file = File::open(path).unwrap();
        let mut csv = csv::Reader::new(
            file,
            Arc::new(schema),
            true,
            None,
            FREQUENT_FLYER_RECORDS,
            None,
            None,
        );

        let batch = csv.next().unwrap().unwrap();

        let c_id = Int64Array::from(batch.column(0).data());
        let al_id = Int64Array::from(batch.column(1).data());

        let mut pk_index = HashMap::new();

        for (row_index, (c_id_v, al_id_v)) in c_id.iter().zip(al_id.iter()).enumerate() {
            pk_index
                .entry(c_id_v.unwrap())
                .or_insert(HashMap::new())
                .insert(al_id_v.unwrap(), row_index);
        }

        FrequentFlyer {
            c_id,
            al_id,
            c_id_str: StringArray::from(batch.column(2).data()),
            sattrs: (3..7)
                .map(|i| StringArray::from(batch.column(i).data()))
                .collect::<Vec<_>>(),
            iattrs: (7..23)
                .map(|i| Int64ArrayMut(Int64Array::from(batch.column(i).data())))
                .collect::<Vec<_>>(),
            pk_index,
        }
    }

    fn get_airline_ids(&self, c_id: i64) -> Vec<i64> {
        self.pk_index
            .get(&c_id)
            .map(|m_al_id| m_al_id.keys().copied().collect())
            .unwrap_or_default()
    }

    fn decrement_iattr(&self, c_id: i64, al_id: i64) {
        let row_index = self.pk_index[&c_id][&al_id];
        assert_eq!(self.c_id.value(row_index), c_id);
        assert_eq!(self.al_id.value(row_index), al_id);

        unsafe {
            self.iattrs[10].set(row_index, self.iattrs[10].0.value(row_index) - 1);
        }
    }

    fn set_iattrs_new_reservation(
        &self,
        c_id: i64,
        al_id: i64,
        iattr11: i64,
        iattr12: i64,
        iattr13: i64,
        iattr14: i64,
    ) {
        if let Some(&row_index) = self
            .pk_index
            .get(&c_id)
            .and_then(|m_al_id| m_al_id.get(&al_id))
        {
            assert_eq!(self.c_id.value(row_index), c_id);
            assert_eq!(self.al_id.value(row_index), al_id);

            unsafe {
                self.iattrs[10].set(row_index, self.iattrs[10].0.value(row_index) + 1);
                self.iattrs[11].set(row_index, iattr11);
                self.iattrs[12].set(row_index, iattr12);
                self.iattrs[13].set(row_index, iattr13);
                self.iattrs[14].set(row_index, iattr14);
            }
        }
    }

    fn set_iattrs_update_customer(&self, c_id: i64, iattr00: i64, iattr01: i64) {
        if let Some(m_al_id) = self.pk_index.get(&c_id) {
            for (&al_id, &row_index) in m_al_id {
                assert_eq!(self.c_id.value(row_index), c_id);
                assert_eq!(self.al_id.value(row_index), al_id);

                unsafe {
                    self.iattrs[0].set(row_index, iattr00);
                    self.iattrs[1].set(row_index, iattr01);
                }
            }
        }
    }
}

struct FlightInfo {
    id: i64,
    al_id: i64,
    seats_left: i64,
    _depart_ap_id: i64,
    depart_time: i64,
    _arrive_ap_id: i64,
    arrive_time: i64,
}

#[allow(dead_code)]
struct Flight {
    id: Int64Array,
    al_id: Int64Array,
    depart_ap_id: Int64Array,
    depart_time: Int64Array,
    arrive_ap_id: Int64Array,
    arrive_time: Int64Array,
    status: Int64Array,
    base_price: Float64Array,
    seats_total: Int64Array,
    seats_left: Int64ArrayMut,
    iattrs: Vec<Int64Array>,
    pk_index: HashMap<i64, usize>,
    depart_time_index: BTreeMap<i64, usize>,
}

impl Flight {
    fn new<P>(path: P) -> Flight
    where
        P: AsRef<Path>,
    {
        println!("Loading FLIGHT...");

        let mut fields = vec![
            Field::new("F_ID", DataType::Int64, false),
            Field::new("F_AL_ID", DataType::Int64, false),
            Field::new("F_DEPART_AP_ID", DataType::Int64, false),
            Field::new("F_DEPART_TIME", DataType::Int64, false),
            Field::new("F_ARRIVE_AP_ID", DataType::Int64, false),
            Field::new("F_ARRIVE_TIME", DataType::Int64, false),
            Field::new("F_STATUS", DataType::Int64, false),
            Field::new("F_BASE_PRICE", DataType::Float64, false),
            Field::new("F_SEATS_TOTAL", DataType::Int64, false),
            Field::new("F_SEATS_LEFT", DataType::Int64, false),
        ];

        for i in 0..30 {
            fields.push(Field::new(
                &format!("F_IATTR{}{}", i / 10, i % 10),
                DataType::Int64,
                true,
            ));
        }

        let schema = Schema::new(fields);

        let file = File::open(path).unwrap();
        let mut csv = csv::Reader::new(
            file,
            Arc::new(schema),
            true,
            None,
            FLIGHT_RECORDS,
            None,
            None,
        );

        let batch = csv.next().unwrap().unwrap();

        let id = Int64Array::from(batch.column(0).data());
        let depart_time = Int64Array::from(batch.column(3).data());

        let pk_index = id
            .iter()
            .enumerate()
            .map(|(i, id)| (id.unwrap(), i))
            .collect();

        let depart_time_index = depart_time
            .iter()
            .enumerate()
            .map(|(i, id)| (id.unwrap(), i))
            .collect();

        Flight {
            id,
            al_id: Int64Array::from(batch.column(1).data()),
            depart_ap_id: Int64Array::from(batch.column(2).data()),
            depart_time,
            arrive_ap_id: Int64Array::from(batch.column(4).data()),
            arrive_time: Int64Array::from(batch.column(5).data()),
            status: Int64Array::from(batch.column(6).data()),
            base_price: Float64Array::from(batch.column(7).data()),
            seats_total: Int64Array::from(batch.column(8).data()),
            seats_left: Int64ArrayMut(Int64Array::from(batch.column(9).data())),
            iattrs: (10..40)
                .map(|i| Int64Array::from(batch.column(i).data()))
                .collect::<Vec<_>>(),
            pk_index,
            depart_time_index,
        }
    }

    fn get_airline_and_seats_left(&self, id: i64) -> Option<(i64, i64)> {
        self.pk_index.get(&id).map(|&row_index| {
            assert_eq!(self.id.value(row_index), id);

            (
                self.al_id.value(row_index),
                self.seats_left.0.value(row_index),
            )
        })
    }

    fn get_price(&self, id: i64) -> Option<f64> {
        self.pk_index.get(&id).map(|&row_index| {
            assert_eq!(self.id.value(row_index), id);

            let base_price = self.base_price.value(row_index);
            let seats_total = self.seats_total.value(row_index);
            let seats_left = self.seats_left.0.value(row_index);
            base_price + (base_price * (1.0 - (seats_left as f64 / seats_total as f64)))
        })
    }

    fn get_flights(
        &self,
        depart_ap_id: i64,
        depart_time_a: i64,
        depart_time_b: i64,
        arrive_ap_id: &HashSet<i64>,
    ) -> Vec<FlightInfo> {
        self.depart_time_index
            .range((
                Bound::Included(&depart_time_a),
                Bound::Included(&depart_time_b),
            ))
            .filter_map(|(&depart_time, &row_index)| {
                assert!(
                    self.depart_time.value(row_index) >= depart_time_a
                        && self.depart_time.value(row_index) <= depart_time_b
                );

                if self.depart_ap_id.value(row_index) == depart_ap_id
                    && arrive_ap_id.contains(&self.arrive_ap_id.value(row_index))
                {
                    Some(FlightInfo {
                        id: self.id.value(row_index),
                        al_id: self.al_id.value(row_index),
                        seats_left: self.seats_left.0.value(row_index),
                        _depart_ap_id: depart_ap_id,
                        depart_time,
                        _arrive_ap_id: self.arrive_ap_id.value(row_index),
                        arrive_time: self.arrive_time.value(row_index),
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    fn increment_seats_left(&self, id: i64) {
        let row_index = self.pk_index[&id];
        assert_eq!(self.id.value(row_index), id);

        let seats_left = self.seats_left.0.value(row_index);
        unsafe {
            self.seats_left.set(row_index, seats_left + 1);
        }
    }

    fn decrement_seats_left(&self, id: i64) {
        let row_index = self.pk_index[&id];
        assert_eq!(self.id.value(row_index), id);

        let seats_left = self.seats_left.0.value(row_index);
        unsafe {
            self.seats_left.set(row_index, seats_left - 1);
        }
    }
}

struct ReservationBlock {
    valid: BooleanArrayMut,
    id: Int64ArrayMut,
    c_id: Int64ArrayMut,
    f_id: Int64ArrayMut,
    seat: Int64ArrayMut,
    price: Float64ArrayMut,
    iattrs: Vec<Int64ArrayMut>,
}

impl ReservationBlock {
    fn new() -> ReservationBlock {
        ReservationBlock {
            valid: BooleanArrayMut(BooleanArray::from(vec![false; BLOCK_CAPACITY])),
            id: Int64ArrayMut(Int64Array::from(vec![i64::default(); BLOCK_CAPACITY])),
            c_id: Int64ArrayMut(Int64Array::from(vec![i64::default(); BLOCK_CAPACITY])),
            f_id: Int64ArrayMut(Int64Array::from(vec![i64::default(); BLOCK_CAPACITY])),
            seat: Int64ArrayMut(Int64Array::from(vec![i64::default(); BLOCK_CAPACITY])),
            price: Float64ArrayMut(Float64Array::from(vec![f64::default(); BLOCK_CAPACITY])),
            iattrs: (0..9)
                .map(|_| Int64ArrayMut(Int64Array::from(vec![i64::default(); BLOCK_CAPACITY])))
                .collect(),
        }
    }
}

struct ReservationPartition {
    blocks: Vec<ReservationBlock>,
    pk_index: HashMap<i64, HashMap<i64, HashMap<i64, (usize, usize)>>>,
    free: Vec<(usize, usize)>,
}

impl ReservationPartition {
    fn new() -> ReservationPartition {
        ReservationPartition {
            blocks: vec![],
            pk_index: HashMap::new(),
            free: vec![],
        }
    }

    fn seat_is_reserved(&self, f_id: i64, seat: i64) -> bool {
        self.pk_index
            .get(&f_id)
            .map(|m_c_id| {
                m_c_id.values().any(|m_id| {
                    m_id.values().any(|&(block_index, row_index)| {
                        self.blocks[block_index].seat.0.value(row_index) == seat
                    })
                })
            })
            .unwrap_or(false)
    }

    fn customer_has_reservation_on_flight(&self, c_id: i64, f_id: i64) -> bool {
        self.pk_index
            .get(&f_id)
            .map(|m_c_id| m_c_id.contains_key(&c_id))
            .unwrap_or(false)
    }

    fn get_reserved_seats_on_flight(&self, f_id: i64) -> Vec<i64> {
        self.pk_index
            .get(&f_id)
            .map(|m_c_id| {
                m_c_id
                    .iter()
                    .flat_map(|(&c_id, m_id)| {
                        m_id.iter().map(move |(&id, &(block_index, row_index))| {
                            let block = &self.blocks[block_index];

                            assert!(block.valid.0.value(row_index));
                            assert_eq!(block.id.0.value(row_index), id);
                            assert_eq!(block.c_id.0.value(row_index), c_id);
                            assert_eq!(block.f_id.0.value(row_index), f_id);

                            block.seat.0.value(row_index)
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Returns (r_id, price)
    fn get_reservation_info(&self, c_id: i64, f_id: i64) -> Option<(i64, f64)> {
        self.pk_index
            .get(&f_id)
            .and_then(|m_c_id| m_c_id.get(&c_id))
            .and_then(|m_id| {
                let (&id, &(block_index, row_index)) = m_id.iter().next()?;

                let block = &self.blocks[block_index];

                assert!(block.valid.0.value(row_index));
                assert_eq!(block.id.0.value(row_index), id);
                assert_eq!(block.c_id.0.value(row_index), c_id);
                assert_eq!(block.f_id.0.value(row_index), f_id);

                Some((id, block.price.0.value(row_index)))
            })
    }

    fn update_reservation(
        &mut self,
        id: i64,
        c_id: i64,
        f_id: i64,
        seat: i64,
        iattr_index: usize,
        iattr: i64,
    ) -> Result<(), Error> {
        let &(block_index, row_index) = self
            .pk_index
            .get(&f_id)
            .and_then(|m_c_id| m_c_id.get(&c_id))
            .and_then(|m_id| m_id.get(&id))
            .ok_or(Error::NonexistentKey(format!(
                "id: {}, c_id: {}, f_id: {}",
                id, c_id, f_id
            )))?;

        let block = &self.blocks[block_index];

        assert!(block.valid.0.value(row_index));
        assert_eq!(block.id.0.value(row_index), id);
        assert_eq!(block.c_id.0.value(row_index), c_id);
        assert_eq!(block.f_id.0.value(row_index), f_id);

        unsafe {
            block.seat.set(row_index, seat);
            block.iattrs[iattr_index].set(row_index, iattr);
        }

        Ok(())
    }

    fn insert(
        &mut self,
        id: i64,
        c_id: i64,
        f_id: i64,
        seat: i64,
        price: f64,
        iattrs: &[i64],
    ) -> Result<(), Error> {
        match self
            .pk_index
            .entry(f_id)
            .or_default()
            .entry(c_id)
            .or_default()
            .entry(id)
        {
            Entry::Occupied(_) => Err(Error::DuplicateKey(format!(
                "id: {}, c_id: {}, f_id: {}",
                id, c_id, f_id
            ))),
            Entry::Vacant(entry) => {
                if self.free.is_empty() {
                    let block_index = self.blocks.len();
                    self.blocks.push(ReservationBlock::new());
                    for row_index in 0..BLOCK_CAPACITY {
                        self.free.push((block_index, row_index));
                    }
                }

                let (block_index, row_index) = self.free.pop().unwrap();
                let block = &self.blocks[block_index];

                assert_eq!(iattrs.len(), block.iattrs.len());
                assert!(!block.valid.0.value(row_index));

                unsafe {
                    block.valid.set(row_index);
                    block.id.set(row_index, id);
                    block.c_id.set(row_index, c_id);
                    block.f_id.set(row_index, f_id);
                    block.seat.set(row_index, seat);
                    block.price.set(row_index, price);
                    for (dst, &src) in block.iattrs.iter().zip(iattrs) {
                        dst.set(row_index, src);
                    }
                }

                entry.insert((block_index, row_index));

                Ok(())
            }
        }
    }

    fn remove(&mut self, id: i64, c_id: i64, f_id: i64) -> Result<(), Error> {
        match self
            .pk_index
            .get_mut(&f_id)
            .and_then(|m_c_id| m_c_id.get_mut(&c_id))
            .and_then(|m_id| m_id.remove(&id))
        {
            None => Err(Error::NonexistentKey(format!(
                "id: {}, c_id: {}, f_id: {}",
                id, c_id, f_id
            ))),
            Some((block_index, row_index)) => {
                let block = &self.blocks[block_index];

                assert!(block.valid.0.value(row_index));

                unsafe {
                    block.valid.clear(row_index);
                }

                self.free.push((block_index, row_index));

                Ok(())
            }
        }
    }
}

struct Reservation {
    partitions: Vec<Mutex<ReservationPartition>>,
}

impl Reservation {
    fn new<P>(path: P) -> Reservation
    where
        P: AsRef<Path>,
    {
        println!("Loading RESERVATION...");

        let partitions = (0..NUM_PARTITIONS)
            .map(|_| Mutex::new(ReservationPartition::new()))
            .collect();

        let reservation = Reservation { partitions };

        let mut fields = vec![
            Field::new("R_ID", DataType::Int64, false),
            Field::new("R_C_ID", DataType::Int64, false),
            Field::new("R_F_ID", DataType::Int64, false),
            Field::new("R_SEAT", DataType::Int64, false),
            Field::new("R_PRICE", DataType::Float64, false),
        ];

        for i in 0..9 {
            fields.push(Field::new(
                &format!("R_IATTR0{}", i % 10),
                DataType::Int64,
                true,
            ));
        }

        let schema = Schema::new(fields);

        let file = File::open(path).unwrap();
        let mut csv = csv::Reader::new(
            file,
            Arc::new(schema),
            true,
            None,
            RESERVATION_RECORDS,
            None,
            None,
        );

        let batch = csv.next().unwrap().unwrap();

        for i in 0..RESERVATION_RECORDS {
            let id = arrow::array::as_primitive_array::<Int64Type>(batch.column(0)).value(i);
            let c_id = arrow::array::as_primitive_array::<Int64Type>(batch.column(1)).value(i);
            let f_id = arrow::array::as_primitive_array::<Int64Type>(batch.column(2)).value(i);
            let seat = arrow::array::as_primitive_array::<Int64Type>(batch.column(3)).value(i);
            let price = arrow::array::as_primitive_array::<Float64Type>(batch.column(4)).value(i);

            let iattrs = (5..14)
                .map(|j| arrow::array::as_primitive_array::<Int64Type>(batch.column(j)).value(i))
                .collect::<Vec<_>>();

            reservation
                .insert(id, c_id, f_id, seat, price, &iattrs)
                .unwrap();
        }

        reservation
    }

    fn seat_is_reserved(&self, f_id: i64, seat: i64) -> bool {
        self.get_partition(f_id).seat_is_reserved(f_id, seat)
    }

    fn customer_has_reservation_on_flight(&self, c_id: i64, f_id: i64) -> bool {
        self.get_partition(f_id)
            .customer_has_reservation_on_flight(c_id, f_id)
    }

    fn get_reserved_seats_on_flight(&self, f_id: i64) -> Vec<i64> {
        self.get_partition(f_id).get_reserved_seats_on_flight(f_id)
    }

    fn get_reservation_info(&self, c_id: i64, f_id: i64) -> Option<(i64, f64)> {
        self.get_partition(f_id).get_reservation_info(c_id, f_id)
    }

    fn update_reservation(
        &self,
        id: i64,
        c_id: i64,
        f_id: i64,
        seat: i64,
        iattr_index: usize,
        iattr: i64,
    ) -> Result<(), Error> {
        self.get_partition(f_id)
            .update_reservation(id, c_id, f_id, seat, iattr_index, iattr)
    }

    fn insert(
        &self,
        id: i64,
        c_id: i64,
        f_id: i64,
        seat: i64,
        price: f64,
        iattrs: &[i64],
    ) -> Result<(), Error> {
        self.get_partition(f_id)
            .insert(id, c_id, f_id, seat, price, iattrs)
    }

    fn remove(&self, id: i64, c_id: i64, f_id: i64) -> Result<(), Error> {
        self.get_partition(f_id).remove(id, c_id, f_id)
    }

    fn get_partition(&self, f_id: i64) -> MutexGuard<ReservationPartition> {
        self.partitions[usize::try_from(f_id).unwrap() % self.partitions.len()]
            .lock()
            .unwrap()
    }
}

pub struct Database {
    _country: Country,
    airport: Airport,
    airport_distance: AirportDistance,
    airline: Airline,
    customer: Customer,
    frequent_flyer: FrequentFlyer,
    flight: Flight,
    reservation: Reservation,
    dibs: Dibs,
    transaction_counter: AtomicUsize,
}

impl Database {
    pub fn new(optimization: OptimizationLevel) -> Database {
        let country = Country::new("/users/gaffneyk/data/country.csv");
        let airport = Airport::new("/users/gaffneyk/data/airport.csv");
        let airport_distance = AirportDistance::new("/users/gaffneyk/data/airport_distance.csv");
        let airline = Airline::new("/users/gaffneyk/data/airline.csv");
        let customer = Customer::new("/users/gaffneyk/data/customer.csv");
        let frequent_flyer = FrequentFlyer::new("/users/gaffneyk/data/frequent_flyer.csv");
        let flight = Flight::new("/users/gaffneyk/data/flight.csv");
        let reservation = Reservation::new("/users/gaffneyk/data/reservation.csv");
        let dibs = seats::dibs(optimization);
        let transaction_counter = AtomicUsize::new(0);

        Database {
            _country: country,
            airport,
            airport_distance,
            airline,
            customer,
            frequent_flyer,
            flight,
            reservation,
            dibs,
            transaction_counter,
        }
    }

    pub fn hello(&self) {}

    fn new_transaction(&self) -> Transaction {
        let transaction_id = self.transaction_counter.fetch_add(1, Ordering::Relaxed);
        Transaction::new(transaction_id, transaction_id)
    }
}

impl SEATSConnection for Database {
    fn delete_reservation(
        &self,
        variant: DeleteReservationVariant,
        f_id: i64,
    ) -> Result<(), seats::Error> {
        let mut transaction = self.new_transaction();

        let (c_id, ff_al_id) = match variant {
            DeleteReservationVariant::CustomerId(c_id) => (c_id, None),
            DeleteReservationVariant::CustomerIdString(c_id_str) => {
                self.dibs.acquire(
                    &mut transaction,
                    seats::GET_CUSTOMER_ID_FROM_STR_TEMPLATE_ID,
                    vec![Value::String(c_id_str.to_string())],
                )?;

                let c_id = self.customer.get_customer_id_from_str(&c_id_str).ok_or(
                    seats::Error::UserAbort(format!("customer {} not found", c_id_str)),
                )?;

                (c_id, None)
            }
            DeleteReservationVariant::FrequentFlyer(ff_c_id_str) => {
                self.dibs.acquire(
                    &mut transaction,
                    seats::GET_CUSTOMER_ID_FROM_STR_TEMPLATE_ID,
                    vec![Value::String(ff_c_id_str.to_string())],
                )?;

                let c_id = self.customer.get_customer_id_from_str(&ff_c_id_str).ok_or(
                    seats::Error::UserAbort(format!("customer {} not found", ff_c_id_str)),
                )?;

                self.dibs.acquire(
                    &mut transaction,
                    seats::GET_AIRLINE_IDS_TEMPLATE_ID,
                    vec![Value::I64(c_id)],
                )?;

                let ff_al_id = self.frequent_flyer.get_airline_ids(c_id).first().copied();

                (c_id, ff_al_id)
            }
        };

        self.dibs.acquire(
            &mut transaction,
            seats::GET_CUSTOMER_ATTRIBUTE_TEMPLATE_ID,
            vec![Value::I64(c_id)],
        )?;

        let c_iattr00 =
            self.customer
                .get_customer_attribute(c_id)
                .ok_or(seats::Error::UserAbort(format!(
                    "customer {} not found",
                    c_id
                )))?;

        self.dibs.acquire(
            &mut transaction,
            seats::GET_RESERVATION_INFO_TEMPLATE_ID,
            vec![Value::I64(c_id), Value::I64(f_id)],
        )?;

        let (r_id, price) =
            self.reservation
                .get_reservation_info(c_id, f_id)
                .ok_or(seats::Error::UserAbort(format!(
                    "no reservation for customer {} on flight {}",
                    c_id, f_id
                )))?;

        self.dibs.acquire(
            &mut transaction,
            seats::INSERT_REMOVE_TEMPLATE_ID,
            vec![Value::I64(r_id), Value::I64(c_id), Value::I64(f_id)],
        )?;

        self.reservation
            .remove(r_id, c_id, f_id)
            .map_err(|_| seats::Error::InvalidOperation)?;

        self.dibs.acquire(
            &mut transaction,
            seats::INCREMENT_DECREMENT_SEATS_LEFT_TEMPLATE_ID,
            vec![Value::I64(f_id)],
        )?;

        self.flight.increment_seats_left(f_id);

        self.dibs.acquire(
            &mut transaction,
            seats::UPDATE_CUSTOMER_DELETE_RESERVATION_TEMPLATE_ID,
            vec![Value::I64(c_id)],
        )?;

        self.customer
            .update_customer_delete_reservation(c_id, -price, c_iattr00);

        if let Some(ff_al_id) = ff_al_id {
            self.dibs.acquire(
                &mut transaction,
                seats::DECREMENT_IATTR_TEMPLATE_ID,
                vec![Value::I64(c_id), Value::I64(ff_al_id)],
            )?;

            self.frequent_flyer.decrement_iattr(c_id, ff_al_id);
        }

        Ok(())
    }

    fn find_flights(
        &self,
        depart_aid: i64,
        arrive_aid: i64,
        start_timestamp: i64,
        end_timestamp: i64,
        distance: i64,
    ) -> Result<Vec<AirportInfo>, seats::Error> {
        let mut transaction = self.new_transaction();

        let distance = distance as f64;

        let mut arrive_aids = vec![arrive_aid];

        if distance > 0.0 {
            self.dibs.acquire(
                &mut transaction,
                seats::GET_NEARBY_AIRPORTS_TEMPLATE_ID,
                vec![Value::I64(depart_aid), Value::F64(distance)],
            )?;

            arrive_aids.extend(
                self.airport_distance
                    .get_nearby_airports(depart_aid, distance),
            );
        }

        self.dibs.acquire(
            &mut transaction,
            seats::GET_FLIGHTS_TEMPLATE_ID,
            vec![
                Value::I64(depart_aid),
                Value::I64(start_timestamp),
                Value::I64(end_timestamp),
            ],
        )?;

        let flights = self
            .flight
            .get_flights(
                depart_aid,
                start_timestamp,
                end_timestamp,
                &arrive_aids.into_iter().take(3).collect(),
            )
            .into_iter()
            .map(|flight_info| {
                self.dibs.acquire(
                    &mut transaction,
                    seats::GET_AIRLINE_NAME_TEMPLATE_ID,
                    vec![Value::I64(flight_info.al_id)],
                )?;

                let al_name = self.airline.get_airline_name(flight_info.al_id);

                self.dibs.acquire(
                    &mut transaction,
                    seats::GET_AIRPORT_INFO_TEMPLATE_ID,
                    vec![Value::I64(depart_aid)],
                )?;

                self.dibs.acquire(
                    &mut transaction,
                    seats::GET_AIRPORT_INFO_TEMPLATE_ID,
                    vec![Value::I64(arrive_aid)],
                )?;

                let (depart_ap_code, depart_ap_name, depart_ap_city, depart_ap_co_id) =
                    self.airport.get_airport_info(depart_aid);

                let (arrive_ap_code, arrive_ap_name, arrive_ap_city, arrive_ap_co_id) =
                    self.airport.get_airport_info(arrive_aid);

                Ok(AirportInfo {
                    f_id: flight_info.id,
                    seats_left: flight_info.seats_left,
                    al_name: al_name.to_string(),
                    depart_time: flight_info.depart_time,
                    depart_ap_code: depart_ap_code.to_string(),
                    depart_ap_name: depart_ap_name.to_string(),
                    depart_ap_city: depart_ap_city.to_string(),
                    depart_ap_co_id,
                    arrive_time: flight_info.arrive_time,
                    arrive_ap_code: arrive_ap_code.to_string(),
                    arrive_ap_name: arrive_ap_name.to_string(),
                    arrive_ap_city: arrive_ap_city.to_string(),
                    arrive_ap_co_id,
                })
            })
            .collect::<Result<Vec<AirportInfo>, seats::Error>>()?;

        Ok(flights)
    }

    fn find_open_seats(&self, f_id: i64) -> Result<Vec<(i64, i64, f64)>, seats::Error> {
        let mut transaction = self.new_transaction();

        let mut seat_map = vec![false; 150];

        self.dibs.acquire(
            &mut transaction,
            seats::GET_PRICE_TEMPLATE_ID,
            vec![Value::I64(f_id)],
        )?;

        let price = match self.flight.get_price(f_id) {
            Some(price) => price,
            None => return Ok(vec![]),
        };

        self.dibs.acquire(
            &mut transaction,
            seats::GET_RESERVED_SEATS_ON_FLIGHT_TEMPLATE_ID,
            vec![Value::I64(f_id)],
        )?;

        for seat in self.reservation.get_reserved_seats_on_flight(f_id) {
            let seat_index = usize::try_from(seat).unwrap();
            assert!(!&seat_map[seat_index]);
            seat_map[seat_index] = true;
        }

        Ok(seat_map
            .iter()
            .enumerate()
            .filter(|(_, &s)| !s)
            .map(|(seat, _)| {
                let adjusted_price = if seat < 10 { 2.0 * price } else { price };
                (f_id, seat as i64, adjusted_price)
            })
            .collect())
    }

    fn new_reservation(
        &self,
        r_id: i64,
        c_id: i64,
        f_id: i64,
        seat: i64,
        price: f64,
        iattrs: &[i64],
    ) -> Result<(), seats::Error> {
        let mut transaction = self.new_transaction();

        self.dibs.acquire(
            &mut transaction,
            seats::GET_AIRLINE_AND_SEATS_LEFT_TEMPLATE_ID,
            vec![Value::I64(f_id)],
        )?;

        let (al_id, seats_left) = self
            .flight
            .get_airline_and_seats_left(f_id)
            .ok_or(seats::Error::UserAbort(format!("invalid flight {}", f_id)))?;

        if seats_left <= 0 {
            return Err(seats::Error::UserAbort(format!(
                "no seats available for flight {}",
                f_id
            )));
        }

        self.dibs.acquire(
            &mut transaction,
            seats::SEAT_IS_RESERVED_TEMPLATE_ID,
            vec![Value::I64(f_id), Value::I64(seat)],
        )?;

        if self.reservation.seat_is_reserved(f_id, seat) {
            return Err(seats::Error::UserAbort(format!(
                "seat {} on flight {} is reserved",
                seat, f_id
            )));
        }

        self.dibs.acquire(
            &mut transaction,
            seats::CUSTOMER_HAS_RESERVATION_ON_FLIGHT_TEMPLATE_ID,
            vec![Value::I64(c_id), Value::I64(f_id)],
        )?;

        if self
            .reservation
            .customer_has_reservation_on_flight(c_id, f_id)
        {
            return Err(seats::Error::UserAbort(format!(
                "customer {} already has reservation on flight {}",
                c_id, f_id
            )));
        }

        self.dibs.acquire(
            &mut transaction,
            seats::INSERT_REMOVE_TEMPLATE_ID,
            vec![Value::I64(r_id), Value::I64(c_id), Value::I64(f_id)],
        )?;

        self.reservation
            .insert(r_id, c_id, f_id, seat, price, iattrs)
            .map_err(|_| seats::Error::InvalidOperation)?;

        self.dibs.acquire(
            &mut transaction,
            seats::INCREMENT_DECREMENT_SEATS_LEFT_TEMPLATE_ID,
            vec![Value::I64(f_id)],
        )?;

        self.flight.decrement_seats_left(f_id);

        self.dibs.acquire(
            &mut transaction,
            seats::UPDATE_CUSTOMER_NEW_RESERVATION_TEMPLATE_ID,
            vec![Value::I64(c_id)],
        )?;

        self.customer
            .update_customer_new_reservation(c_id, iattrs[0], iattrs[1], iattrs[2], iattrs[3]);

        self.dibs.acquire(
            &mut transaction,
            seats::SET_IATTRS_NEW_RESERVATION_TEMPLATE_ID,
            vec![Value::I64(c_id), Value::I64(al_id)],
        )?;

        self.frequent_flyer
            .set_iattrs_new_reservation(c_id, al_id, iattrs[4], iattrs[5], iattrs[6], iattrs[7]);

        Ok(())
    }

    fn update_customer(
        &self,
        variant: UpdateCustomerVariant,
        update_ff: bool,
        iattr0: i64,
        iattr1: i64,
    ) -> Result<(), seats::Error> {
        let mut transaction = self.new_transaction();

        let c_id =
            match variant {
                UpdateCustomerVariant::CustomerId(c_id) => c_id,
                UpdateCustomerVariant::CustomerIdString(c_id_str) => {
                    self.dibs.acquire(
                        &mut transaction,
                        seats::GET_CUSTOMER_ID_FROM_STR_TEMPLATE_ID,
                        vec![Value::String(c_id_str.to_string())],
                    )?;

                    self.customer.get_customer_id_from_str(&c_id_str).ok_or(
                        seats::Error::UserAbort(format!("customer {} not found", c_id_str)),
                    )?
                }
            };

        if update_ff {
            self.dibs.acquire(
                &mut transaction,
                seats::SET_IATTRS_UPDATE_CUSTOMER_TEMPLATE_ID,
                vec![Value::I64(c_id)],
            )?;

            self.frequent_flyer
                .set_iattrs_update_customer(c_id, iattr0, iattr1);
        }

        self.dibs.acquire(
            &mut transaction,
            seats::UPDATE_CUSTOMER_IATTRS_TEMPLATE_ID,
            vec![Value::I64(c_id)],
        )?;

        self.customer.update_customer_iattrs(c_id, iattr0, iattr1);

        Ok(())
    }

    fn update_reservation(
        &self,
        r_id: i64,
        c_id: i64,
        f_id: i64,
        seat: i64,
        iattr_index: usize,
        iattr: i64,
    ) -> Result<(), seats::Error> {
        assert!(iattr_index < 4);

        let mut transaction = self.new_transaction();

        self.dibs.acquire(
            &mut transaction,
            seats::SEAT_IS_RESERVED_TEMPLATE_ID,
            vec![Value::I64(f_id), Value::I64(seat)],
        )?;

        if self.reservation.seat_is_reserved(f_id, seat) {
            return Err(seats::Error::UserAbort(format!(
                "seat {} on flight {} is reserved",
                seat, f_id
            )));
        }

        self.dibs.acquire(
            &mut transaction,
            seats::CUSTOMER_HAS_RESERVATION_ON_FLIGHT_TEMPLATE_ID,
            vec![Value::I64(c_id), Value::I64(f_id)],
        )?;

        if !self
            .reservation
            .customer_has_reservation_on_flight(c_id, f_id)
        {
            return Err(seats::Error::UserAbort(format!(
                "customer {} has no reservation on flight {}",
                c_id, f_id
            )));
        }

        self.dibs.acquire(
            &mut transaction,
            seats::UPDATE_RESERVATION_TEMPLATE_ID,
            vec![Value::I64(r_id), Value::I64(c_id), Value::I64(f_id)],
        )?;

        self.reservation
            .update_reservation(r_id, c_id, f_id, seat, iattr_index, iattr)
            .map_err(|_| seats::Error::InvalidOperation)
    }
}

#[test]
fn test() {
    let country = Country::new("/Users/kpg/data/country.csv");
    println!("{}", country.name.value(0));

    let airport = Airport::new("/Users/kpg/data/airport.csv");
    println!("{}", airport.name.value(0));

    let airport_distance = AirportDistance::new("/Users/kpg/data/airport_distance.csv");
    println!("{}", airport_distance.id0.value(0));

    let airline = Airline::new("/Users/kpg/data/airline.csv");
    println!("{}", airline.name.value(0));

    let customer = Customer::new("/Users/kpg/data/customer.csv");
    println!("{}", customer.id_str.value(0));

    let frequent_flyer = FrequentFlyer::new("/Users/kpg/data/frequent_flyer.csv");
    println!("{}", frequent_flyer.c_id_str.value(0));

    let flight = Flight::new("/Users/kpg/data/flight.csv");
    println!("{}", flight.id.value(0));

    let _reservation = Reservation::new("/Users/kpg/data/reservation.csv");
}
