use crate::benchmarks::seats;
use crate::benchmarks::seats::SEATSConnection;
use crate::systems::arrow::{BooleanArrayMut, Float64ArrayMut, Int64ArrayMut};
use arrow::array::{
    Array, ArrayBuilder, Float64Array, Float64Builder, Int64Array, Int64Builder, PrimitiveArrayOps,
    StringArray, StringBuilder,
};
use arrow::csv;
use arrow::datatypes::{DataType, Field, Schema};
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryFrom;
use std::fmt::Debug;
use std::fs::File;
use std::ops::Bound;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

const BLOCK_CAPACITY: usize = 1024;
const NUM_PARTITIONS: usize = 1024;

enum Error {
    DuplicateKey,
    NonexistentKey,
}

#[allow(dead_code)]
struct Country {
    id: Int64Array,
    name: StringArray,
    code_2: StringArray,
    code_3: StringArray,
}

impl Country {
    fn new<P>(path: P) -> Country
    where
        P: AsRef<Path>,
    {
        let schema = Schema::new(vec![
            Field::new("CO_ID", DataType::Int64, false),
            Field::new("CO_NAME", DataType::Utf8, false),
            Field::new("CO_CODE_2", DataType::Utf8, false),
            Field::new("CO_CODE_3", DataType::Utf8, false),
        ]);

        let file = File::open(path).unwrap();
        let csv = csv::Reader::new(file, Arc::new(schema), true, None, 1024, None);

        let mut id_builder = Int64Builder::new(0);
        let mut name_builder = StringBuilder::new(0);
        let mut code_2_builder = StringBuilder::new(0);
        let mut code_3_builder = StringBuilder::new(0);

        for result in csv {
            let batch = result.unwrap();

            id_builder.append_data(&[batch.column(0).data()]).unwrap();
            name_builder.append_data(&[batch.column(1).data()]).unwrap();
            code_2_builder
                .append_data(&[batch.column(2).data()])
                .unwrap();
            code_3_builder
                .append_data(&[batch.column(3).data()])
                .unwrap();
        }

        Country {
            id: id_builder.finish(),
            name: name_builder.finish(),
            code_2: code_2_builder.finish(),
            code_3: code_3_builder.finish(),
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
}

impl Airport {
    fn new<P>(path: P) -> Airport
    where
        P: AsRef<Path>,
    {
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
        let csv = csv::Reader::new(file, Arc::new(schema), true, None, 1024, None);

        let mut id_builder = Int64Builder::new(0);
        let mut code_builder = StringBuilder::new(0);
        let mut name_builder = StringBuilder::new(0);
        let mut city_builder = StringBuilder::new(0);
        let mut postal_code_builder = StringBuilder::new(0);
        let mut co_id_builder = Int64Builder::new(0);
        let mut longitude_builder = Float64Builder::new(0);
        let mut latitude_builder = Float64Builder::new(0);
        let mut gmt_offset_builder = Float64Builder::new(0);
        let mut wac_builder = Int64Builder::new(0);
        let mut iattr_builders = (0..16).map(|_| Int64Builder::new(0)).collect::<Vec<_>>();

        for result in csv {
            let batch = result.unwrap();

            id_builder.append_data(&[batch.column(0).data()]).unwrap();
            code_builder.append_data(&[batch.column(1).data()]).unwrap();
            name_builder.append_data(&[batch.column(2).data()]).unwrap();
            city_builder.append_data(&[batch.column(3).data()]).unwrap();
            postal_code_builder
                .append_data(&[batch.column(4).data()])
                .unwrap();
            co_id_builder
                .append_data(&[batch.column(5).data()])
                .unwrap();
            longitude_builder
                .append_data(&[batch.column(6).data()])
                .unwrap();
            latitude_builder
                .append_data(&[batch.column(7).data()])
                .unwrap();
            gmt_offset_builder
                .append_data(&[batch.column(8).data()])
                .unwrap();
            wac_builder.append_data(&[batch.column(9).data()]).unwrap();

            for (i, iattr_builder) in iattr_builders.iter_mut().enumerate() {
                iattr_builder
                    .append_data(&[batch.column(i + 10).data()])
                    .unwrap();
            }
        }

        Airport {
            id: id_builder.finish(),
            code: code_builder.finish(),
            name: name_builder.finish(),
            city: city_builder.finish(),
            postal_code: postal_code_builder.finish(),
            co_id: co_id_builder.finish(),
            longitude: longitude_builder.finish(),
            latitude: latitude_builder.finish(),
            gmt_offset: gmt_offset_builder.finish(),
            wac: wac_builder.finish(),
            iattrs: iattr_builders.into_iter().map(|mut b| b.finish()).collect(),
        }
    }
}

#[allow(dead_code)]
struct AirportDistance {
    id0: Int64Array,
    id1: Int64Array,
    distance: Float64Array,
}

impl AirportDistance {
    fn new<P>(path: P) -> AirportDistance
    where
        P: AsRef<Path>,
    {
        let schema = Schema::new(vec![
            Field::new("D_AP_ID0", DataType::Int64, false),
            Field::new("D_AP_ID1", DataType::Int64, false),
            Field::new("D_DISTANCE", DataType::Float64, false),
        ]);

        let file = File::open(path).unwrap();
        let csv = csv::Reader::new(file, Arc::new(schema), true, None, 1024, None);

        let mut id0_builder = Int64Builder::new(0);
        let mut id1_builder = Int64Builder::new(0);
        let mut distance_builder = Float64Builder::new(0);

        for result in csv {
            let batch = result.unwrap();

            id0_builder.append_data(&[batch.column(0).data()]).unwrap();
            id1_builder.append_data(&[batch.column(1).data()]).unwrap();
            distance_builder
                .append_data(&[batch.column(2).data()])
                .unwrap();
        }

        AirportDistance {
            id0: id0_builder.finish(),
            id1: id1_builder.finish(),
            distance: distance_builder.finish(),
        }
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
}

impl Airline {
    pub fn new<P>(path: P) -> Airline
    where
        P: AsRef<Path>,
    {
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
        let csv = csv::Reader::new(file, Arc::new(schema), true, None, 1024, None);

        let mut id_builder = Int64Builder::new(0);
        let mut iata_code_builder = StringBuilder::new(0);
        let mut icao_code_builder = StringBuilder::new(0);
        let mut call_sign_builder = StringBuilder::new(0);
        let mut name_builder = StringBuilder::new(0);
        let mut co_id_builder = Int64Builder::new(0);
        let mut iattr_builders = (0..16).map(|_| Int64Builder::new(0)).collect::<Vec<_>>();

        for result in csv {
            let batch = result.unwrap();

            id_builder.append_data(&[batch.column(0).data()]).unwrap();
            iata_code_builder
                .append_data(&[batch.column(1).data()])
                .unwrap();
            icao_code_builder
                .append_data(&[batch.column(2).data()])
                .unwrap();
            call_sign_builder
                .append_data(&[batch.column(3).data()])
                .unwrap();
            name_builder.append_data(&[batch.column(4).data()]).unwrap();
            co_id_builder
                .append_data(&[batch.column(5).data()])
                .unwrap();

            for (i, iattr_builder) in iattr_builders.iter_mut().enumerate() {
                iattr_builder
                    .append_data(&[batch.column(i + 6).data()])
                    .unwrap();
            }
        }

        Airline {
            id: id_builder.finish(),
            iata_code: iata_code_builder.finish(),
            icao_code: icao_code_builder.finish(),
            call_sign: call_sign_builder.finish(),
            name: name_builder.finish(),
            co_id: co_id_builder.finish(),
            iattrs: iattr_builders.into_iter().map(|mut b| b.finish()).collect(),
        }
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
        let csv = csv::Reader::new(file, Arc::new(schema), true, None, 1024, None);

        let mut id_builder = Int64Builder::new(0);
        let mut id_str_builder = StringBuilder::new(0);
        let mut base_ap_id_builder = Int64Builder::new(0);
        let mut balance_builder = Float64Builder::new(0);
        let mut sattr_builders = (0..20).map(|_| StringBuilder::new(0)).collect::<Vec<_>>();
        let mut iattr_builders = (0..20).map(|_| Int64Builder::new(0)).collect::<Vec<_>>();

        for result in csv {
            let batch = result.unwrap();

            id_builder.append_data(&[batch.column(0).data()]).unwrap();
            id_str_builder
                .append_data(&[batch.column(1).data()])
                .unwrap();
            base_ap_id_builder
                .append_data(&[batch.column(2).data()])
                .unwrap();
            balance_builder
                .append_data(&[batch.column(3).data()])
                .unwrap();

            for (i, sattr_builder) in sattr_builders.iter_mut().enumerate() {
                sattr_builder
                    .append_data(&[batch.column(i + 4).data()])
                    .unwrap();
            }

            for (i, iattr_builder) in iattr_builders.iter_mut().enumerate() {
                iattr_builder
                    .append_data(&[batch.column(i + 24).data()])
                    .unwrap();
            }
        }

        let id = id_builder.finish();
        let id_str = id_str_builder.finish();

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
            base_ap_id: base_ap_id_builder.finish(),
            balance: Float64ArrayMut(balance_builder.finish()),
            sattrs: sattr_builders.into_iter().map(|mut b| b.finish()).collect(),
            iattrs: iattr_builders
                .into_iter()
                .map(|mut b| Int64ArrayMut(b.finish()))
                .collect(),
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

    fn get_customer_base_airport(&self, c_id: i64) -> Option<i64> {
        self.pk_index.get(&c_id).map(|&row_index| {
            assert_eq!(self.id.value(row_index), c_id);
            self.base_ap_id.value(row_index)
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
        let row_index = self.pk_index[&c_id];
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

    fn update_customer_iattrs(&self, c_id: i64, iattr00: i64, iattr01: i64) {
        let row_index = self.pk_index[&c_id];
        assert_eq!(self.id.value(row_index), c_id);

        unsafe {
            self.iattrs[0].set(row_index, iattr00);
            self.iattrs[1].set(row_index, iattr01);
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
        let csv = csv::Reader::new(file, Arc::new(schema), true, None, 1024, None);

        let mut c_id_builder = Int64Builder::new(0);
        let mut al_id_builder = Int64Builder::new(0);
        let mut c_id_str_builder = StringBuilder::new(0);
        let mut sattr_builders = (0..4).map(|_| StringBuilder::new(0)).collect::<Vec<_>>();
        let mut iattr_builders = (0..16).map(|_| Int64Builder::new(0)).collect::<Vec<_>>();

        for result in csv {
            let batch = result.unwrap();

            c_id_builder.append_data(&[batch.column(0).data()]).unwrap();
            al_id_builder
                .append_data(&[batch.column(1).data()])
                .unwrap();
            c_id_str_builder
                .append_data(&[batch.column(2).data()])
                .unwrap();

            for (i, sattr_builder) in sattr_builders.iter_mut().enumerate() {
                sattr_builder
                    .append_data(&[batch.column(i + 3).data()])
                    .unwrap();
            }

            for (i, iattr_builder) in iattr_builders.iter_mut().enumerate() {
                iattr_builder
                    .append_data(&[batch.column(i + 7).data()])
                    .unwrap();
            }
        }

        let c_id = c_id_builder.finish();
        let al_id = al_id_builder.finish();

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
            c_id_str: c_id_str_builder.finish(),
            sattrs: sattr_builders.into_iter().map(|mut b| b.finish()).collect(),
            iattrs: iattr_builders
                .into_iter()
                .map(|mut b| Int64ArrayMut(b.finish()))
                .collect(),
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
        // May need to change this. In the original, we don't care if we update frequent flyer.
        let row_index = self.pk_index[&c_id][&al_id];
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

    fn set_iattrs_update_customer(&self, c_id: i64, al_id: i64, iattr00: i64, iattr01: i64) {
        let row_index = self.pk_index[&c_id][&al_id];
        assert_eq!(self.c_id.value(row_index), c_id);
        assert_eq!(self.al_id.value(row_index), al_id);

        unsafe {
            self.iattrs[0].set(row_index, iattr00);
            self.iattrs[1].set(row_index, iattr01);
        }
    }
}

struct FlightInfo {
    id: i64,
    al_id: i64,
    seats_left: i64,
    depart_ap_id: i64,
    depart_time: i64,
    arrive_ap_id: i64,
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
        let csv = csv::Reader::new(file, Arc::new(schema), true, None, 1024, None);

        let mut id_builder = Int64Builder::new(0);
        let mut al_id_builder = Int64Builder::new(0);
        let mut depart_ap_id_builder = Int64Builder::new(0);
        let mut depart_time_builder = Int64Builder::new(0);
        let mut arrive_ap_id_builder = Int64Builder::new(0);
        let mut arrive_time_builder = Int64Builder::new(0);
        let mut status_builder = Int64Builder::new(0);
        let mut base_price_builder = Float64Builder::new(0);
        let mut seats_total_builder = Int64Builder::new(0);
        let mut seats_left_builder = Int64Builder::new(0);
        let mut iattr_builders = (0..30).map(|_| Int64Builder::new(0)).collect::<Vec<_>>();

        for result in csv {
            let batch = result.unwrap();

            id_builder.append_data(&[batch.column(0).data()]).unwrap();
            al_id_builder
                .append_data(&[batch.column(1).data()])
                .unwrap();
            depart_ap_id_builder
                .append_data(&[batch.column(2).data()])
                .unwrap();
            depart_time_builder
                .append_data(&[batch.column(3).data()])
                .unwrap();
            arrive_ap_id_builder
                .append_data(&[batch.column(4).data()])
                .unwrap();
            arrive_time_builder
                .append_data(&[batch.column(5).data()])
                .unwrap();
            status_builder
                .append_data(&[batch.column(6).data()])
                .unwrap();
            base_price_builder
                .append_data(&[batch.column(7).data()])
                .unwrap();
            seats_total_builder
                .append_data(&[batch.column(8).data()])
                .unwrap();
            seats_left_builder
                .append_data(&[batch.column(9).data()])
                .unwrap();

            for (i, iattr_builder) in iattr_builders.iter_mut().enumerate() {
                iattr_builder
                    .append_data(&[batch.column(i + 10).data()])
                    .unwrap();
            }
        }

        let id = id_builder.finish();
        let depart_time = depart_time_builder.finish();

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
            al_id: al_id_builder.finish(),
            depart_ap_id: depart_ap_id_builder.finish(),
            depart_time,
            arrive_ap_id: arrive_ap_id_builder.finish(),
            arrive_time: arrive_time_builder.finish(),
            status: status_builder.finish(),
            base_price: base_price_builder.finish(),
            seats_total: seats_total_builder.finish(),
            seats_left: Int64ArrayMut(seats_left_builder.finish()),
            iattrs: iattr_builders.into_iter().map(|mut b| b.finish()).collect(),
            pk_index,
            depart_time_index,
        }
    }

    fn get_seats_left(&self, id: i64) -> i64 {
        let row_index = self.pk_index[&id];
        assert_eq!(self.id.value(row_index), id);

        self.seats_left.0.value(row_index)
    }

    fn get_price(&self, id: i64) -> f64 {
        let row_index = self.pk_index[&id];
        assert_eq!(self.id.value(row_index), id);

        let base_price = self.base_price.value(row_index);
        let seats_total = self.seats_total.value(row_index);
        let seats_left = self.seats_left.0.value(row_index);
        base_price + (base_price * (1.0 - (seats_left as f64 / seats_total as f64)))
    }

    fn get_flights(
        &self,
        depart_ap_id: i64,
        depart_time_a: i64,
        depart_time_b: i64,
        al_id: i64,
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
                    && self.al_id.value(row_index) == al_id
                    && arrive_ap_id.contains(&self.arrive_ap_id.value(row_index))
                {
                    Some(FlightInfo {
                        id: self.id.value(row_index),
                        al_id,
                        seats_left: self.seats_left.0.value(row_index),
                        depart_ap_id,
                        depart_time,
                        arrive_ap_id: self.arrive_ap_id.value(row_index),
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
        unimplemented!()
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
            .ok_or(Error::NonexistentKey)?;

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
            Entry::Occupied(_) => Err(Error::DuplicateKey),
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
            None => Err(Error::NonexistentKey),
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
        unimplemented!()
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

    fn get_partition(&self, f_id: i64) -> MutexGuard<ReservationPartition> {
        self.partitions[usize::try_from(f_id).unwrap() % self.partitions.len()]
            .lock()
            .unwrap()
    }
}

// #[allow(dead_code)]
// struct Reservation {
//     id: Int64Array,
//     c_id: Int64Array,
//     f_id: Int64Array,
//     seat: Int64Array,
//     price: Float64Array,
//     iattrs: Vec<Int64Array>,
// }
//
// impl Reservation {
//     fn new<P>(path: P) -> Reservation
//     where
//         P: AsRef<Path>,
//     {
//         let mut fields = vec![
//             Field::new("R_ID", DataType::Int64, false),
//             Field::new("R_C_ID", DataType::Int64, false),
//             Field::new("R_F_ID", DataType::Int64, false),
//             Field::new("R_SEAT", DataType::Int64, false),
//             Field::new("R_PRICE", DataType::Float64, false),
//         ];
//
//         for i in 0..9 {
//             fields.push(Field::new(
//                 &format!("R_IATTR0{}", i % 10),
//                 DataType::Int64,
//                 true,
//             ));
//         }
//
//         let schema = Schema::new(fields);
//
//         let file = File::open(path).unwrap();
//         let csv = csv::Reader::new(file, Arc::new(schema), true, None, 1024, None);
//
//         let mut id_builder = Int64Builder::new(0);
//         let mut c_id_builder = Int64Builder::new(0);
//         let mut f_id_builder = Int64Builder::new(0);
//         let mut seat_builder = Int64Builder::new(0);
//         let mut price_builder = Float64Builder::new(0);
//         let mut iattr_builders = (0..9).map(|_| Int64Builder::new(0)).collect::<Vec<_>>();
//
//         for result in csv {
//             let batch = result.unwrap();
//
//             id_builder.append_data(&[batch.column(0).data()]).unwrap();
//             c_id_builder.append_data(&[batch.column(1).data()]).unwrap();
//             f_id_builder.append_data(&[batch.column(2).data()]).unwrap();
//             seat_builder.append_data(&[batch.column(3).data()]).unwrap();
//             price_builder
//                 .append_data(&[batch.column(4).data()])
//                 .unwrap();
//
//             for (i, iattr_builder) in iattr_builders.iter_mut().enumerate() {
//                 iattr_builder
//                     .append_data(&[batch.column(i + 5).data()])
//                     .unwrap();
//             }
//         }
//
//         let id = id_builder.finish();
//         let c_id = c_id_builder.finish();
//         let f_id = f_id_builder.finish();
//         let seat = seat_builder.finish();
//
//         let pk_index = id
//             .iter()
//             .zip(c_id.iter().zip(f_id.iter()))
//             .enumerate()
//             .map(|(i, (id_v, (c_id_v, f_id_v)))| {
//                 ((id_v.unwrap(), c_id_v.unwrap(), f_id_v.unwrap()), i)
//             })
//             .collect();
//
//         let mut f_id_seat_index = HashMap::new();
//         for (i, (f_id_v, seat_v)) in f_id.iter().zip(seat.iter()).enumerate() {
//             f_id_seat_index
//                 .entry(f_id_v.unwrap())
//                 .or_insert(HashMap::new())
//                 .insert(seat_v.unwrap(), i);
//         }
//
//         Reservation {
//             id,
//             c_id,
//             f_id,
//             seat,
//             price: price_builder.finish(),
//             iattrs: iattr_builders.into_iter().map(|mut b| b.finish()).collect(),
//             pk_index,
//             f_id_seat_index,
//         }
//     }
// }

pub struct Database {
    country: Country,
    airport: Airport,
    airport_distance: AirportDistance,
    airline: Airline,
    customer: Customer,
    frequent_flyer: FrequentFlyer,
    flight: Flight,
    reservation: Reservation,
}

impl SEATSConnection for Database {
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
        S2: AsRef<str> + Debug,
    {
        unimplemented!()
    }

    fn find_flights(
        &self,
        depart_airport_id: i64,
        arrive_airport_id: i64,
        start_timestamp: i64,
        end_timestamp: i64,
        distance: i64,
    ) -> Result<(), seats::Error> {
        unimplemented!()
    }

    fn find_open_seats(&self, f_id: i64) -> Result<Vec<(i64, i64, f64)>, seats::Error> {
        let mut seat_map = vec![false; 150];
        let price = self.flight.get_price(f_id);

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
        reservation_id: i64,
        customer_id: i64,
        flight_id: i64,
        seat_num: i64,
        price: f64,
        attrs: &[i64],
    ) -> Result<(), seats::Error> {
        unimplemented!()
    }

    fn update_customer<S>(
        &self,
        customer_id: i64,
        customer_id_string: Option<S>,
        update_frequent_flyer: i64,
        attr0: i64,
        attr1: i64,
    ) -> Result<(), seats::Error>
    where
        S: AsRef<str> + Debug,
    {
        unimplemented!()
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

        if self.reservation.seat_is_reserved(f_id, seat) {
            return Err(seats::Error::SeatReserved { f_id, seat });
        }

        if !self
            .reservation
            .customer_has_reservation_on_flight(c_id, f_id)
        {
            return Err(seats::Error::NonexistentReservation { c_id, f_id });
        }

        self.reservation
            .update_reservation(r_id, c_id, f_id, seat, iattr_index, iattr)
            .map_err(|e| seats::Error::InvalidOperation)
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

    let reservation = Reservation::new("/Users/kpg/data/reservation.csv");
}
