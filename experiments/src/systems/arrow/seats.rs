use arrow::array::{
    ArrayBuilder, Float64Array, Float64Builder, Int64Array, Int64Builder, PrimitiveArrayOps,
    StringArray, StringBuilder,
};
use arrow::csv;
use arrow::datatypes::{DataType, Field, Schema};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

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
    balance: Float64Array,
    sattrs: Vec<StringArray>,
    iattrs: Vec<Int64Array>,
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

        Customer {
            id: id_builder.finish(),
            id_str: id_str_builder.finish(),
            base_ap_id: base_ap_id_builder.finish(),
            balance: balance_builder.finish(),
            sattrs: sattr_builders.into_iter().map(|mut b| b.finish()).collect(),
            iattrs: iattr_builders.into_iter().map(|mut b| b.finish()).collect(),
        }
    }
}

#[allow(dead_code)]
struct FrequentFlyer {
    c_id: Int64Array,
    al_id: Int64Array,
    c_id_str: StringArray,
    sattrs: Vec<StringArray>,
    iattrs: Vec<Int64Array>,
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

        FrequentFlyer {
            c_id: c_id_builder.finish(),
            al_id: al_id_builder.finish(),
            c_id_str: c_id_str_builder.finish(),
            sattrs: sattr_builders.into_iter().map(|mut b| b.finish()).collect(),
            iattrs: iattr_builders.into_iter().map(|mut b| b.finish()).collect(),
        }
    }
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
    seats_left: Int64Array,
    iattrs: Vec<Int64Array>,
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

        Flight {
            id: id_builder.finish(),
            al_id: al_id_builder.finish(),
            depart_ap_id: depart_ap_id_builder.finish(),
            depart_time: depart_time_builder.finish(),
            arrive_ap_id: arrive_ap_id_builder.finish(),
            arrive_time: arrive_time_builder.finish(),
            status: status_builder.finish(),
            base_price: base_price_builder.finish(),
            seats_total: seats_total_builder.finish(),
            seats_left: seats_left_builder.finish(),
            iattrs: iattr_builders.into_iter().map(|mut b| b.finish()).collect(),
        }
    }
}

#[allow(dead_code)]
struct Reservation {
    id: Int64Array,
    c_id: Int64Array,
    f_id: Int64Array,
    seat: Int64Array,
    price: Float64Array,
    iattrs: Vec<Int64Array>,
}

impl Reservation {
    fn new<P>(path: P) -> Reservation
    where
        P: AsRef<Path>,
    {
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
        let csv = csv::Reader::new(file, Arc::new(schema), true, None, 1024, None);

        let mut id_builder = Int64Builder::new(0);
        let mut c_id_builder = Int64Builder::new(0);
        let mut f_id_builder = Int64Builder::new(0);
        let mut seat_builder = Int64Builder::new(0);
        let mut price_builder = Float64Builder::new(0);
        let mut iattr_builders = (0..9).map(|_| Int64Builder::new(0)).collect::<Vec<_>>();

        for result in csv {
            let batch = result.unwrap();

            id_builder.append_data(&[batch.column(0).data()]).unwrap();
            c_id_builder.append_data(&[batch.column(1).data()]).unwrap();
            f_id_builder.append_data(&[batch.column(2).data()]).unwrap();
            seat_builder.append_data(&[batch.column(3).data()]).unwrap();
            price_builder
                .append_data(&[batch.column(4).data()])
                .unwrap();

            for (i, iattr_builder) in iattr_builders.iter_mut().enumerate() {
                iattr_builder
                    .append_data(&[batch.column(i + 5).data()])
                    .unwrap();
            }
        }

        Reservation {
            id: id_builder.finish(),
            c_id: c_id_builder.finish(),
            f_id: f_id_builder.finish(),
            seat: seat_builder.finish(),
            price: price_builder.finish(),
            iattrs: iattr_builders.into_iter().map(|mut b| b.finish()).collect(),
        }
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
    println!("{}", reservation.id.value(0));
}
