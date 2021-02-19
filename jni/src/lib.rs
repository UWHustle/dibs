use dibs::OptimizationLevel;
use dibs_experiments::benchmarks::seats::{
    DeleteReservationVariant, SEATSConnection, UpdateCustomerVariant,
};
use dibs_experiments::systems;
use jni::objects::{JClass, JString};
use jni::sys::{jdouble, jlong, jlongArray};
use jni::JNIEnv;
use std::convert::TryInto;

#[macro_use]
extern crate lazy_static;

lazy_static! {
    static ref CONN: systems::arrow::seats::Database =
        systems::arrow::seats::Database::new(OptimizationLevel::Filtered);
}

#[no_mangle]
pub extern "system" fn Java_com_oltpbenchmark_DBWorkload_handshake() {
    println!("Loading native database...");
    CONN.hello();
    println!("Done.")
}

#[no_mangle]
pub extern "system" fn Java_com_oltpbenchmark_benchmarks_seats_procedures_DeleteReservation_deleteReservation(
    env: JNIEnv,
    _class: JClass,
    f_id: jlong,
    c_id: jlong,
    c_id_str: JString,
    ff_c_id_str: JString,
) -> jlong {
    let c_id_str = env.get_string(c_id_str).map(|s| String::from(s)).ok();

    let ff_c_id_str = env.get_string(ff_c_id_str).map(|s| String::from(s)).ok();

    let variant = match (c_id, c_id_str, ff_c_id_str) {
        (c_id, None, None) => {
            assert_ne!(c_id, -1, "invalid arguments to delete_reservation");
            DeleteReservationVariant::CustomerId(c_id)
        }
        (-1, Some(c_id_str), None) => DeleteReservationVariant::CustomerIdString(c_id_str),
        (-1, None, Some(ff_c_id_str)) => DeleteReservationVariant::FrequentFlyer(ff_c_id_str),
        _ => panic!("invalid arguments to delete_reservation"),
    };

    match CONN.delete_reservation(variant, f_id) {
        Ok(_) => 0,
        Err(e) => {
            println!("{:?}", e);
            1
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_oltpbenchmark_benchmarks_seats_procedures_FindFlights_findFlights(
    env: JNIEnv,
    _class: JClass,
    depart_aid: jlong,
    arrive_aid: jlong,
    start_timestamp: jlong,
    end_timestamp: jlong,
    distance: jlong,
) -> jlongArray {
    match CONN.find_flights(
        depart_aid,
        arrive_aid,
        start_timestamp,
        end_timestamp,
        distance,
    ) {
        Ok(airport_infos) => {
            let flights = airport_infos
                .into_iter()
                .map(|airport_info| airport_info.f_id)
                .collect::<Vec<_>>();

            let ret = env
                .new_long_array((flights.len() + 1).try_into().unwrap())
                .unwrap();

            env.set_long_array_region(ret, 0, &[0]).unwrap();
            env.set_long_array_region(ret, 1, &flights).unwrap();

            ret
        }
        Err(e) => {
            println!("{:?}", e);

            let ret = env.new_long_array(1).unwrap();

            env.set_long_array_region(ret, 0, &[1]).unwrap();

            ret
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_oltpbenchmark_benchmarks_seats_procedures_FindOpenSeats_findOpenSeats(
    env: JNIEnv,
    _class: JClass,
    f_id: jlong,
) -> jlongArray {
    match CONN.find_open_seats(f_id) {
        Ok(seat_infos) => {
            let seats = seat_infos
                .into_iter()
                .map(|(_, seat, _)| seat)
                .collect::<Vec<_>>();

            let ret = env
                .new_long_array((seats.len() + 1).try_into().unwrap())
                .unwrap();

            env.set_long_array_region(ret, 0, &[0]).unwrap();
            env.set_long_array_region(ret, 1, &seats).unwrap();

            ret
        }
        Err(e) => {
            println!("{:?}", e);

            let ret = env.new_long_array(1).unwrap();

            env.set_long_array_region(ret, 0, &[1]).unwrap();

            ret
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_oltpbenchmark_benchmarks_seats_procedures_NewReservation_newReservation(
    env: JNIEnv,
    _class: JClass,
    r_id: jlong,
    c_id: jlong,
    f_id: jlong,
    seat: jlong,
    price: jdouble,
    attrs: jlongArray,
) -> jlong {
    let mut iattrs = vec![0; env.get_array_length(attrs).unwrap() as usize];
    env.get_long_array_region(attrs, 0, &mut iattrs).unwrap();

    match CONN.new_reservation(r_id, c_id, f_id, seat, price, &iattrs) {
        Ok(_) => 0,
        Err(e) => {
            println!("{:?}", e);
            1
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_oltpbenchmark_benchmarks_seats_procedures_UpdateCustomer_updateCustomer(
    env: JNIEnv,
    _class: JClass,
    c_id: jlong,
    c_id_str: JString,
    update_ff: jlong,
    iattr0: jlong,
    iattr1: jlong,
) -> jlong {
    let c_id_str = env.get_string(c_id_str).map(|s| String::from(s)).ok();

    let variant = match (c_id, c_id_str) {
        (-1, Some(c_id_str)) => UpdateCustomerVariant::CustomerIdString(c_id_str),
        (c_id, None) => {
            assert_ne!(c_id, -1, "invalid arguments to update_customer");
            UpdateCustomerVariant::CustomerId(c_id)
        }
        _ => panic!("invalid arguments to update_customer"),
    };

    match CONN.update_customer(variant, update_ff != -1, iattr0, iattr1) {
        Ok(_) => 0,
        Err(e) => {
            println!("{:?}", e);
            1
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_oltpbenchmark_benchmarks_seats_procedures_UpdateReservation_updateReservation(
    _env: JNIEnv,
    _class: JClass,
    r_id: jlong,
    f_id: jlong,
    c_id: jlong,
    seat: jlong,
    iattr_idx: jlong,
    iattr_val: jlong,
) -> jlong {
    match CONN.update_reservation(r_id, c_id, f_id, seat, iattr_idx as usize, iattr_val) {
        Ok(_) => 0,
        Err(e) => {
            println!("{:?}", e);
            1
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
