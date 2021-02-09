// use dibs_experiments::benchmarks::seats::SEATSConnection;
// use dibs_experiments::systems::odbc;
// use jni::objects::{JClass, JString};
// use jni::sys::{jdouble, jlong, jlongArray};
// use jni::JNIEnv;
// use odbc_sys::Env;
//
// #[macro_use]
// extern crate lazy_static;
//
// struct EnvPtr {
//     env: *mut Env,
// }
//
// unsafe impl Sync for EnvPtr {}
//
// lazy_static! {
//     static ref ENV: EnvPtr = EnvPtr {
//         env: unsafe { odbc::alloc_env().unwrap() }
//     };
// }
//
// thread_local! {
//     static CONN: Box<dyn SEATSConnection>;
// }
//
// #[no_mangle]
// pub extern "system" fn Java_com_oltpbenchmark_benchmarks_seats_procedures_DeleteReservation_deleteReservation(
//     env: JNIEnv,
//     _class: JClass,
//     flight_id: jlong,
//     customer_id: jlong,
//     customer_id_string: JString,
//     frequent_flyer_customer_id_string: JString,
//     frequent_flyer_airline_id: jlong,
// ) {
//     let customer_id_string = env
//         .get_string(customer_id_string)
//         .map(|s| String::from(s))
//         .ok();
//     let frequent_flyer_customer_id_string = env
//         .get_string(frequent_flyer_customer_id_string)
//         .map(|s| String::from(s))
//         .ok();
//
//     CONN.with(|c| {
//         c.delete_reservation(
//             flight_id,
//             customer_id,
//             customer_id_string,
//             frequent_flyer_customer_id_string,
//             frequent_flyer_airline_id,
//         )
//         .unwrap()
//     });
// }
//
// #[no_mangle]
// pub extern "system" fn Java_com_oltpbenchmark_benchmarks_seats_procedures_FindFlights_findFlights(
//     _env: JNIEnv,
//     _class: JClass,
//     depart_airport_id: jlong,
//     arrive_airport_id: jlong,
//     start_timestamp: jlong,
//     end_timestamp: jlong,
//     distance: jlong,
// ) {
//     CONN.with(|c| {
//         c.find_flights(
//             depart_airport_id,
//             arrive_airport_id,
//             start_timestamp,
//             end_timestamp,
//             distance,
//         )
//         .unwrap()
//     });
// }
//
// #[no_mangle]
// pub extern "system" fn Java_com_oltpbenchmark_benchmarks_seats_procedures_FindOpenSeats_findOpenSeats(
//     _env: JNIEnv,
//     _class: JClass,
//     flight_id: jlong,
// ) {
//     CONN.with(|c| c.find_open_seats(flight_id)).unwrap();
// }
//
// #[no_mangle]
// pub extern "system" fn Java_com_oltpbenchmark_benchmarks_seats_procedures_NewReservation_newReservation(
//     env: JNIEnv,
//     _class: JClass,
//     reservation_id: jlong,
//     customer_id: jlong,
//     flight_id: jlong,
//     seat_num: jlong,
//     price: jdouble,
//     attrs: jlongArray,
// ) {
//     let mut attrs_vec = vec![0; env.get_array_length(attrs).unwrap() as usize];
//     env.get_long_array_region(attrs, 0, &mut attrs_vec).unwrap();
//
//     CONN.with(|c| {
//         c.new_reservation(
//             reservation_id,
//             customer_id,
//             flight_id,
//             seat_num,
//             price,
//             &attrs_vec,
//         )
//         .unwrap();
//     });
// }
//
// #[no_mangle]
// pub extern "system" fn Java_com_oltpbenchmark_benchmarks_seats_procedures_UpdateCustomer_updateCustomer(
//     env: JNIEnv,
//     _class: JClass,
//     customer_id: jlong,
//     customer_id_string: JString,
//     update_frequent_flyer: jlong,
//     attr0: jlong,
//     attr1: jlong,
// ) {
//     let customer_id_string = env
//         .get_string(customer_id_string)
//         .map(|s| String::from(s))
//         .ok();
//
//     CONN.with(|c| {
//         c.update_customer(
//             customer_id,
//             customer_id_string,
//             update_frequent_flyer,
//             attr0,
//             attr1,
//         )
//         .unwrap()
//     });
// }
//
// #[no_mangle]
// pub extern "system" fn Java_com_oltpbenchmark_benchmarks_seats_procedures_UpdateReservation_updateReservation(
//     _env: JNIEnv,
//     _class: JClass,
//     reservation_id: jlong,
//     flight_id: jlong,
//     customer_id: jlong,
//     seat_num: jlong,
//     attr_idx: jlong,
//     attr_val: jlong,
// ) {
//     CONN.with(|c| {
//         c.update_reservation(
//             reservation_id,
//             flight_id,
//             customer_id,
//             seat_num,
//             attr_idx,
//             attr_val,
//         )
//         .unwrap()
//     });
// }
//
// #[cfg(test)]
// mod tests {
//     #[test]
//     fn it_works() {
//         assert_eq!(2 + 2, 4);
//     }
// }
