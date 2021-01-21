use crate::benchmarks::tatp;
use crate::benchmarks::tatp::TATPConnection;
use crate::Connection;
use itertools::Itertools;
use rand::seq::SliceRandom;
use rand::Rng;

pub fn load_tatp(num_rows: u32) {
    assert!(num_rows > 0);
    assert_eq!(num_rows % 100, 0);

    let mut rng = rand::thread_rng();

    let env = odbc::create_environment_v3().unwrap();
    let conn = env.connect("DIBS", "SA", "DIBS123!").unwrap();

    let exec_direct = |sql| {
        odbc::Statement::with_parent(&conn)
            .unwrap()
            .exec_direct(sql)
            .unwrap();
    };

    exec_direct("DROP TABLE IF EXISTS call_forwarding;");
    exec_direct("DROP TABLE IF EXISTS special_facility;");
    exec_direct("DROP TABLE IF EXISTS access_info;");
    exec_direct("DROP TABLE IF EXISTS subscriber;");

    exec_direct(
        "CREATE TABLE subscriber (s_id INTEGER NOT NULL,
                    bit_1 TINYINT, bit_2 TINYINT, bit_3 TINYINT, bit_4 TINYINT,
                    bit_5 TINYINT, bit_6 TINYINT, bit_7 TINYINT, bit_8 TINYINT,
                    bit_9 TINYINT, bit_10 TINYINT,
                    hex_1 TINYINT, hex_2 TINYINT, hex_3 TINYINT, hex_4 TINYINT,
                    hex_5 TINYINT, hex_6 TINYINT, hex_7 TINYINT, hex_8 TINYINT,
                    hex_9 TINYINT, hex_10 TINYINT,
                    byte2_1 TINYINT, byte2_2 TINYINT, byte2_3 TINYINT, byte2_4 TINYINT,
                    byte2_5 TINYINT, byte2_6 TINYINT, byte2_7 TINYINT, byte2_8 TINYINT,
                    byte2_9 TINYINT, byte2_10 TINYINT,
                    msc_location BIGINT, vlr_location BIGINT,
                    PRIMARY KEY (s_id));",
    );

    exec_direct(
        "CREATE TABLE access_info (s_id INTEGER NOT NULL,
                ai_type TINYINT NOT NULL,
                data1 TINYINT, data2 TINYINT, data3 VARCHAR(3), data4 VARCHAR(5),
                PRIMARY KEY (s_id, ai_type),
                FOREIGN KEY (s_id) REFERENCES subscriber (s_id));",
    );

    exec_direct(
        "CREATE TABLE special_facility (s_id INTEGER NOT NULL,
                sf_type TINYINT NOT NULL,
                is_active TINYINT, error_cntrl TINYINT,
                data_a TINYINT, data_b VARCHAR(5),
                PRIMARY KEY (s_id, sf_type),
                FOREIGN KEY (s_id) REFERENCES subscriber (s_id));",
    );

    exec_direct(
        "CREATE TABLE call_forwarding (s_id INTEGER NOT NULL,
                sf_type TINYINT NOT NULL,
                start_time TINYINT, end_time TINYINT, numberx VARCHAR(15),
                PRIMARY KEY (s_id, sf_type, start_time) WITH (IGNORE_DUP_KEY = ON));",
    );

    let mut s_ids = (1..=num_rows).collect::<Vec<_>>();
    s_ids.shuffle(&mut rng);

    let ai_types = s_ids
        .iter()
        .flat_map(|&s_id| {
            let num_ai_types = rng.gen_range(1, 5);
            [1, 2, 3, 4]
                .choose_multiple(&mut rng, num_ai_types)
                .map(move |&ai_type| (s_id, ai_type))
        })
        .collect::<Vec<_>>();

    let sf_types = s_ids
        .iter()
        .flat_map(|&s_id| {
            let num_sf_types = rng.gen_range(1, 5);
            [1, 2, 3, 4]
                .choose_multiple(&mut rng, num_sf_types)
                .map(move |&sf_type| (s_id, sf_type))
        })
        .collect::<Vec<_>>();

    let cf_start_times = sf_types
        .iter()
        .flat_map(|&(s_id, sf_type)| {
            let num_start_times = rng.gen_range(0, 4);
            [0, 8, 16]
                .choose_multiple(&mut rng, num_start_times)
                .map(move |&start_time| (s_id, sf_type, start_time))
        })
        .collect::<Vec<_>>();

    for s_chunk in s_ids.chunks(1000) {
        odbc::Statement::with_parent(&conn)
            .unwrap()
            .exec_direct(&format!(
                "INSERT INTO subscriber VALUES {};",
                s_chunk
                    .iter()
                    .map(|&s_id| format!(
                        "({},{},{},{},{},{})",
                        s_id,
                        (0..10).map(|_| rng.gen_range(0, 2)).join(","),
                        (0..10).map(|_| rng.gen_range(0, 16)).join(","),
                        (0..10).map(|_| rng.gen_range(0, 256)).join(","),
                        rng.gen::<u32>(),
                        rng.gen::<u32>(),
                    ))
                    .join(",")
            ))
            .unwrap();
    }

    for ai_chunk in ai_types.chunks(1000) {
        odbc::Statement::with_parent(&conn)
            .unwrap()
            .exec_direct(&format!(
                "INSERT INTO access_info VALUES {};",
                ai_chunk
                    .iter()
                    .map(|&(s_id, ai_type)| format!(
                        "({},{},{},{},'{}','{}')",
                        s_id,
                        ai_type,
                        rng.gen::<u8>(),
                        rng.gen::<u8>(),
                        tatp::uppercase_alphabetic_string(3, &mut rng),
                        tatp::uppercase_alphabetic_string(5, &mut rng)
                    ))
                    .join(",")
            ))
            .unwrap();
    }

    for sf_chunk in sf_types.chunks(1000) {
        odbc::Statement::with_parent(&conn)
            .unwrap()
            .exec_direct(&format!(
                "INSERT INTO special_facility VALUES {};",
                sf_chunk
                    .iter()
                    .map(|&(s_id, sf_type)| format!(
                        "({},{},{},{},{},'{}')",
                        s_id,
                        sf_type,
                        if rng.gen_bool(0.85) { 1 } else { 0 },
                        rng.gen::<u8>(),
                        rng.gen::<u8>(),
                        tatp::uppercase_alphabetic_string(5, &mut rng),
                    ))
                    .join(",")
            ))
            .unwrap();
    }

    for cf_chunk in cf_start_times.chunks(1000) {
        odbc::Statement::with_parent(&conn)
            .unwrap()
            .exec_direct(&format!(
                "INSERT INTO call_forwarding VALUES {}",
                cf_chunk
                    .iter()
                    .map(|&(s_id, sf_type, start_time)| format!(
                        "({},{},{},{},'{}')",
                        s_id,
                        sf_type,
                        start_time,
                        start_time + rng.gen_range(1, 9),
                        tatp::uppercase_alphabetic_string(15, &mut rng)
                    ))
                    .join(",")
            ))
            .unwrap();
    }
}

type Statement<'a> =
    odbc::Statement<'a, 'a, odbc::Prepared, odbc::NoResult, odbc::odbc_safe::AutocommitOff>;

pub struct SQLServerTATPConnection<'a> {
    get_subscriber_data_stmt: Option<Statement<'a>>,
    get_new_destination_stmt: Option<Statement<'a>>,
    get_access_data_stmt: Option<Statement<'a>>,
    update_subscriber_bit_stmt: Option<Statement<'a>>,
    update_special_facility_data_stmt: Option<Statement<'a>>,
    update_subscriber_location_stmt: Option<Statement<'a>>,
    get_special_facility_types_stmt: Option<Statement<'a>>,
    insert_call_forwarding_stmt: Option<Statement<'a>>,
    delete_call_forwarding_stmt: Option<Statement<'a>>,
    conn: Box<odbc::Connection<'a, odbc::odbc_safe::AutocommitOff>>,
    _env: Box<odbc::Environment<odbc::Version3>>,
}

impl<'a> SQLServerTATPConnection<'a> {
    pub fn new() -> SQLServerTATPConnection<'a> {
        let env = Box::into_raw(Box::new(odbc::create_environment_v3().unwrap()));

        let conn = Box::into_raw(Box::new(
            unsafe { &*env }
                .connect("DIBS", "SA", "DIBS123!")
                .unwrap()
                .disable_autocommit()
                .unwrap(),
        ));

        let prepare = |sql| {
            Some(
                odbc::Statement::with_parent(unsafe { &*conn })
                    .unwrap()
                    .prepare(sql)
                    .unwrap(),
            )
        };

        let get_subscriber_data_stmt = prepare(
            "SELECT *
            FROM subscriber
            WHERE s_id = ?;",
        );

        let get_new_destination_stmt = prepare(
            "SELECT cf.numberx
            FROM special_facility AS sf, call_forwarding AS cf
            WHERE
                (sf.s_id = ?
                    AND sf.sf_type = ?
                    AND sf.is_active = 1)
                AND (cf.s_id = sf.s_id
                    AND cf.sf_type = sf.sf_type)
                AND (cf.start_time <= ?
                    AND ? < cf.end_time);",
        );

        let get_access_data_stmt = prepare(
            "SELECT data1, data2, data3, data4
            FROM access_info
            WHERE s_id = ? AND ai_type = ?;",
        );

        let update_subscriber_bit_stmt = prepare(
            "UPDATE subscriber
            SET bit_1 = ?
            WHERE s_id = ?;",
        );

        let update_special_facility_data_stmt = prepare(
            "UPDATE special_facility
            SET data_a = ?
            WHERE s_id = ? AND sf_type = ?;",
        );

        let update_subscriber_location_stmt = prepare(
            "UPDATE subscriber
            SET vlr_location = ?
            WHERE s_id = ?;",
        );

        let get_special_facility_types_stmt = prepare(
            "SELECT sf_type
            FROM special_facility
            WHERE s_id = ?;",
        );

        let insert_call_forwarding_stmt = prepare(
            "INSERT INTO call_forwarding
            VALUES (?, ?, ?, ?, ?);",
        );

        let delete_call_forwarding_stmt = prepare(
            "DELETE FROM call_forwarding
            WHERE s_id = ? AND sf_type = ? AND start_time = ?;",
        );

        SQLServerTATPConnection {
            get_subscriber_data_stmt,
            get_new_destination_stmt,
            get_access_data_stmt,
            update_subscriber_bit_stmt,
            update_special_facility_data_stmt,
            update_subscriber_location_stmt,
            get_special_facility_types_stmt,
            insert_call_forwarding_stmt,
            delete_call_forwarding_stmt,
            conn: unsafe { Box::from_raw(conn) },
            _env: unsafe { Box::from_raw(env) },
        }
    }
}

impl Connection for SQLServerTATPConnection<'_> {
    fn begin(&mut self) {}

    fn commit(&mut self) {
        self.conn.commit().unwrap();
    }

    fn rollback(&mut self) {
        self.conn.rollback().unwrap();
    }

    fn savepoint(&mut self) {
        unimplemented!()
    }
}

fn execute_update<'a, 'b>(
    stmt: odbc::Statement<'a, 'b, odbc::Prepared, odbc::NoResult, odbc::odbc_safe::AutocommitOff>,
) -> Statement<'a> {
    match stmt.execute().unwrap() {
        odbc::ResultSetState::Data(stmt) => {
            stmt.close_cursor().unwrap().reset_parameters().unwrap()
        }
        odbc::ResultSetState::NoData(stmt) => stmt.reset_parameters().unwrap(),
    }
}

impl<'a> TATPConnection for SQLServerTATPConnection<'a> {
    fn get_subscriber_data(&mut self, s_id: u32) -> ([bool; 10], [u8; 10], [u8; 10], u32, u32) {
        let mut stmt = self.get_subscriber_data_stmt.take().unwrap();
        stmt = stmt.bind_parameter(1, &s_id).unwrap();

        match stmt.execute().unwrap() {
            odbc::ResultSetState::Data(mut stmt) => {
                let mut cursor = stmt.fetch().unwrap().unwrap();

                let mut bit = [false; 10];
                for i in 0..10 {
                    bit[i] = cursor.get_data((i + 2) as u16).unwrap().unwrap();
                }

                let mut hex = [0; 10];
                for i in 0..10 {
                    hex[i] = cursor.get_data((i + 12) as u16).unwrap().unwrap();
                }

                let mut byte2 = [0; 10];
                for i in 0..10 {
                    byte2[i] = cursor.get_data((i + 22) as u16).unwrap().unwrap();
                }

                let msc_location = cursor.get_data(32).unwrap().unwrap();

                let vlr_location = cursor.get_data(33).unwrap().unwrap();

                self.get_subscriber_data_stmt =
                    Some(stmt.close_cursor().unwrap().reset_parameters().unwrap());

                (bit, hex, byte2, msc_location, vlr_location)
            }
            odbc::ResultSetState::NoData(_) => panic!(),
        }
    }

    fn get_new_destination(
        &mut self,
        s_id: u32,
        sf_type: u8,
        start_time: u8,
        end_time: u8,
    ) -> Vec<String> {
        let mut stmt = self.get_new_destination_stmt.take().unwrap();
        stmt = stmt.bind_parameter(1, &s_id).unwrap();
        stmt = stmt.bind_parameter(2, &sf_type).unwrap();
        stmt = stmt.bind_parameter(3, &start_time).unwrap();
        stmt = stmt.bind_parameter(4, &end_time).unwrap();

        match stmt.execute().unwrap() {
            odbc::ResultSetState::Data(mut stmt) => {
                let mut numberx = vec![];

                while let Some(mut cursor) = stmt.fetch().unwrap() {
                    numberx.push(cursor.get_data(1).unwrap().unwrap());
                }

                self.get_new_destination_stmt =
                    Some(stmt.close_cursor().unwrap().reset_parameters().unwrap());

                numberx
            }
            odbc::ResultSetState::NoData(_) => panic!(),
        }
    }

    fn get_access_data(&mut self, s_id: u32, ai_type: u8) -> Option<(u8, u8, String, String)> {
        let mut stmt = self.get_access_data_stmt.take().unwrap();
        stmt = stmt.bind_parameter(1, &s_id).unwrap();
        stmt = stmt.bind_parameter(2, &ai_type).unwrap();

        match stmt.execute().unwrap() {
            odbc::ResultSetState::Data(mut stmt) => {
                let data = stmt.fetch().unwrap().map(|mut cursor| {
                    let data1 = cursor.get_data(1).unwrap().unwrap();
                    let data2 = cursor.get_data(2).unwrap().unwrap();
                    let data3 = cursor.get_data(3).unwrap().unwrap();
                    let data4 = cursor.get_data(4).unwrap().unwrap();

                    (data1, data2, data3, data4)
                });

                self.get_access_data_stmt =
                    Some(stmt.close_cursor().unwrap().reset_parameters().unwrap());

                data
            }
            odbc::ResultSetState::NoData(_) => panic!(),
        }
    }

    fn update_subscriber_bit(&mut self, bit_1: bool, s_id: u32) {
        let mut stmt = self.update_subscriber_bit_stmt.take().unwrap();
        stmt = stmt.bind_parameter(1, &bit_1).unwrap();
        stmt = stmt.bind_parameter(2, &s_id).unwrap();

        self.update_subscriber_bit_stmt = Some(execute_update(stmt));
    }

    fn update_special_facility_data(&mut self, data_a: u8, s_id: u32, sf_type: u8) {
        let mut stmt = self.update_special_facility_data_stmt.take().unwrap();
        stmt = stmt.bind_parameter(1, &data_a).unwrap();
        stmt = stmt.bind_parameter(2, &s_id).unwrap();
        stmt = stmt.bind_parameter(3, &sf_type).unwrap();

        self.update_special_facility_data_stmt = Some(execute_update(stmt));
    }

    fn update_subscriber_location(&mut self, vlr_location: u32, s_id: u32) {
        let mut stmt = self.update_subscriber_location_stmt.take().unwrap();
        stmt = stmt.bind_parameter(1, &vlr_location).unwrap();
        stmt = stmt.bind_parameter(2, &s_id).unwrap();

        self.update_subscriber_location_stmt = Some(execute_update(stmt));
    }

    fn get_special_facility_types(&mut self, s_id: u32) -> Vec<u8> {
        let mut stmt = self.get_special_facility_types_stmt.take().unwrap();
        stmt = stmt.bind_parameter(1, &s_id).unwrap();

        match stmt.execute().unwrap() {
            odbc::ResultSetState::Data(mut stmt) => {
                let mut sf_types = vec![];

                while let Some(mut cursor) = stmt.fetch().unwrap() {
                    sf_types.push(cursor.get_data(1).unwrap().unwrap());
                }

                self.get_special_facility_types_stmt =
                    Some(stmt.close_cursor().unwrap().reset_parameters().unwrap());

                sf_types
            }
            odbc::ResultSetState::NoData(_) => panic!(),
        }
    }

    fn insert_call_forwarding(
        &mut self,
        s_id: u32,
        sf_type: u8,
        start_time: u8,
        end_time: u8,
        numberx: &str,
    ) {
        let mut stmt = self.insert_call_forwarding_stmt.take().unwrap();
        stmt = stmt.bind_parameter(1, &s_id).unwrap();
        stmt = stmt.bind_parameter(2, &sf_type).unwrap();
        stmt = stmt.bind_parameter(3, &start_time).unwrap();
        stmt = stmt.bind_parameter(4, &end_time).unwrap();
        stmt = stmt.bind_parameter(5, &numberx).unwrap();

        self.insert_call_forwarding_stmt = Some(execute_update(stmt));
    }

    fn delete_call_forwarding(&mut self, s_id: u32, sf_type: u8, start_time: u8) {
        let mut stmt = self.delete_call_forwarding_stmt.take().unwrap();
        stmt = stmt.bind_parameter(1, &s_id).unwrap();
        stmt = stmt.bind_parameter(2, &sf_type).unwrap();
        stmt = stmt.bind_parameter(3, &start_time).unwrap();

        self.delete_call_forwarding_stmt = Some(execute_update(stmt));
    }
}

unsafe impl Send for SQLServerTATPConnection<'_> {}
