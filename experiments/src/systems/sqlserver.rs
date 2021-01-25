use crate::benchmarks::tatp;
use crate::benchmarks::tatp_sp::TATPSPConnection;
use crate::systems::odbc;
use crate::systems::odbc::{
    alloc_dbc, alloc_stmt, bind_parameter, connect, disconnect, exec_direct, execute, fetch,
    free_dbc, free_stmt, get_data, prepare, reset_stmt, Char, Error,
};
use crate::Connection;
use itertools::Itertools;
use odbc_sys::{Dbc, Env, Stmt};
use rand::seq::SliceRandom;
use rand::Rng;
use std::ffi::CString;

pub unsafe fn load_tatp(env: *mut Env, num_rows: u32) -> odbc::Result<()> {
    assert!(num_rows > 0);
    assert_eq!(num_rows % 100, 0);

    let mut rng = rand::thread_rng();

    let dbc = alloc_dbc(env)?;
    connect(dbc, "DIBS", "SA", "DIBS123!")?;

    exec_direct(dbc, "USE dibs;")?;

    exec_direct(dbc, "DROP PROCEDURE IF EXISTS tatp.get_subscriber_data")?;
    exec_direct(dbc, "DROP PROCEDURE IF EXISTS tatp.get_new_destination")?;
    exec_direct(dbc, "DROP PROCEDURE IF EXISTS tatp.get_access_data")?;
    exec_direct(dbc, "DROP PROCEDURE IF EXISTS tatp.update_subscriber_data")?;
    exec_direct(dbc, "DROP PROCEDURE IF EXISTS tatp.update_location")?;
    exec_direct(dbc, "DROP PROCEDURE IF EXISTS tatp.insert_call_forwarding")?;
    exec_direct(dbc, "DROP PROCEDURE IF EXISTS tatp.delete_call_forwarding")?;

    exec_direct(dbc, "DROP TABLE IF EXISTS tatp.call_forwarding;")?;
    exec_direct(dbc, "DROP TABLE IF EXISTS tatp.special_facility;")?;
    exec_direct(dbc, "DROP TABLE IF EXISTS tatp.access_info;")?;
    exec_direct(dbc, "DROP TABLE IF EXISTS tatp.subscriber;")?;

    exec_direct(dbc, "DROP SCHEMA IF EXISTS tatp")?;

    exec_direct(dbc, "CREATE SCHEMA tatp")?;

    exec_direct(
        dbc,
        "CREATE TABLE tatp.subscriber (s_id INTEGER NOT NULL,
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
                    PRIMARY KEY NONCLUSTERED (s_id))
                WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_ONLY);",
    )?;

    exec_direct(
        dbc,
        "CREATE TABLE tatp.access_info (s_id INTEGER NOT NULL,
                ai_type TINYINT NOT NULL,
                data1 TINYINT, data2 TINYINT, data3 VARCHAR(3), data4 VARCHAR(5),
                PRIMARY KEY NONCLUSTERED (s_id, ai_type),
                FOREIGN KEY (s_id) REFERENCES tatp.subscriber (s_id))
             WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_ONLY);",
    )?;

    exec_direct(
        dbc,
        "CREATE TABLE tatp.special_facility (s_id INTEGER NOT NULL,
                sf_type TINYINT NOT NULL,
                is_active TINYINT, error_cntrl TINYINT,
                data_a TINYINT, data_b VARCHAR(5),
                PRIMARY KEY NONCLUSTERED (s_id, sf_type),
                FOREIGN KEY (s_id) REFERENCES tatp.subscriber (s_id))
             WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_ONLY);",
    )?;

    exec_direct(
        dbc,
        "CREATE TABLE tatp.call_forwarding (s_id INTEGER NOT NULL,
                sf_type TINYINT NOT NULL,
                start_time TINYINT, end_time TINYINT, numberx VARCHAR(15),
                PRIMARY KEY NONCLUSTERED (s_id, sf_type, start_time))
             WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_ONLY);",
    )?;

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
        exec_direct(
            dbc,
            &format!(
                "INSERT INTO tatp.subscriber VALUES {};",
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
            ),
        )?;
    }

    for ai_chunk in ai_types.chunks(1000) {
        exec_direct(
            dbc,
            &format!(
                "INSERT INTO tatp.access_info VALUES {};",
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
            ),
        )?;
    }

    for sf_chunk in sf_types.chunks(1000) {
        exec_direct(
            dbc,
            &format!(
                "INSERT INTO tatp.special_facility VALUES {};",
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
            ),
        )?;
    }

    for cf_chunk in cf_start_times.chunks(1000) {
        exec_direct(
            dbc,
            &format!(
                "INSERT INTO tatp.call_forwarding VALUES {}",
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
            ),
        )?;
    }

    let create_procedure = |name, params, sql| {
        exec_direct(
            dbc,
            &format!(
                "CREATE PROCEDURE {} ({})
                WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
                AS BEGIN ATOMIC WITH
                (TRANSACTION ISOLATION LEVEL = SERIALIZABLE, LANGUAGE = 'english')
                    {}
                END",
                name, params, sql,
            ),
        )
    };

    create_procedure(
        "tatp.get_subscriber_data",
        "@s_id INT",
        "SELECT
            bit_1, bit_2, bit_3, bit_4, bit_5,
            bit_6, bit_7, bit_8, bit_9, bit_10,
            hex_1, hex_2, hex_3, hex_4, hex_5,
            hex_6, hex_7, hex_8, hex_9, hex_10,
            byte2_1, byte2_2, byte2_3, byte2_4, byte2_5,
            byte2_6, byte2_7, byte2_8, byte2_9, byte2_10,
            msc_location, vlr_location
        FROM tatp.subscriber
        WHERE s_id = @s_id;",
    )?;

    create_procedure(
        "tatp.get_new_destination",
        "@s_id INT, @sf_type TINYINT, @start_time TINYINT, @end_time TINYINT",
        "SELECT cf.numberx
        FROM tatp.special_facility AS sf, tatp.call_forwarding AS cf
        WHERE
            (sf.s_id = @s_id
                AND sf.sf_type = @sf_type
                AND sf.is_active = 1)
            AND (cf.s_id = sf.s_id
                AND cf.sf_type = sf.sf_type)
            AND (cf.start_time <= @start_time
                AND @end_time < cf.end_time);",
    )?;

    create_procedure(
        "tatp.get_access_data",
        "@s_id INT, @ai_type TINYINT",
        "SELECT data1, data2, data3, data4
        FROM tatp.access_info
        WHERE s_id = @s_id
            AND ai_type = @ai_type",
    )?;

    create_procedure(
        "tatp.update_subscriber_data",
        "@bit_1 TINYINT, @s_id INT, @data_a TINYINT, @sf_type TINYINT",
        "UPDATE tatp.subscriber
        SET bit_1 = @bit_1
        WHERE s_id = @s_id;

        UPDATE tatp.special_facility
        SET data_a = @data_a
        WHERE s_id = @s_id
            AND sf_type = @sf_type;",
    )?;

    create_procedure(
        "tatp.update_location",
        "@vlr_location BIGINT, @s_id INT",
        "UPDATE tatp.subscriber
        SET vlr_location = @vlr_location
        WHERE s_id = @s_id;",
    )?;

    create_procedure(
        "tatp.insert_call_forwarding",
        "@s_id INT, @sf_type TINYINT, @start_time TINYINT, @end_time TINYINT, @numberx VARCHAR(15)",
        "SELECT sf_type
        FROM tatp.special_facility
        WHERE s_id = @s_id;

        INSERT INTO tatp.call_forwarding
        VALUES (@s_id, @sf_type, @start_time, @end_time, @numberx);",
    )?;

    create_procedure(
        "tatp.delete_call_forwarding",
        "@s_id INT, @sf_type TINYINT, @start_time TINYINT",
        "DELETE FROM tatp.call_forwarding
        WHERE s_id = @s_id
            AND sf_type = @sf_type
            AND start_time = @start_time;",
    )?;

    disconnect(dbc)?;
    free_dbc(dbc)?;

    Ok(())
}

pub struct SQLServerTATPConnection {
    dbc: *mut Dbc,
    get_subscriber_data_stmt: *mut Stmt,
    get_new_destination_stmt: *mut Stmt,
    get_access_data_stmt: *mut Stmt,
    update_subscriber_data_stmt: *mut Stmt,
    update_location_stmt: *mut Stmt,
    insert_call_forwarding_stmt: *mut Stmt,
    delete_call_forwarding_stmt: *mut Stmt,
}

impl SQLServerTATPConnection {
    pub fn new(env: *mut Env) -> odbc::Result<SQLServerTATPConnection> {
        unsafe {
            let dbc = alloc_dbc(env)?;
            connect(dbc, "DIBS", "SA", "DIBS123!")?;

            exec_direct(dbc, "USE dibs;")?;

            let get_subscriber_data_stmt = alloc_stmt(dbc)?;
            prepare(
                get_subscriber_data_stmt,
                "{ CALL tatp.get_subscriber_data (?) }",
            )?;

            let get_new_destination_stmt = alloc_stmt(dbc)?;
            prepare(
                get_new_destination_stmt,
                "{ CALL tatp.get_new_destination (?, ?, ?, ?) }",
            )?;

            let get_access_data_stmt = alloc_stmt(dbc)?;
            prepare(get_access_data_stmt, "{ CALL tatp.get_access_data (?, ?) }")?;

            let update_subscriber_data_stmt = alloc_stmt(dbc)?;
            prepare(
                update_subscriber_data_stmt,
                "{ CALL tatp.update_subscriber_data (?, ?, ?, ?) }",
            )?;

            let update_location_stmt = alloc_stmt(dbc)?;
            prepare(update_location_stmt, "{ CALL tatp.update_location (?, ?) }")?;

            let insert_call_forwarding_stmt = alloc_stmt(dbc)?;
            prepare(
                insert_call_forwarding_stmt,
                "{ CALL tatp.insert_call_forwarding (?, ?, ?, ?, ?) }",
            )?;

            let delete_call_forwarding_stmt = alloc_stmt(dbc)?;
            prepare(
                delete_call_forwarding_stmt,
                "{ CALL tatp.delete_call_forwarding (?, ?, ?) }",
            )?;

            Ok(SQLServerTATPConnection {
                dbc,
                get_subscriber_data_stmt,
                get_new_destination_stmt,
                get_access_data_stmt,
                update_subscriber_data_stmt,
                update_location_stmt,
                insert_call_forwarding_stmt,
                delete_call_forwarding_stmt,
            })
        }
    }
}

impl Connection for SQLServerTATPConnection {
    fn begin(&mut self) {}

    fn commit(&mut self) {}

    fn rollback(&mut self) {}

    fn savepoint(&mut self) {}
}

impl TATPSPConnection for SQLServerTATPConnection {
    fn get_subscriber_data(&mut self, mut s_id: u32) -> ([bool; 10], [u8; 10], [u8; 10], u32, u32) {
        unsafe {
            bind_parameter(self.get_subscriber_data_stmt, 1, &mut s_id).unwrap();

            execute_with_retry(self.get_subscriber_data_stmt);

            fetch(self.get_subscriber_data_stmt).unwrap();

            let mut bit = [false; 10];
            for i in 0..10 {
                let mut bit_u8 = 0u8;
                get_data(self.get_subscriber_data_stmt, i as u16 + 1, &mut bit_u8).unwrap();
                bit[i] = bit_u8 == 1;
            }

            let mut hex = [0; 10];
            for i in 0..10 {
                get_data(self.get_subscriber_data_stmt, i as u16 + 11, &mut hex[i]).unwrap();
            }

            let mut byte2 = [0; 10];
            for i in 0..10 {
                get_data(self.get_subscriber_data_stmt, i as u16 + 21, &mut byte2[i]).unwrap();
            }

            let mut msc_location = 0u32;
            get_data(self.get_subscriber_data_stmt, 31, &mut msc_location).unwrap();

            let mut vlr_location = 0u32;
            get_data(self.get_subscriber_data_stmt, 32, &mut vlr_location).unwrap();

            reset_stmt(self.get_subscriber_data_stmt).unwrap();

            (bit, hex, byte2, msc_location, vlr_location)
        }
    }

    fn get_new_destination(
        &mut self,
        mut s_id: u32,
        mut sf_type: u8,
        mut start_time: u8,
        mut end_time: u8,
    ) -> Vec<String> {
        unsafe {
            bind_parameter(self.get_new_destination_stmt, 1, &mut s_id).unwrap();
            bind_parameter(self.get_new_destination_stmt, 2, &mut sf_type).unwrap();
            bind_parameter(self.get_new_destination_stmt, 3, &mut start_time).unwrap();
            bind_parameter(self.get_new_destination_stmt, 4, &mut end_time).unwrap();

            execute_with_retry(self.get_new_destination_stmt);

            let mut numberx = vec![];

            while fetch(self.get_new_destination_stmt).unwrap() {
                // TODO: Implement this.
                let mut numberx_bytes = vec![0u8; 16];
                let mut numberx_char = Char::new(&mut numberx_bytes);
                get_data(self.get_new_destination_stmt, 1, &mut numberx_char).unwrap();
                numberx.push(
                    CString::from_vec_with_nul_unchecked(numberx_bytes)
                        .into_string()
                        .unwrap(),
                );
            }

            reset_stmt(self.get_new_destination_stmt).unwrap();

            numberx
        }
    }

    fn get_access_data(
        &mut self,
        mut s_id: u32,
        mut ai_type: u8,
    ) -> Option<(u8, u8, String, String)> {
        unsafe {
            bind_parameter(self.get_access_data_stmt, 1, &mut s_id).unwrap();
            bind_parameter(self.get_access_data_stmt, 2, &mut ai_type).unwrap();

            execute_with_retry(self.get_access_data_stmt);

            let result = if fetch(self.get_access_data_stmt).unwrap() {
                let mut data1 = 0u8;
                get_data(self.get_access_data_stmt, 1, &mut data1).unwrap();

                let mut data2 = 0u8;
                get_data(self.get_access_data_stmt, 2, &mut data2).unwrap();

                let mut data3_bytes = vec![0u8; 4];
                let mut data3_char = Char::new(&mut data3_bytes);
                get_data(self.get_access_data_stmt, 3, &mut data3_char).unwrap();

                let mut data4_bytes = vec![0u8; 6];
                let mut data4_char = Char::new(&mut data4_bytes);
                get_data(self.get_access_data_stmt, 4, &mut data4_char).unwrap();

                Some((
                    data1,
                    data2,
                    CString::from_vec_with_nul_unchecked(data3_bytes)
                        .into_string()
                        .unwrap(),
                    CString::from_vec_with_nul_unchecked(data4_bytes)
                        .into_string()
                        .unwrap(),
                ))
            } else {
                None
            };

            reset_stmt(self.get_access_data_stmt).unwrap();

            result
        }
    }

    fn update_subscriber_data(
        &mut self,
        bit_1: bool,
        mut s_id: u32,
        mut data_a: u8,
        mut sf_type: u8,
    ) {
        unsafe {
            let mut bit_1_u8 = bit_1 as u8;
            bind_parameter(self.update_subscriber_data_stmt, 1, &mut bit_1_u8).unwrap();
            bind_parameter(self.update_subscriber_data_stmt, 2, &mut s_id).unwrap();
            bind_parameter(self.update_subscriber_data_stmt, 3, &mut data_a).unwrap();
            bind_parameter(self.update_subscriber_data_stmt, 4, &mut sf_type).unwrap();

            execute_with_retry(self.update_subscriber_data_stmt);

            reset_stmt(self.update_subscriber_data_stmt).unwrap();
        }
    }

    fn update_location(&mut self, mut vlr_location: u32, mut s_id: u32) {
        unsafe {
            bind_parameter(self.update_location_stmt, 1, &mut vlr_location).unwrap();
            bind_parameter(self.update_location_stmt, 2, &mut s_id).unwrap();

            execute_with_retry(self.update_location_stmt);

            reset_stmt(self.update_location_stmt).unwrap();
        }
    }

    fn insert_call_forwarding(
        &mut self,
        mut s_id: u32,
        mut sf_type: u8,
        mut start_time: u8,
        mut end_time: u8,
        numberx: &str,
    ) {
        unsafe {
            bind_parameter(self.insert_call_forwarding_stmt, 1, &mut s_id).unwrap();
            bind_parameter(self.insert_call_forwarding_stmt, 2, &mut sf_type).unwrap();
            bind_parameter(self.insert_call_forwarding_stmt, 3, &mut start_time).unwrap();
            bind_parameter(self.insert_call_forwarding_stmt, 4, &mut end_time).unwrap();

            let mut numberx_bytes = numberx.as_bytes().to_vec();
            let mut numberx_char = Char::new(&mut numberx_bytes);

            bind_parameter(self.insert_call_forwarding_stmt, 5, &mut numberx_char).unwrap();

            execute_with_retry(self.insert_call_forwarding_stmt);

            reset_stmt(self.insert_call_forwarding_stmt).unwrap();
        }
    }

    fn delete_call_forwarding(&mut self, mut s_id: u32, mut sf_type: u8, mut start_time: u8) {
        unsafe {
            bind_parameter(self.delete_call_forwarding_stmt, 1, &mut s_id).unwrap();
            bind_parameter(self.delete_call_forwarding_stmt, 2, &mut sf_type).unwrap();
            bind_parameter(self.delete_call_forwarding_stmt, 3, &mut start_time).unwrap();

            execute_with_retry(self.delete_call_forwarding_stmt);

            reset_stmt(self.delete_call_forwarding_stmt).unwrap();
        }
    }
}

impl Drop for SQLServerTATPConnection {
    fn drop(&mut self) {
        unsafe {
            free_stmt(self.get_subscriber_data_stmt).unwrap();
            free_stmt(self.get_new_destination_stmt).unwrap();
            free_stmt(self.get_access_data_stmt).unwrap();
            free_stmt(self.update_subscriber_data_stmt).unwrap();
            free_stmt(self.update_location_stmt).unwrap();
            free_stmt(self.insert_call_forwarding_stmt).unwrap();
            free_stmt(self.delete_call_forwarding_stmt).unwrap();

            disconnect(self.dbc).unwrap();
            free_dbc(self.dbc).unwrap();
        }
    }
}

fn execute_with_retry(stmt: *mut Stmt) {
    while let Err(error) = unsafe { execute(stmt) } {
        match error {
            Error::NoDiagnositics => panic!("Statement execution returned unexpected error"),
            Error::Diagnostics(diagnostic_record) => {
                if diagnostic_record.native_error != 43102 {
                    panic!("{:?}", diagnostic_record);
                }
            }
        }
    }
}
