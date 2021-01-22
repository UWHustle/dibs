use crate::benchmarks::tatp;
use crate::benchmarks::tatp_sp::TATPSPConnection;
use crate::Connection;
use itertools::Itertools;
use rand::seq::SliceRandom;
use rand::Rng;

fn exec_direct(conn: &odbc::Connection<odbc::safe::AutocommitOn>, sql: &str) {
    odbc::Statement::with_parent(&conn)
        .unwrap()
        .exec_direct(sql)
        .unwrap();
}

pub fn load_tatp(num_rows: u32) {
    assert!(num_rows > 0);
    assert_eq!(num_rows % 100, 0);

    let mut rng = rand::thread_rng();

    let env = odbc::create_environment_v3().unwrap();
    let conn = env.connect("DIBS", "SA", "DIBS123!").unwrap();

    exec_direct(&conn, "USE dibs;");

    exec_direct(&conn, "DROP PROCEDURE IF EXISTS tatp.get_subscriber_data");
    exec_direct(&conn, "DROP PROCEDURE IF EXISTS tatp.get_new_destination");
    exec_direct(&conn, "DROP PROCEDURE IF EXISTS tatp.get_access_data");
    exec_direct(
        &conn,
        "DROP PROCEDURE IF EXISTS tatp.update_subscriber_data",
    );
    exec_direct(&conn, "DROP PROCEDURE IF EXISTS tatp.update_location");
    exec_direct(
        &conn,
        "DROP PROCEDURE IF EXISTS tatp.insert_call_forwarding",
    );
    exec_direct(
        &conn,
        "DROP PROCEDURE IF EXISTS tatp.delete_call_forwarding",
    );

    exec_direct(&conn, "DROP TABLE IF EXISTS tatp.call_forwarding;");
    exec_direct(&conn, "DROP TABLE IF EXISTS tatp.special_facility;");
    exec_direct(&conn, "DROP TABLE IF EXISTS tatp.access_info;");
    exec_direct(&conn, "DROP TABLE IF EXISTS tatp.subscriber;");

    exec_direct(&conn, "DROP SCHEMA IF EXISTS tatp");

    exec_direct(&conn, "CREATE SCHEMA tatp");

    exec_direct(
        &conn,
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
    );

    exec_direct(
        &conn,
        "CREATE TABLE tatp.access_info (s_id INTEGER NOT NULL,
                ai_type TINYINT NOT NULL,
                data1 TINYINT, data2 TINYINT, data3 VARCHAR(3), data4 VARCHAR(5),
                PRIMARY KEY NONCLUSTERED (s_id, ai_type),
                FOREIGN KEY (s_id) REFERENCES tatp.subscriber (s_id))
             WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_ONLY);",
    );

    exec_direct(
        &conn,
        "CREATE TABLE tatp.special_facility (s_id INTEGER NOT NULL,
                sf_type TINYINT NOT NULL,
                is_active TINYINT, error_cntrl TINYINT,
                data_a TINYINT, data_b VARCHAR(5),
                PRIMARY KEY NONCLUSTERED (s_id, sf_type),
                FOREIGN KEY (s_id) REFERENCES tatp.subscriber (s_id))
             WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_ONLY);",
    );

    exec_direct(
        &conn,
        "CREATE TABLE tatp.call_forwarding (s_id INTEGER NOT NULL,
                sf_type TINYINT NOT NULL,
                start_time TINYINT, end_time TINYINT, numberx VARCHAR(15),
                PRIMARY KEY NONCLUSTERED (s_id, sf_type, start_time))
             WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_ONLY);",
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
        exec_direct(
            &conn,
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
        );
    }

    for ai_chunk in ai_types.chunks(1000) {
        exec_direct(
            &conn,
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
        );
    }

    for sf_chunk in sf_types.chunks(1000) {
        exec_direct(
            &conn,
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
        );
    }

    for cf_chunk in cf_start_times.chunks(1000) {
        exec_direct(
            &conn,
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
        );
    }

    let prepare = |name, params: &[&str], sql| {
        exec_direct(
            &conn,
            &format!(
                "CREATE PROCEDURE {}
                ({})
                WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
                AS BEGIN ATOMIC WITH
                (TRANSACTION ISOLATION LEVEL = SERIALIZABLE, LANGUAGE = 'english')
                    DECLARE @retry BIT = 1;
                    WHILE (@retry = 1)
                    BEGIN
                        BEGIN TRY
                            {}
                            SET @retry = 0;
                        END TRY
                        
                        BEGIN CATCH
                            IF (ERROR_NUMBER() NOT IN 
                                (41302, 41305, 41325, 41301, 41823, 41840, 41839, 1205))
                            BEGIN
                                THROW
                            END
                        END CATCH
                    END
                END",
                name,
                params.iter().join(", "),
                sql,
            ),
        );
    };

    prepare(
        "tatp.get_subscriber_data",
        &["@s_id INT"],
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
    );

    prepare(
        "tatp.get_new_destination",
        &[
            "@s_id INT",
            "@sf_type TINYINT",
            "@start_time TINYINT",
            "@end_time TINYINT",
        ],
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
    );

    prepare(
        "tatp.get_access_data",
        &["@s_id INT", "@ai_type TINYINT"],
        "SELECT data1, data2, data3, data4
        FROM tatp.access_info
        WHERE s_id = @s_id
            AND ai_type = @ai_type",
    );

    prepare(
        "tatp.update_subscriber_data",
        &[
            "@bit_1 TINYINT",
            "@s_id INT",
            "@data_a TINYINT",
            "@sf_type TINYINT",
        ],
        "UPDATE tatp.subscriber
        SET bit_1 = @bit_1
        WHERE s_id = @s_id;

        UPDATE tatp.special_facility
        SET data_a = @data_a
        WHERE s_id = @s_id
            AND sf_type = @sf_type;",
    );

    prepare(
        "tatp.update_location",
        &["@vlr_location BIGINT", "@s_id INT"],
        "UPDATE tatp.subscriber
        SET vlr_location = @vlr_location
        WHERE s_id = @s_id;",
    );

    prepare(
        "tatp.insert_call_forwarding",
        &[
            "@s_id INT",
            "@sf_type TINYINT",
            "@start_time TINYINT",
            "@end_time TINYINT",
            "@numberx VARCHAR(15)",
        ],
        "SELECT sf_type
        FROM tatp.special_facility
        WHERE s_id = @s_id;

        INSERT INTO tatp.call_forwarding
        VALUES (@s_id, @sf_type, @start_time, @end_time, @numberx);",
    );

    prepare(
        "tatp.delete_call_forwarding",
        &["@s_id INT", "@sf_type TINYINT", "@start_time TINYINT"],
        "DELETE FROM tatp.call_forwarding
        WHERE s_id = @s_id
            AND sf_type = @sf_type
            AND start_time = @start_time;",
    );
}

pub struct SQLServerTATPConnection<'a> {
    conn: odbc::Connection<'a, odbc::safe::AutocommitOn>,
    _env: Box<odbc::Environment<odbc::Version3>>,
}

impl<'a> SQLServerTATPConnection<'a> {
    pub fn new() -> SQLServerTATPConnection<'a> {
        let env = Box::into_raw(Box::new(odbc::create_environment_v3().unwrap()));
        let conn = unsafe { &*env }.connect("DIBS", "SA", "DIBS123!").unwrap();

        exec_direct(&conn, "USE dibs;");

        SQLServerTATPConnection {
            conn,
            _env: unsafe { Box::from_raw(env) },
        }
    }
}

impl Connection for SQLServerTATPConnection<'_> {
    fn begin(&mut self) {}

    fn commit(&mut self) {}

    fn rollback(&mut self) {}

    fn savepoint(&mut self) {}
}

impl<'a> TATPSPConnection for SQLServerTATPConnection<'a> {
    fn get_subscriber_data(&mut self, s_id: u32) -> ([bool; 10], [u8; 10], [u8; 10], u32, u32) {
        match odbc::Statement::with_parent(&self.conn)
            .unwrap()
            .bind_parameter(1, &s_id)
            .unwrap()
            .exec_direct("{ CALL tatp.get_subscriber_data (?) }")
            .unwrap()
        {
            odbc::ResultSetState::Data(mut stmt) => {
                let mut cursor = stmt.fetch().unwrap().unwrap();

                let mut bit = [false; 10];
                for i in 0..10 {
                    bit[i] = cursor.get_data((i + 1) as u16).unwrap().unwrap();
                }

                let mut hex = [0; 10];
                for i in 0..10 {
                    hex[i] = cursor.get_data((i + 11) as u16).unwrap().unwrap();
                }

                let mut byte2 = [0; 10];
                for i in 0..10 {
                    byte2[i] = cursor.get_data((i + 21) as u16).unwrap().unwrap();
                }

                let msc_location = cursor.get_data(31).unwrap().unwrap();

                let vlr_location = cursor.get_data(32).unwrap().unwrap();

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
        match odbc::Statement::with_parent(&self.conn)
            .unwrap()
            .bind_parameter(1, &s_id)
            .unwrap()
            .bind_parameter(2, &sf_type)
            .unwrap()
            .bind_parameter(3, &start_time)
            .unwrap()
            .bind_parameter(4, &end_time)
            .unwrap()
            .exec_direct("{ CALL tatp.get_new_destination (?, ?, ?, ?) }")
            .unwrap()
        {
            odbc::ResultSetState::Data(mut stmt) => {
                let mut numberx = vec![];

                while let Some(mut cursor) = stmt.fetch().unwrap() {
                    numberx.push(cursor.get_data(1).unwrap().unwrap());
                }

                numberx
            }
            odbc::ResultSetState::NoData(_) => panic!(),
        }
    }

    fn get_access_data(&mut self, s_id: u32, ai_type: u8) -> Option<(u8, u8, String, String)> {
        match odbc::Statement::with_parent(&self.conn)
            .unwrap()
            .bind_parameter(1, &s_id)
            .unwrap()
            .bind_parameter(2, &ai_type)
            .unwrap()
            .exec_direct("{ CALL tatp.get_access_data (?, ?) }")
            .unwrap()
        {
            odbc::ResultSetState::Data(mut stmt) => stmt.fetch().unwrap().map(|mut cursor| {
                let data1 = cursor.get_data(1).unwrap().unwrap();
                let data2 = cursor.get_data(2).unwrap().unwrap();
                let data3 = cursor.get_data(3).unwrap().unwrap();
                let data4 = cursor.get_data(4).unwrap().unwrap();

                (data1, data2, data3, data4)
            }),
            odbc::ResultSetState::NoData(_) => panic!(),
        }
    }

    fn update_subscriber_data(&mut self, bit_1: bool, s_id: u32, data_a: u8, sf_type: u8) {
        odbc::Statement::with_parent(&self.conn)
            .unwrap()
            .bind_parameter(1, &bit_1)
            .unwrap()
            .bind_parameter(2, &s_id)
            .unwrap()
            .bind_parameter(3, &data_a)
            .unwrap()
            .bind_parameter(4, &sf_type)
            .unwrap()
            .exec_direct("{ CALL tatp.update_subscriber_data (?, ?, ?, ?) }")
            .unwrap();
    }

    fn update_location(&mut self, vlr_location: u32, s_id: u32) {
        odbc::Statement::with_parent(&self.conn)
            .unwrap()
            .bind_parameter(1, &vlr_location)
            .unwrap()
            .bind_parameter(2, &s_id)
            .unwrap()
            .exec_direct("{ CALL tatp.update_location (?, ?) }")
            .unwrap();
    }

    fn insert_call_forwarding(
        &mut self,
        s_id: u32,
        sf_type: u8,
        start_time: u8,
        end_time: u8,
        numberx: &str,
    ) {
        odbc::Statement::with_parent(&self.conn)
            .unwrap()
            .bind_parameter(1, &s_id)
            .unwrap()
            .bind_parameter(2, &sf_type)
            .unwrap()
            .bind_parameter(3, &start_time)
            .unwrap()
            .bind_parameter(4, &end_time)
            .unwrap()
            .bind_parameter(5, &numberx)
            .unwrap()
            .exec_direct("{ CALL tatp.insert_call_forwarding (?, ?, ?, ?, ?) }")
            .unwrap();
    }

    fn delete_call_forwarding(&mut self, s_id: u32, sf_type: u8, start_time: u8) {
        odbc::Statement::with_parent(&self.conn)
            .unwrap()
            .bind_parameter(1, &s_id)
            .unwrap()
            .bind_parameter(2, &sf_type)
            .unwrap()
            .bind_parameter(3, &start_time)
            .unwrap()
            .exec_direct("{ CALL tatp.delete_call_forwarding (?, ?, ?) }")
            .unwrap();
    }
}
