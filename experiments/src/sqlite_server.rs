use crate::tatp::TATPConnection;
use crate::{tatp, Connection};
use itertools::Itertools;
use rand::seq::SliceRandom;
use rand::Rng;
use rusqlite::{params, ErrorCode, Statement};
use std::path::Path;

pub fn load_tatp<P>(path: P, num_rows: u32)
where
    P: AsRef<Path>,
{
    let mut rng = rand::thread_rng();

    let conn = rusqlite::Connection::open(path).unwrap();

    // unsafe {
    //     libsqlite3_sys::sqlite3_config(libsqlite3_sys::SQLITE_CONFIG_MULTITHREAD);
    //     libsqlite3_sys::sqlite3_config(libsqlite3_sys::SQLITE_CONFIG_MEMSTATUS, 0);
    // }

    conn.pragma_update(None, "journal_mode", &"WAL").unwrap();
    conn.pragma_update(None, "synchronous", &"FULL").unwrap();

    conn.execute("DROP TABLE IF EXISTS subscriber;", params![])
        .unwrap();
    conn.execute("DROP TABLE IF EXISTS access_info;", params![])
        .unwrap();
    conn.execute("DROP TABLE IF EXISTS special_facility;", params![])
        .unwrap();
    conn.execute("DROP TABLE IF EXISTS call_forwarding;", params![])
        .unwrap();

    conn.execute(
        "CREATE TABLE subscriber (s_id INTEGER PRIMARY KEY,
                    bit_1 INTEGER, bit_2 INTEGER, bit_3 INTEGER, bit_4 INTEGER,
                    bit_5 INTEGER, bit_6 INTEGER, bit_7 INTEGER, bit_8 INTEGER,
                    bit_9 INTEGER, bit_10 INTEGER,
                    hex_1 INTEGER, hex_2 INTEGER, hex_3 INTEGER, hex_4 INTEGER,
                    hex_5 INTEGER, hex_6 INTEGER, hex_7 INTEGER, hex_8 INTEGER,
                    hex_9 INTEGER, hex_10 INTEGER,
                    byte2_1 INTEGER, byte2_2 INTEGER, byte2_3 INTEGER, byte2_4 INTEGER,
                    byte2_5 INTEGER, byte2_6 INTEGER, byte2_7 INTEGER, byte2_8 INTEGER,
                    byte2_9 INTEGER, byte2_10 INTEGER,
                    msc_location INTEGER, vlr_location INTEGER);",
        params![],
    )
    .unwrap();

    conn.execute(
        "CREATE TABLE access_info (s_id INTEGER NOT NULL,
                ai_type INTEGER NOT NULL,
                data1 INTEGER, data2 INTEGER, data3 TEXT, data4 TEXT,
                PRIMARY KEY (s_id, ai_type),
                FOREIGN KEY (s_id) REFERENCES Subscriber (s_id));",
        params![],
    )
    .unwrap();

    conn.execute(
        "CREATE TABLE special_facility (s_id INTEGER NOT NULL,
                sf_type INTEGER NOT NULL,
                is_active INTEGER, error_cntrl INTEGER,
                data_a INTEGER, data_b TEXT,
                PRIMARY KEY (s_id, sf_type),
                FOREIGN KEY (s_id) REFERENCES Subscriber (s_id));",
        params![],
    )
    .unwrap();

    conn.execute(
        "CREATE TABLE call_forwarding (s_id INTEGER NOT NULL,
                sf_type INTEGER NOT NULL,
                start_time INTEGER, end_time INTEGER, numberx TEXT,
                PRIMARY KEY (s_id, sf_type, start_time),
                FOREIGN KEY (s_id, sf_type)
                REFERENCES Special_Facility(s_id, sf_type));",
        params![],
    )
    .unwrap();

    let mut s_ids = (1..=num_rows).collect::<Vec<_>>();
    s_ids.shuffle(&mut rng);

    conn.execute(
        &format!(
            "INSERT INTO subscriber VALUES {};",
            s_ids
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
        params![],
    )
    .unwrap();

    conn.execute(
        &format!(
            "INSERT INTO access_info VALUES {};",
            s_ids
                .iter()
                .flat_map(|&s_id| {
                    let num_ai_types = rng.gen_range(1, 5);
                    [1, 2, 3, 4]
                        .choose_multiple(&mut rng, num_ai_types)
                        .map(move |&ai_type| {
                            format!(
                                "({},{},{},{},'{}','{}')",
                                s_id,
                                ai_type,
                                rng.gen::<u8>(),
                                rng.gen::<u8>(),
                                tatp::uppercase_alphabetic_string(3, &mut rng),
                                tatp::uppercase_alphabetic_string(5, &mut rng)
                            )
                        })
                })
                .join(",")
        ),
        params![],
    )
    .unwrap();

    let sf_types = s_ids
        .iter()
        .flat_map(|&s_id| {
            let num_sf_types = rng.gen_range(1, 5);
            [1, 2, 3, 4]
                .choose_multiple(&mut rng, num_sf_types)
                .map(move |&sf_type| (s_id, sf_type))
        })
        .collect::<Vec<_>>();

    conn.execute(
        &format!(
            "INSERT INTO special_facility VALUES {};",
            sf_types
                .iter()
                .map(|&(s_id, sf_type)| {
                    format!(
                        "({},{},{},{},{},'{}')",
                        s_id,
                        sf_type,
                        if rng.gen_bool(0.85) { 1 } else { 0 },
                        rng.gen::<u8>(),
                        rng.gen::<u8>(),
                        tatp::uppercase_alphabetic_string(5, &mut rng),
                    )
                })
                .join(",")
        ),
        params![],
    )
    .unwrap();

    conn.execute(
        &format!(
            "INSERT INTO call_forwarding VALUES {};",
            sf_types
                .iter()
                .flat_map(|&(s_id, sf_type)| {
                    let num_start_times = rng.gen_range(0, 4);
                    [0, 8, 16]
                        .choose_multiple(&mut rng, num_start_times)
                        .map(move |&start_time| {
                            format!(
                                "({},{},{},{},'{}')",
                                s_id,
                                sf_type,
                                start_time,
                                start_time + rng.gen_range(1, 9),
                                tatp::uppercase_alphabetic_string(15, &mut rng)
                            )
                        })
                })
                .join(",")
        ),
        params![],
    )
    .unwrap();
}

struct SQLiteBaseConnection<'a> {
    begin_stmt: Statement<'a>,
    commit_stmt: Statement<'a>,
    rollback_stmt: Statement<'a>,
    savepoint_stmt: Statement<'a>,
}

impl<'a> SQLiteBaseConnection<'a> {
    fn new(conn: *mut rusqlite::Connection) -> SQLiteBaseConnection<'a> {
        let begin_stmt = unsafe { conn.as_ref() }.unwrap().prepare("BEGIN;").unwrap();

        let commit_stmt = unsafe { conn.as_ref() }
            .unwrap()
            .prepare("COMMIT;")
            .unwrap();

        let rollback_stmt = unsafe { conn.as_ref() }
            .unwrap()
            .prepare("ROLLBACK TO 'X';")
            .unwrap();

        let savepoint_stmt = unsafe { conn.as_ref() }
            .unwrap()
            .prepare("SAVEPOINT 'X';")
            .unwrap();

        SQLiteBaseConnection {
            begin_stmt,
            commit_stmt,
            rollback_stmt,
            savepoint_stmt,
        }
    }
}

pub struct SQLiteTATPConnection<'a> {
    _conn: Box<rusqlite::Connection>,
    base: SQLiteBaseConnection<'a>,
    get_subscriber_data_stmt: Statement<'a>,
    get_new_destination_stmt: Statement<'a>,
    get_access_data_stmt: Statement<'a>,
    update_subscriber_bit_stmt: Statement<'a>,
    update_special_facility_data_stmt: Statement<'a>,
    update_subscriber_location_stmt: Statement<'a>,
    get_special_facility_types_stmt: Statement<'a>,
    insert_call_forwarding_stmt: Statement<'a>,
    delete_call_forwarding_stmt: Statement<'a>,
}

impl<'a> SQLiteTATPConnection<'a> {
    pub fn new<P>(path: P) -> SQLiteTATPConnection<'a>
    where
        P: AsRef<Path>,
    {
        let conn = Box::into_raw(Box::new(rusqlite::Connection::open(path).unwrap()));

        let base = SQLiteBaseConnection::new(conn);

        let get_subscriber_data_stmt = unsafe { conn.as_ref() }
            .unwrap()
            .prepare(
                "SELECT *
                FROM subscriber
                WHERE s_id = ?;",
            )
            .unwrap();

        let get_new_destination_stmt = unsafe { conn.as_ref() }
            .unwrap()
            .prepare(
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
            )
            .unwrap();

        let get_access_data_stmt = unsafe { conn.as_ref() }
            .unwrap()
            .prepare(
                "SELECT data1, data2, data3, data4
                        FROM access_info
                        WHERE s_id = ? AND ai_type = ?;",
            )
            .unwrap();

        let update_subscriber_bit_stmt = unsafe { conn.as_ref() }
            .unwrap()
            .prepare(
                "UPDATE subscriber
                        SET bit_1 = ?
                        WHERE s_id = ?;",
            )
            .unwrap();

        let update_special_facility_data_stmt = unsafe { conn.as_ref() }
            .unwrap()
            .prepare(
                "UPDATE special_facility
                        SET data_a = ?
                        WHERE s_id = ? AND sf_type = ?;",
            )
            .unwrap();

        let update_subscriber_location_stmt = unsafe { conn.as_ref() }
            .unwrap()
            .prepare(
                "UPDATE subscriber
                        SET vlr_location = ?
                        WHERE s_id = ?;",
            )
            .unwrap();

        let get_special_facility_types_stmt = unsafe { conn.as_ref() }
            .unwrap()
            .prepare(
                "SELECT sf_type
                        FROM special_facility
                        WHERE s_id = ?;",
            )
            .unwrap();

        let insert_call_forwarding_stmt = unsafe { conn.as_ref() }
            .unwrap()
            .prepare(
                "INSERT INTO call_forwarding
                        VALUES (?, ?, ?, ?, ?);",
            )
            .unwrap();

        let delete_call_forwarding_stmt = unsafe { conn.as_ref() }
            .unwrap()
            .prepare(
                "DELETE FROM call_forwarding
                        WHERE s_id = ? AND sf_type = ? AND start_time = ?;",
            )
            .unwrap();

        SQLiteTATPConnection {
            _conn: unsafe { Box::from_raw(conn) },
            base,
            get_subscriber_data_stmt,
            get_new_destination_stmt,
            get_access_data_stmt,
            update_subscriber_bit_stmt,
            update_special_facility_data_stmt,
            update_subscriber_location_stmt,
            get_special_facility_types_stmt,
            insert_call_forwarding_stmt,
            delete_call_forwarding_stmt,
        }
    }
}

impl Connection for SQLiteTATPConnection<'_> {
    fn begin(&mut self) {
        self.base.begin_stmt.execute(params![]).unwrap();
    }

    fn commit(&mut self) {
        self.base.commit_stmt.execute(params![]).unwrap();
    }

    fn rollback(&mut self) {
        self.base.rollback_stmt.execute(params![]).unwrap();
    }

    fn savepoint(&mut self) {
        self.base.savepoint_stmt.execute(params![]).unwrap();
    }
}

impl TATPConnection for SQLiteTATPConnection<'_> {
    fn get_subscriber_data(&mut self, s_id: u32) -> ([bool; 10], [u8; 10], [u8; 10], u32, u32) {
        let mut rows = self.get_subscriber_data_stmt.query(&[s_id]).unwrap();
        let row = rows.next().unwrap().unwrap();

        let mut bit = [false; 10];
        for i in 0..10 {
            bit[i] = row.get(i + 1).unwrap();
        }

        let mut hex = [0; 10];
        for i in 0..10 {
            hex[i] = row.get(i + 11).unwrap();
        }

        let mut byte2 = [0; 10];
        for i in 0..10 {
            byte2[i] = row.get(i + 21).unwrap();
        }

        (bit, hex, byte2, row.get(31).unwrap(), row.get(32).unwrap())
    }

    fn get_new_destination(
        &mut self,
        s_id: u32,
        sf_type: u8,
        start_time: u8,
        end_time: u8,
    ) -> Vec<String> {
        let mut numberx = vec![];

        let mut rows = self
            .get_new_destination_stmt
            .query(params![s_id, sf_type, start_time, end_time])
            .unwrap();

        while let Some(row) = rows.next().unwrap() {
            numberx.push(row.get(0).unwrap());
        }

        numberx
    }

    fn get_access_data(&mut self, s_id: u32, ai_type: u8) -> Option<(u8, u8, String, String)> {
        let mut rows = self
            .get_access_data_stmt
            .query(params![s_id, ai_type])
            .unwrap();

        rows.next().unwrap().map(|row| {
            (
                row.get(0).unwrap(),
                row.get(1).unwrap(),
                row.get(2).unwrap(),
                row.get(3).unwrap(),
            )
        })
    }

    fn update_subscriber_bit(&mut self, bit_1: bool, s_id: u32) {
        self.update_subscriber_bit_stmt
            .execute(params![bit_1, s_id])
            .unwrap();
    }

    fn update_special_facility_data(&mut self, data_a: u8, s_id: u32, sf_type: u8) {
        self.update_special_facility_data_stmt
            .execute(params![data_a, s_id, sf_type])
            .unwrap();
    }

    fn update_subscriber_location(&mut self, vlr_location: u32, s_id: u32) {
        self.update_subscriber_location_stmt
            .execute(params![vlr_location, s_id])
            .unwrap();
    }

    fn get_special_facility_types(&mut self, s_id: u32) -> Vec<u8> {
        let mut sf_type = vec![];

        let mut rows = self.get_special_facility_types_stmt.query(&[s_id]).unwrap();

        while let Some(row) = rows.next().unwrap() {
            sf_type.push(row.get(0).unwrap());
        }

        sf_type
    }

    fn insert_call_forwarding(
        &mut self,
        s_id: u32,
        sf_type: u8,
        start_time: u8,
        end_time: u8,
        numberx: &str,
    ) {
        if let Err(error) = self
            .insert_call_forwarding_stmt
            .execute(params![s_id, sf_type, start_time, end_time, numberx])
        {
            match &error {
                rusqlite::Error::SqliteFailure(sqlite_error, _) => {
                    if sqlite_error.code != ErrorCode::ConstraintViolation {
                        panic!(error.to_string())
                    }
                }
                _ => panic!(error.to_string()),
            }
        }
    }

    fn delete_call_forwarding(&mut self, s_id: u32, sf_type: u8, start_time: u8) {
        self.delete_call_forwarding_stmt
            .execute(params![s_id, sf_type, start_time])
            .unwrap();
    }
}

impl Drop for SQLiteTATPConnection<'_> {
    fn drop(&mut self) {
    }
}

unsafe impl Send for SQLiteTATPConnection<'_> {}
