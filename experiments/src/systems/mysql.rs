use crate::benchmarks::ycsb;
use crate::benchmarks::ycsb::YCSBConnection;
use crate::Connection;
use itertools::Itertools;
use mysql::prelude::Queryable;
use mysql::{params, Conn, OptsBuilder, Statement, TxOpts};
use rand::distributions::Alphanumeric;
use rand::seq::SliceRandom;
use rand::Rng;
use std::str::FromStr;

#[derive(PartialEq, Clone, Copy)]
pub enum IsolationMechanism {
    MySQLSerializable,
    MySQLReadUncommitted,
    DibsSerializable,
}

impl FromStr for IsolationMechanism {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, ()> {
        match s {
            "MySQLSerializable" => Ok(IsolationMechanism::MySQLSerializable),
            "MySQLReadUncommitted" => Ok(IsolationMechanism::MySQLReadUncommitted),
            "DibsSerializable" => Ok(IsolationMechanism::DibsSerializable),
            _ => Err(()),
        }
    }
}

pub fn load_ycsb(num_rows: u32, field_size: usize) {
    assert!(num_rows > 0);
    assert_eq!(num_rows % 1000, 0);

    let mut rng = rand::thread_rng();

    let mut conn = Conn::new(OptsBuilder::new().user(Some("dibs")).db_name(Some("ycsb"))).unwrap();

    conn.query_drop("DROP TABLE IF EXISTS ycsb.users;").unwrap();

    conn.query_drop(&format!(
        "CREATE TABLE ycsb.users (id INTEGER PRIMARY KEY, {});",
        (0..ycsb::NUM_FIELDS)
            .map(|field| format!("field_{} CHAR({})", field, field_size))
            .join(",")
    ))
    .unwrap();

    let mut ids = (0..num_rows).collect::<Vec<_>>();
    ids.shuffle(&mut rng);

    let mut transaction = conn.start_transaction(TxOpts::default()).unwrap();

    for i in 0..num_rows as usize / 1000 {
        transaction
            .query_drop(&format!(
                "INSERT INTO ycsb.users VALUES {};",
                ids.iter()
                    .skip(i * 1000)
                    .take(1000)
                    .map(|&id| format!(
                        "({},{})",
                        id,
                        (0..ycsb::NUM_FIELDS)
                            .map(|_| format!(
                                "'{}'",
                                rng.sample_iter(&Alphanumeric)
                                    .take(field_size)
                                    .collect::<String>()
                            ))
                            .join(",")
                    ))
                    .join(",")
            ))
            .unwrap();
    }

    transaction.commit().unwrap();
}

pub struct MySQLYCSBConnection {
    conn: Conn,
    select_user_stmts: Vec<Statement>,
    update_user_stmts: Vec<Statement>,
}

impl MySQLYCSBConnection {
    pub fn new(isolation: IsolationMechanism) -> MySQLYCSBConnection {
        let mut conn =
            Conn::new(OptsBuilder::new().user(Some("dibs")).db_name(Some("ycsb"))).unwrap();

        conn.query_drop(format!(
            "SET SESSION TRANSACTION ISOLATION LEVEL {};",
            match isolation {
                IsolationMechanism::MySQLSerializable => "SERIALIZABLE",
                IsolationMechanism::MySQLReadUncommitted | IsolationMechanism::DibsSerializable => {
                    "READ UNCOMMITTED"
                }
            }
        ))
        .unwrap();

        let select_user_stmts = (0..ycsb::NUM_FIELDS)
            .map(|field| {
                conn.prep(&format!(
                    "SELECT field_{} FROM ycsb.users WHERE id = ?;",
                    field
                ))
                .unwrap()
            })
            .collect();

        let update_user_stmts = (0..ycsb::NUM_FIELDS)
            .map(|field| {
                conn.prep(&format!(
                    "UPDATE ycsb.users SET field_{} = :field WHERE id = :id;",
                    field
                ))
                .unwrap()
            })
            .collect();

        MySQLYCSBConnection {
            conn,
            select_user_stmts,
            update_user_stmts,
        }
    }
}

impl Connection for MySQLYCSBConnection {
    fn begin(&mut self) {
        self.conn.query_drop("START TRANSACTION").unwrap();
    }

    fn commit(&mut self) {
        self.conn.query_drop("COMMIT").unwrap();
    }

    fn rollback(&mut self) {
        unimplemented!()
    }

    fn savepoint(&mut self) {
        unimplemented!()
    }
}

impl YCSBConnection for MySQLYCSBConnection {
    fn select_user(&mut self, field: usize, user_id: u32) -> String {
        self.conn
            .exec_first(&self.select_user_stmts[field], (user_id,))
            .unwrap()
            .unwrap()
    }

    fn update_user(&mut self, field: usize, data: &str, user_id: u32) {
        self.conn
            .exec_drop(
                &self.update_user_stmts[field],
                params! {
                    "field" => data,
                    "id" => user_id
                },
            )
            .unwrap();
    }
}
