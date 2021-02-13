use crate::benchmarks::ycsb;
use crate::benchmarks::ycsb::YCSBConnection;
use crate::Connection;
use arrow::array::{FixedSizeBinaryArray, FixedSizeBinaryBuilder, UInt32Array, UInt32Builder};
use fnv::FnvHashMap;
use rand::distributions::Alphanumeric;
use rand::seq::SliceRandom;
use rand::Rng;
use std::sync::Arc;

pub struct ArrowYCSBDatabase {
    _col_user_id: UInt32Array,
    col_fields: Vec<FixedSizeBinaryArray>,
    index: FnvHashMap<u32, usize>,
}

impl ArrowYCSBDatabase {
    pub fn new(num_rows: u32, field_size: usize) -> ArrowYCSBDatabase {
        assert!(field_size > 0 && field_size <= i32::max_value() as usize);

        let mut rng = rand::thread_rng();

        let mut user_ids = (0..num_rows).collect::<Vec<_>>();
        user_ids.shuffle(&mut rng);

        let mut user_id_builder = UInt32Builder::new(user_ids.len());
        let mut field_builders = (0..ycsb::NUM_FIELDS)
            .map(|_| FixedSizeBinaryBuilder::new(user_ids.len(), field_size as i32))
            .collect::<Vec<_>>();

        let mut index = FnvHashMap::default();

        for (row, &user_id) in user_ids.iter().enumerate() {
            user_id_builder.append_value(user_id).unwrap();

            for field_builder in &mut field_builders {
                field_builder
                    .append_value(
                        rng.sample_iter(&Alphanumeric)
                            .take(field_size)
                            .collect::<String>()
                            .as_bytes(),
                    )
                    .unwrap();
            }

            index.insert(user_id, row);
        }

        ArrowYCSBDatabase {
            _col_user_id: user_id_builder.finish(),
            col_fields: field_builders.into_iter().map(|mut b| b.finish()).collect(),
            index,
        }
    }
}

pub struct ArrowYCSBConnection {
    db: Arc<ArrowYCSBDatabase>,
}

impl ArrowYCSBConnection {
    pub fn new(db: Arc<ArrowYCSBDatabase>) -> ArrowYCSBConnection {
        ArrowYCSBConnection { db }
    }
}

impl Connection for ArrowYCSBConnection {
    fn begin(&mut self) {}
    fn commit(&mut self) {}
    fn rollback(&mut self) {}
    fn savepoint(&mut self) {}
}

impl YCSBConnection for ArrowYCSBConnection {
    fn select_user(&mut self, field: usize, user_id: u32) -> String {
        let row = self.db.index.get(&user_id).unwrap();
        String::from_utf8(self.db.col_fields[field].value(*row).to_vec()).unwrap()
    }

    fn update_user(&mut self, field: usize, data: &str, user_id: u32) {
        let row = self.db.index.get(&user_id).unwrap();
        let value = self.db.col_fields[field].value(*row);

        assert_eq!(data.len(), value.len());

        let data_dst = value.as_ptr() as *mut u8;

        unsafe {
            data_dst.copy_from(data.as_ptr(), data.len());
        }
    }
}
