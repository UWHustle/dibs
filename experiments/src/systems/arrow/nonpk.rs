use crate::benchmarks::nonpk::NonPKConnection;
use crate::Connection;
use arrow::array::UInt32Array;
use fnv::FnvHashMap;
use rand::seq::SliceRandom;
use std::sync::Arc;

pub struct ArrowNonPKDatabase {
    col_pk: UInt32Array,
    _col_non_pk: UInt32Array,
    col_field: UInt32Array,
    index_pk: FnvHashMap<u32, usize>,
    index_non_pk: FnvHashMap<u32, usize>,
}

impl ArrowNonPKDatabase {
    pub fn new(num_rows: u32) -> ArrowNonPKDatabase {
        let mut rng = rand::thread_rng();

        let mut col_pks = (0..num_rows).collect::<Vec<_>>();
        let mut col_non_pks = col_pks.clone();
        let mut col_fields = col_pks.clone();

        col_pks.shuffle(&mut rng);
        col_non_pks.shuffle(&mut rng);
        col_fields.shuffle(&mut rng);

        let index_pk = col_pks.iter().enumerate().map(|(i, &v)| (v, i)).collect();
        let index_non_pk = col_non_pks
            .iter()
            .enumerate()
            .map(|(i, &v)| (v, i))
            .collect();

        let col_pk = UInt32Array::from(col_pks);
        let col_non_pk = UInt32Array::from(col_non_pks);
        let col_field = UInt32Array::from(col_fields);

        ArrowNonPKDatabase {
            col_pk,
            _col_non_pk: col_non_pk,
            col_field,
            index_pk,
            index_non_pk,
        }
    }
}

pub struct ArrowNonPKConnection {
    db: Arc<ArrowNonPKDatabase>,
}

impl ArrowNonPKConnection {
    pub fn new(db: Arc<ArrowNonPKDatabase>) -> ArrowNonPKConnection {
        ArrowNonPKConnection { db }
    }
}

impl Connection for ArrowNonPKConnection {
    fn begin(&mut self) {}

    fn commit(&mut self) {}

    fn rollback(&mut self) {}

    fn savepoint(&mut self) {}
}

impl NonPKConnection for ArrowNonPKConnection {
    fn get_pk(&self, non_pk_v: u32) -> u32 {
        let row = self.db.index_non_pk[&non_pk_v];
        self.db.col_pk.value(row)
    }

    fn update(&self, pk_v: u32, field_v: u32) {
        let row = self.db.index_pk[&pk_v];
        let dst = &self.db.col_field.values()[row] as *const u32 as *mut u32;
        unsafe {
            *dst = field_v;
        }
    }
}
