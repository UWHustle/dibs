use arrow::array::{BooleanArray, Float64Array, Int64Array, PrimitiveArrayOps};
use std::convert::TryInto;

pub mod scan;
pub mod seats;
pub mod tatp;
pub mod ycsb;

pub struct BooleanArrayMut(BooleanArray);

impl BooleanArrayMut {
    pub unsafe fn set(&self, i: usize) {
        let dst = self.0.values().raw_data().offset((i / 8) as isize) as *mut u8;
        *dst |= 1 << (i % 8);
    }

    pub unsafe fn clear(&self, i: usize) {
        let dst = self.0.values().raw_data().offset((i / 8) as isize) as *mut u8;
        *dst &= !(1 << (i % 8));
    }
}

pub struct Int64ArrayMut(Int64Array);

impl Int64ArrayMut {
    pub unsafe fn set(&self, i: usize, v: i64) {
        let dst = self.0.raw_values().offset(i.try_into().unwrap()) as *mut i64;
        *dst = v;
    }
}

pub struct Float64ArrayMut(Float64Array);

impl Float64ArrayMut {
    pub unsafe fn set(&self, i: usize, v: f64) {
        let dst = self.0.raw_values().offset(i.try_into().unwrap()) as *mut f64;
        *dst = v;
    }
}
