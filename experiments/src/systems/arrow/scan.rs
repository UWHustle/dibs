use crate::benchmarks::scan::ScanConnection;
use crate::systems::arrow::tatp::Subscriber;
use crate::Connection;
use std::sync::Arc;

pub struct ArrowScanDatabase {
    subscriber: Subscriber,
}

impl ArrowScanDatabase {
    pub fn new(num_rows: u32) -> ArrowScanDatabase {
        ArrowScanDatabase {
            subscriber: Subscriber::new(num_rows),
        }
    }
}

pub struct ArrowScanConnection {
    db: Arc<ArrowScanDatabase>,
}

impl ArrowScanConnection {
    pub fn new(db: Arc<ArrowScanDatabase>) -> ArrowScanConnection {
        ArrowScanConnection { db }
    }
}

impl Connection for ArrowScanConnection {
    fn begin(&mut self) {}
    fn commit(&mut self) {}
    fn rollback(&mut self) {}
    fn savepoint(&mut self) {}
}

impl ScanConnection for ArrowScanConnection {
    fn get_subscriber_data_scan(
        &self,
        byte2: [(u8, u8, u8, u8); 10],
    ) -> Vec<([bool; 10], [u8; 10], [u8; 10], u32, u32)> {
        self.db
            .subscriber
            .scan(byte2)
            .map(|row| self.db.subscriber.get_row_data(row))
            .collect()
    }

    fn update_subscriber_location_scan(&self, vlr_location: u32, byte2: [(u8, u8, u8, u8); 10]) {
        for row in self.db.subscriber.scan(byte2) {
            self.db.subscriber.update_row_location(row, vlr_location);
        }
    }
}
