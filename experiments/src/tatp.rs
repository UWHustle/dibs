use crate::Server;
use rand::rngs::ThreadRng;
use rand::Rng;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;



pub trait TATPServer {
    /// Get subscriber data by ID.
    /// ```sql
    /// SELECT *
    /// FROM subscriber
    /// WHERE s_id = ?;
    /// ```
    fn get_subscriber_data(&self, s_id: u32) -> ([bool; 10], [u8; 10], [u8; 10], u32, u32);

    /// Get new destination.
    /// ```sql
    /// SELECT cf.numberx
    /// FROM special_facility AS sf, call_forwarding AS cf
    /// WHERE
    ///     (sf.s_id = ?
    ///         AND sf.sf_type = ?
    ///         AND sf.is_active = 1)
    ///     AND (cf.s_id = sf.s_id
    ///         AND cf.sf_type = sf.sf_type)
    ///     AND (cf.start_time <= ?
    ///         AND ? < cf.end_time);
    /// ```
    fn get_new_destination(
        &self,
        s_id: u32,
        sf_type: u8,
        start_time: u8,
        end_time: u8,
    ) -> Vec<[u8; 15]>;

    /// Get access data.
    /// ```sql
    /// SELECT data1, data2, data3, data4
    /// FROM access_info
    /// WHERE s_id = ? AND ai_type = ?;
    /// ```
    fn get_access_data(&self, s_id: u32, ai_type: u8) -> Option<(u8, u8, [u8; 3], [u8; 5])>;

    /// Update subscriber bit.
    /// ```sql
    /// UPDATE subscriber
    /// SET bit_1 = ?
    /// WHERE s_id = ?;
    /// ```
    fn update_subscriber_bit(&self, bit_1: bool, s_id: u32);

    /// Update special facility data.
    /// ```sql
    /// UPDATE special_facility
    /// SET data_a = ?
    /// WHERE s_id = ? AND sf_type = ?
    fn update_special_facility_data(&self, data_a: u8, s_id: u32, sf_type: u8);

    /// Update subscriber location.
    /// ```sql
    /// UPDATE subscriber
    /// SET vlr_location = ?
    /// WHERE s_id = ?;
    /// ```
    fn update_subscriber_location(&self, vlr_location: u32, s_id: u32);

    /// Get special facility types.
    /// ```sql
    /// SELECT sf_type
    /// FROM special_facility
    /// WHERE s_id = ?;
    /// ```
    fn get_special_facility_types(&self, s_id: u32) -> Vec<u8>;

    /// Insert call forwarding.
    /// ```sql
    /// INSERT INTO call_forwarding
    /// VALUES (?, ?, ?, ?, ?);
    /// ```
    fn insert_call_forwarding(
        &self,
        s_id: u32,
        sf_type: u8,
        start_time: u8,
        end_time: u8,
        numberx: [u8; 15],
    );

    /// Delete call forwarding.
    /// ```sql
    /// DELETE FROM call_forwarding
    /// WHERE s_id = ? AND sf_type = ? AND start_time = ?;
    /// ```
    fn delete_call_forwarding(&self, s_id: u32, sf_type: u8, start_time: u8);
}

#[derive(Clone)]
pub struct TATPConfig {
    num_rows: u32,
    a_val: u32,
}

impl TATPConfig {
    pub fn new(num_rows: u32) -> TATPConfig {
        let a_val = if num_rows <= 1000000 {
            65535
        } else if num_rows <= 10000000 {
            1048575
        } else {
            2097151
        };

        TATPConfig { num_rows, a_val }
    }

    pub fn get_num_rows(&self) -> u32 {
        self.num_rows
    }

    fn gen_s_id(&self, rng: &mut ThreadRng) -> u32 {
        (rng.gen_range(0, self.a_val + 1) | rng.gen_range(1, self.num_rows + 1)) % self.num_rows + 1
    }

    fn gen_numberx(&self, rng: &mut ThreadRng) -> [u8; 15] {
        let mut numberx = [0; 15];
        let s = rng.gen_range(1, self.num_rows + 1).to_string();
        numberx[(15 - s.len())..].copy_from_slice(s.as_bytes());
        numberx
    }
}

pub struct TATPClient<S> {
    config: TATPConfig,
    server: Arc<S>,
    transaction_counter: Arc<AtomicUsize>,
    terminate: Arc<AtomicBool>,
}

impl<S> TATPClient<S> {
    pub fn new(
        config: TATPConfig,
        server: Arc<S>,
        transaction_counter: Arc<AtomicUsize>,
        terminate: Arc<AtomicBool>,
    ) -> TATPClient<S> {
        TATPClient {
            config,
            server,
            transaction_counter,
            terminate,
        }
    }
}

impl<S> TATPClient<S>
where
    S: Server + TATPServer,
{
    pub fn run(&self) {
        let mut rng = rand::thread_rng();

        while !self.terminate.load(Ordering::Relaxed) {
            let transaction_type = rng.gen_range(0, 100);
            let s_id = self.config.gen_s_id(&mut rng);

            if transaction_type < 35 {
                // GET_SUBSCRIBER_DATA
                // Probability: 35%
                test::black_box(self.server.get_subscriber_data(s_id));
            } else if transaction_type < 45 {
                // GET_NEW_DESTINATION
                // Probability: 10%
                let sf_type = rng.gen_range(1, 5);
                let start_time = rng.gen_range(0, 3) * 8;
                let end_time = rng.gen_range(1, 25);
                self.server
                    .get_new_destination(s_id, sf_type, start_time, end_time);
            } else if transaction_type < 80 {
                // GET_ACCESS_DATA
                // Probability: 35%
                let ai_type = rng.gen_range(1, 5);
                self.server.get_access_data(s_id, ai_type);
            } else if transaction_type < 82 {
                // UPDATE_SUBSCRIBER_DATA
                // Probability: 2%
                let bit_1 = rng.gen();
                let data_a = rng.gen();
                let sf_type = rng.gen_range(1, 5);
                self.server.update_subscriber_bit(bit_1, s_id);
                self.server
                    .update_special_facility_data(data_a, s_id, sf_type);
            } else if transaction_type < 96 {
                // UPDATE_LOCATION
                // Probability: 14%
                let vlr_location = rng.gen();
                self.server.update_subscriber_location(vlr_location, s_id);
            } else if transaction_type < 98 {
                // INSERT_CALL_FORWARDING
                // Probability: 2%
                let sf_type = rng.gen_range(1, 5);
                let start_time = rng.gen_range(0, 3) * 8;
                let end_time = rng.gen_range(1, 25);
                let numberx = self.config.gen_numberx(&mut rng);
                self.server.get_special_facility_types(s_id);
                self.server
                    .insert_call_forwarding(s_id, sf_type, start_time, end_time, numberx);
            } else {
                // DELETE_CALL_FORWARDING
                // Probability: 2%
                let sf_type = rng.gen_range(1, 5);
                let start_time = rng.gen_range(0, 3) * 8;
                self.server
                    .delete_call_forwarding(s_id, sf_type, start_time);
            }

            self.transaction_counter.fetch_add(1, Ordering::Relaxed);
        }
    }
}
