use crate::tatp::{TATPConfig, TATPServer};
use crate::Server;
use arrow::array::{
    ArrayBuilder, BooleanArray, BooleanBuilder, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
    PrimitiveArrayOps, UInt32Array, UInt32Builder, UInt8Array, UInt8Builder,
};
use fnv::FnvHashMap;
use rand::seq::SliceRandom;
use rand::Rng;
use std::collections::hash_map::Entry;
use std::convert::TryInto;
use std::sync::Mutex;

struct Subscriber {
    col_s_id: UInt32Array,
    col_bit: Vec<BooleanArray>,
    col_hex: Vec<UInt8Array>,
    col_byte2: Vec<UInt8Array>,
    col_msc_location: UInt32Array,
    col_vlr_location: UInt32Array,
    index: FnvHashMap<u32, usize>,
}

impl Subscriber {
    fn new(config: &TATPConfig) -> Subscriber {
        let mut rng = rand::thread_rng();

        let mut s_ids = (1..=config.get_num_rows()).collect::<Vec<_>>();
        s_ids.shuffle(&mut rng);

        let mut s_id_builder = UInt32Builder::new(s_ids.len());
        let mut bit_builders = (0..10)
            .map(|_| BooleanBuilder::new(s_ids.len()))
            .collect::<Vec<_>>();
        let mut hex_builders = (0..10)
            .map(|_| UInt8Builder::new(s_ids.len()))
            .collect::<Vec<_>>();
        let mut byte2_builders = (0..10)
            .map(|_| UInt8Builder::new(s_ids.len()))
            .collect::<Vec<_>>();
        let mut msc_location_builder = UInt32Builder::new(s_ids.len());
        let mut vlr_location_builder = UInt32Builder::new(s_ids.len());

        let mut index = FnvHashMap::default();

        for (row, s_id) in s_ids.iter().enumerate() {
            s_id_builder.append_value(*s_id).unwrap();

            for bit_builder in &mut bit_builders {
                bit_builder.append_value(rng.gen()).unwrap();
            }

            for hex_builder in &mut hex_builders {
                hex_builder.append_value(rng.gen_range(0, 16)).unwrap();
            }

            for byte2_builder in &mut byte2_builders {
                byte2_builder.append_value(rng.gen()).unwrap();
            }

            msc_location_builder
                .append_value(rng.gen_range(1, u32::max_value()))
                .unwrap();

            vlr_location_builder
                .append_value(rng.gen_range(1, u32::max_value()))
                .unwrap();

            index.insert(*s_id, row);
        }

        Subscriber {
            col_s_id: s_id_builder.finish(),
            col_bit: bit_builders.into_iter().map(|mut b| b.finish()).collect(),
            col_hex: hex_builders.into_iter().map(|mut b| b.finish()).collect(),
            col_byte2: byte2_builders.into_iter().map(|mut b| b.finish()).collect(),
            col_msc_location: msc_location_builder.finish(),
            col_vlr_location: vlr_location_builder.finish(),
            index,
        }
    }

    fn get_row_data(&self, row: usize) -> ([bool; 10], [u8; 10], [u8; 10], u32, u32) {
        let mut bit = [false; 10];
        for (dst, src) in bit.iter_mut().zip(&self.col_bit) {
            *dst = src.value(row);
        }

        let mut hex = [0; 10];
        for (dst, src) in hex.iter_mut().zip(&self.col_hex) {
            *dst = src.value(row);
        }

        let mut byte2 = [0; 10];
        for (dst, src) in byte2.iter_mut().zip(&self.col_byte2) {
            *dst = src.value(row);
        }

        (
            bit,
            hex,
            byte2,
            self.col_msc_location.value(row),
            self.col_vlr_location.value(row),
        )
    }

    fn update_row_bit(&self, row: usize, bit_1: bool) {
        unsafe {
            let bit_1_dst = self.col_bit[0]
                .values()
                .raw_data()
                .offset((row / 8) as isize) as *mut u8;

            if bit_1 {
                *bit_1_dst |= 1 << (row % 8);
            } else {
                *bit_1_dst &= !(1 << (row % 8));
            }
        }
    }

    fn update_row_location(&self, row: usize, vlr_location: u32) {
        unsafe {
            let vlr_location_dst =
                self.col_vlr_location.raw_values().offset(row as isize) as *mut u32;

            *vlr_location_dst = vlr_location;
        }
    }

    fn scan(&self, byte2: [(u8, u8, u8, u8); 10]) -> impl Iterator<Item = usize> + '_ {
        (0..self.col_s_id.len()).filter(move |&row| {
            self.col_byte2
                .iter()
                .zip(&byte2)
                .all(|(col_byte2, &(a, b, c, d))| {
                    let value = col_byte2.value(row);
                    (value >= a && value <= b) || (value >= c && value <= d)
                })
        })
    }
}

struct AccessInfo {
    _col_s_id: UInt32Array,
    _col_ai_type: UInt8Array,
    col_data1: UInt8Array,
    col_data2: UInt8Array,
    col_data3: FixedSizeBinaryArray,
    col_data4: FixedSizeBinaryArray,
    index: FnvHashMap<(u32, u8), usize>,
}

impl AccessInfo {
    fn new(subscriber: &Subscriber) -> AccessInfo {
        let mut rng = rand::thread_rng();

        let capacity = subscriber.col_s_id.len() * 4;

        let mut s_id_builder = UInt32Builder::new(capacity);
        let mut ai_type_builder = UInt8Builder::new(capacity);
        let mut data1_builder = UInt8Builder::new(capacity);
        let mut data2_builder = UInt8Builder::new(capacity);
        let mut data3_builder = FixedSizeBinaryBuilder::new(capacity, 3);
        let mut data4_builder = FixedSizeBinaryBuilder::new(capacity, 5);
        let mut index = FnvHashMap::default();

        for s_id in &subscriber.col_s_id {
            let num_ai_types = rng.gen_range(1, 5);
            for ai_type in [1, 2, 3, 4].choose_multiple(&mut rng, num_ai_types) {
                s_id_builder.append_value(s_id.unwrap()).unwrap();
                ai_type_builder.append_value(*ai_type).unwrap();
                data1_builder.append_value(rng.gen()).unwrap();
                data2_builder.append_value(rng.gen()).unwrap();
                data3_builder
                    .append_value(&(0..3).map(|_| rng.gen()).collect::<Vec<_>>())
                    .unwrap();
                data4_builder
                    .append_value(&(0..5).map(|_| rng.gen()).collect::<Vec<_>>())
                    .unwrap();
                index.insert((s_id.unwrap(), *ai_type), s_id_builder.len() - 1);
            }
        }

        AccessInfo {
            _col_s_id: s_id_builder.finish(),
            _col_ai_type: ai_type_builder.finish(),
            col_data1: data1_builder.finish(),
            col_data2: data2_builder.finish(),
            col_data3: data3_builder.finish(),
            col_data4: data4_builder.finish(),
            index,
        }
    }
}

struct SpecialFacility {
    col_s_id: UInt32Array,
    col_sf_type: UInt8Array,
    col_is_active: BooleanArray,
    _col_error_cntrl: UInt8Array,
    col_data_a: UInt8Array,
    _col_data_b: FixedSizeBinaryArray,
    index: FnvHashMap<u32, FnvHashMap<u8, usize>>,
}

impl SpecialFacility {
    fn new(subscriber: &Subscriber) -> SpecialFacility {
        let mut rng = rand::thread_rng();

        let capacity = subscriber.col_s_id.len() * 4;

        let mut s_id_builder = UInt32Builder::new(capacity);
        let mut sf_type_builder = UInt8Builder::new(capacity);
        let mut is_active_builder = BooleanBuilder::new(capacity);
        let mut error_cntrl_builder = UInt8Builder::new(capacity);
        let mut data_a_builder = UInt8Builder::new(capacity);
        let mut data_b_builder = FixedSizeBinaryBuilder::new(capacity, 5);
        let mut index = FnvHashMap::default();

        for s_id in &subscriber.col_s_id {
            let num_sf_types = rng.gen_range(1, 5);
            let mut sub_index = FnvHashMap::default();
            for sf_type in [1, 2, 3, 4].choose_multiple(&mut rng, num_sf_types) {
                s_id_builder.append_value(s_id.unwrap()).unwrap();
                sf_type_builder.append_value(*sf_type).unwrap();
                is_active_builder.append_value(rng.gen_bool(0.85)).unwrap();
                error_cntrl_builder.append_value(rng.gen()).unwrap();
                data_a_builder.append_value(rng.gen()).unwrap();
                data_b_builder
                    .append_value(&(0..5).map(|_| rng.gen()).collect::<Vec<_>>())
                    .unwrap();
                sub_index.insert(*sf_type, s_id_builder.len() - 1);
            }

            index.insert(s_id.unwrap(), sub_index);
        }

        SpecialFacility {
            col_s_id: s_id_builder.finish(),
            col_sf_type: sf_type_builder.finish(),
            col_is_active: is_active_builder.finish(),
            _col_error_cntrl: error_cntrl_builder.finish(),
            col_data_a: data_a_builder.finish(),
            _col_data_b: data_b_builder.finish(),
            index,
        }
    }
}

struct CallForwarding {
    s_id: UInt32Array,
    sf_type: UInt8Array,
    start_time: UInt8Array,
    end_time: UInt8Array,
    numberx: FixedSizeBinaryArray,
    index: Vec<Mutex<FnvHashMap<(u32, u8), FnvHashMap<u8, usize>>>>,
    free: Mutex<Vec<usize>>,
}

impl CallForwarding {
    fn new(special_facility: &SpecialFacility) -> CallForwarding {
        let mut rng = rand::thread_rng();

        let num_free_rows = special_facility.col_s_id.len() * 3;

        let capacity = 2 * num_free_rows;

        let mut s_id_builder = UInt32Builder::new(capacity);
        let mut sf_type_builder = UInt8Builder::new(capacity);
        let mut start_time_builder = UInt8Builder::new(capacity);
        let mut end_time_builder = UInt8Builder::new(capacity);
        let mut numberx_builder = FixedSizeBinaryBuilder::new(capacity, 15);
        let index = (0..100)
            .map(|_| Mutex::new(FnvHashMap::default()))
            .collect::<Vec<_>>();
        let free = Mutex::new(vec![]);

        for (s_id, sf_type) in special_facility
            .col_s_id
            .iter()
            .zip(&special_facility.col_sf_type)
        {
            let num_start_times = rng.gen_range(0, 4);
            let mut sub_index = FnvHashMap::default();
            for start_time in [0, 8, 16].choose_multiple(&mut rng, num_start_times) {
                s_id_builder.append_value(s_id.unwrap()).unwrap();
                sf_type_builder.append_value(sf_type.unwrap()).unwrap();
                start_time_builder.append_value(*start_time).unwrap();
                end_time_builder
                    .append_value(start_time + rng.gen_range(1, 9))
                    .unwrap();
                numberx_builder
                    .append_value(&(0..15).map(|_| rng.gen()).collect::<Vec<_>>())
                    .unwrap();
                sub_index.insert(sf_type.unwrap(), s_id_builder.len() - 1);
            }

            index[s_id.unwrap() as usize % index.len()]
                .lock()
                .unwrap()
                .insert((s_id.unwrap(), sf_type.unwrap()), sub_index);
        }

        for _ in 0..num_free_rows {
            s_id_builder.append_value(0).unwrap();
            sf_type_builder.append_value(0).unwrap();
            start_time_builder.append_value(0).unwrap();
            end_time_builder.append_value(0).unwrap();
            numberx_builder
                .append_value(&(0..15).map(|_| rng.gen()).collect::<Vec<_>>())
                .unwrap();
            free.lock().unwrap().push(s_id_builder.len() - 1);
        }

        CallForwarding {
            s_id: s_id_builder.finish(),
            sf_type: sf_type_builder.finish(),
            start_time: start_time_builder.finish(),
            end_time: end_time_builder.finish(),
            numberx: numberx_builder.finish(),
            index,
            free,
        }
    }
}

impl CallForwarding {
    fn get_index_partition(
        &self,
        s_id: u32,
    ) -> &Mutex<FnvHashMap<(u32, u8), FnvHashMap<u8, usize>>> {
        &self.index[s_id as usize % self.index.len()]
    }
}

pub struct ArrowTATPServer {
    subscriber: Subscriber,
    access_info: AccessInfo,
    special_facility: SpecialFacility,
    call_forwarding: CallForwarding,
}

impl ArrowTATPServer {
    pub fn new(config: &TATPConfig) -> ArrowTATPServer {
        let subscriber = Subscriber::new(config);
        let access_info = AccessInfo::new(&subscriber);
        let special_facility = SpecialFacility::new(&subscriber);
        let call_forwarding = CallForwarding::new(&special_facility);

        ArrowTATPServer {
            subscriber,
            access_info,
            special_facility,
            call_forwarding,
        }
    }
}

impl Server for ArrowTATPServer {
    fn begin_transaction(&self) {}

    fn commit_transaction(&self) {}
}

impl TATPServer for ArrowTATPServer {
    fn get_subscriber_data(&self, s_id: u32) -> ([bool; 10], [u8; 10], [u8; 10], u32, u32) {
        self.subscriber.get_row_data(self.subscriber.index[&s_id])
    }

    fn get_new_destination(
        &self,
        s_id: u32,
        sf_type: u8,
        start_time: u8,
        end_time: u8,
    ) -> Vec<[u8; 15]> {
        let mut result = vec![];

        if let Some(sf_row) = self
            .special_facility
            .index
            .get(&s_id)
            .and_then(|m| m.get(&sf_type))
        {
            if self.special_facility.col_is_active.value(*sf_row) {
                if let Some(cf_rows) = self
                    .call_forwarding
                    .get_index_partition(s_id)
                    .lock()
                    .unwrap()
                    .get(&(s_id, sf_type))
                {
                    for (cf_start_time, cf_row) in cf_rows {
                        if *cf_start_time <= start_time
                            && end_time < self.call_forwarding.end_time.value(*cf_row)
                        {
                            result.push(
                                self.call_forwarding
                                    .numberx
                                    .value(*cf_row)
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                    }
                }
            }
        }

        result
    }

    fn get_access_data(&self, s_id: u32, ai_type: u8) -> Option<(u8, u8, [u8; 3], [u8; 5])> {
        self.access_info.index.get(&(s_id, ai_type)).map(|row| {
            (
                self.access_info.col_data1.value(*row),
                self.access_info.col_data2.value(*row),
                self.access_info.col_data3.value(*row).try_into().unwrap(),
                self.access_info.col_data4.value(*row).try_into().unwrap(),
            )
        })
    }

    fn update_subscriber_bit(&self, bit_1: bool, s_id: u32) {
        self.subscriber
            .update_row_bit(self.subscriber.index[&s_id], bit_1);
    }

    fn update_special_facility_data(&self, data_a: u8, s_id: u32, sf_type: u8) {
        if let Some(row) = self
            .special_facility
            .index
            .get(&s_id)
            .and_then(|m| m.get(&sf_type))
        {
            unsafe {
                let data_a_dst = self
                    .special_facility
                    .col_data_a
                    .raw_values()
                    .offset(*row as isize) as *mut u8;

                *data_a_dst = data_a;
            }
        }
    }

    fn update_subscriber_location(&self, vlr_location: u32, s_id: u32) {
        self.subscriber
            .update_row_location(self.subscriber.index[&s_id], vlr_location);
    }

    fn get_special_facility_types(&self, s_id: u32) -> Vec<u8> {
        self.special_facility.index[&s_id]
            .iter()
            .map(|(&sf_type, _)| sf_type)
            .collect()
    }

    fn insert_call_forwarding(
        &self,
        s_id: u32,
        sf_type: u8,
        start_time: u8,
        end_time: u8,
        numberx: [u8; 15],
    ) {
        if let Entry::Vacant(entry) = self
            .call_forwarding
            .get_index_partition(s_id)
            .lock()
            .unwrap()
            .entry((s_id, sf_type))
            .or_insert(FnvHashMap::default())
            .entry(start_time)
        {
            let row = self.call_forwarding.free.lock().unwrap().pop().unwrap();
            entry.insert(row);

            unsafe {
                let s_id_dst =
                    self.call_forwarding.s_id.raw_values().offset(row as isize) as *mut u32;

                let sf_type_dst = self
                    .call_forwarding
                    .sf_type
                    .raw_values()
                    .offset(row as isize) as *mut u8;

                let start_time_dst = self
                    .call_forwarding
                    .start_time
                    .raw_values()
                    .offset(row as isize) as *mut u8;

                let end_time_dst = self
                    .call_forwarding
                    .end_time
                    .raw_values()
                    .offset(row as isize) as *mut u8;

                let numberx_dst = self.call_forwarding.numberx.value(row).as_ptr() as *mut u8;

                *s_id_dst = s_id;
                *sf_type_dst = sf_type;
                *start_time_dst = start_time;
                *end_time_dst = end_time;
                numberx_dst.copy_from(numberx.as_ptr(), numberx.len());
            }
        }
    }

    fn delete_call_forwarding(&self, s_id: u32, sf_type: u8, start_time: u8) {
        if let Entry::Occupied(entry) = self
            .call_forwarding
            .get_index_partition(s_id)
            .lock()
            .unwrap()
            .entry((s_id, sf_type))
            .or_insert(FnvHashMap::default())
            .entry(start_time)
        {
            self.call_forwarding
                .free
                .lock()
                .unwrap()
                .push(entry.remove());
        }
    }

    fn get_subscriber_data_scan(
        &self,
        byte2: [(u8, u8, u8, u8); 10],
    ) -> Vec<([bool; 10], [u8; 10], [u8; 10], u32, u32)> {
        self.subscriber
            .scan(byte2)
            .map(|row| self.subscriber.get_row_data(row))
            .collect()
    }

    fn update_subscriber_bit_scan(&self, bit_1: bool, byte2: [(u8, u8, u8, u8); 10]) {
        for row in self.subscriber.scan(byte2) {
            self.subscriber.update_row_bit(row, bit_1);
        }
    }

    fn update_subscriber_location_scan(&self, vlr_location: u32, byte2: [(u8, u8, u8, u8); 10]) {
        for row in self.subscriber.scan(byte2) {
            self.subscriber.update_row_location(row, vlr_location);
        }
    }
}
