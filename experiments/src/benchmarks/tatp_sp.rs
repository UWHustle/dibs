use crate::benchmarks::tatp::{TATPGenerator, TATPProcedure};
use crate::{AccessType, Generator, Procedure};
use dibs::{AcquireError, Dibs, Transaction};
use std::sync::Arc;

pub trait TATPSPConnection {
    fn get_subscriber_data(&mut self, s_id: u32) -> ([bool; 10], [u8; 10], [u8; 10], u32, u32);

    fn get_new_destination(
        &mut self,
        s_id: u32,
        sf_type: u8,
        start_time: u8,
        end_time: u8,
    ) -> Vec<String>;

    fn get_access_data(&mut self, s_id: u32, ai_type: u8) -> Option<(u8, u8, String, String)>;

    fn update_subscriber_data(&mut self, bit_1: bool, s_id: u32, data_a: u8, sf_type: u8);

    fn update_location(&mut self, vlr_location: u32, s_id: u32);

    fn insert_call_forwarding(
        &mut self,
        s_id: u32,
        sf_type: u8,
        start_time: u8,
        end_time: u8,
        numberx: &str,
    );

    fn delete_call_forwarding(&mut self, s_id: u32, sf_type: u8, start_time: u8);
}

pub struct TATPSPProcedure(TATPProcedure);

impl AccessType for TATPSPProcedure {
    fn is_read_only(&self) -> bool {
        self.0.is_read_only()
    }
}

impl<C> Procedure<C> for TATPSPProcedure
where
    C: TATPSPConnection,
{
    fn execute(
        &self,
        dibs: &Option<Arc<Dibs>>,
        _transaction: &mut Transaction,
        connection: &mut C,
    ) -> Result<(), AcquireError> {
        assert!(dibs.is_none());
        match &self.0 {
            &TATPProcedure::GetSubscriberData { s_id } => {
                connection.get_subscriber_data(s_id);
            }

            &TATPProcedure::GetNewDestination {
                s_id,
                sf_type,
                start_time,
                end_time,
            } => {
                connection.get_new_destination(s_id, sf_type, start_time, end_time);
            }

            &TATPProcedure::GetAccessData { s_id, ai_type } => {
                connection.get_access_data(s_id, ai_type);
            }

            &TATPProcedure::UpdateSubscriberData {
                bit_1,
                s_id,
                data_a,
                sf_type,
            } => {
                connection.update_subscriber_data(bit_1, s_id, data_a, sf_type);
            }

            &TATPProcedure::UpdateLocation { vlr_location, s_id } => {
                connection.update_location(vlr_location, s_id);
            }

            TATPProcedure::InsertCallForwarding {
                s_id,
                sf_type,
                start_time,
                end_time,
                numberx,
            } => {
                connection.insert_call_forwarding(*s_id, *sf_type, *start_time, *end_time, numberx)
            }

            &TATPProcedure::DeleteCallForwarding {
                s_id,
                sf_type,
                start_time,
            } => connection.delete_call_forwarding(s_id, sf_type, start_time),
        }

        Ok(())
    }
}

pub struct TATPSPGenerator(TATPGenerator);

impl TATPSPGenerator {
    pub fn new(num_rows: u32) -> TATPSPGenerator {
        TATPSPGenerator(TATPGenerator::new(num_rows))
    }
}

impl Generator for TATPSPGenerator {
    type Item = TATPSPProcedure;

    fn next(&self) -> TATPSPProcedure {
        TATPSPProcedure(self.0.next())
    }
}
