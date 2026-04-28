use anyhow::{Context, anyhow, bail};
use flatbuffers::{ForwardsUOffset, Table, VOffsetT};

pub mod marketdata {
    pub mod v_1 {
        use super::super::*;

        #[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
        #[repr(transparent)]
        pub struct RecordPayload(pub u8);

        impl RecordPayload {
            pub const TRADE: Self = Self(1);
            pub const LIQUIDATION: Self = Self(2);
            pub const OPEN_INTEREST: Self = Self(3);
            pub const QUOTE: Self = Self(4);
        }

        #[derive(Clone, Copy)]
        pub struct ContractMeta<'a> {
            pub _tab: Table<'a>,
        }

        impl<'a> ContractMeta<'a> {
            pub const VT_CONTRACT_ORIGINAL_ID: VOffsetT = 4;
            pub const VT_HAS_CONTRACT_DELIVERY_TIME_UNIX_MS: VOffsetT = 6;
            pub const VT_CONTRACT_DELIVERY_TIME_UNIX_MS: VOffsetT = 8;
            pub const VT_CONTRACT_MARGIN: VOffsetT = 10;
            pub const VT_CONTRACT_DENOMINATOR: VOffsetT = 12;
            pub const VT_CONTRACT_TYPE: VOffsetT = 14;

            pub fn init_from_table(table: Table<'a>) -> Self {
                Self { _tab: table }
            }

            pub fn contract_original_id(&self) -> Option<&'a str> {
                unsafe {
                    self._tab
                        .get::<ForwardsUOffset<&str>>(Self::VT_CONTRACT_ORIGINAL_ID, None)
                }
            }

            pub fn has_contract_delivery_time_unix_ms(&self) -> bool {
                unsafe {
                    self._tab
                        .get::<bool>(Self::VT_HAS_CONTRACT_DELIVERY_TIME_UNIX_MS, Some(false))
                        .unwrap()
                }
            }

            pub fn contract_delivery_time_unix_ms(&self) -> i64 {
                unsafe {
                    self._tab
                        .get::<i64>(Self::VT_CONTRACT_DELIVERY_TIME_UNIX_MS, Some(0))
                        .unwrap()
                }
            }

            pub fn contract_margin(&self) -> Option<&'a str> {
                unsafe {
                    self._tab
                        .get::<ForwardsUOffset<&str>>(Self::VT_CONTRACT_MARGIN, None)
                }
            }

            pub fn contract_denominator(&self) -> Option<&'a str> {
                unsafe {
                    self._tab
                        .get::<ForwardsUOffset<&str>>(Self::VT_CONTRACT_DENOMINATOR, None)
                }
            }

            pub fn contract_type(&self) -> Option<&'a str> {
                unsafe {
                    self._tab
                        .get::<ForwardsUOffset<&str>>(Self::VT_CONTRACT_TYPE, None)
                }
            }
        }

        macro_rules! string_field {
            ($self:expr, $offset:expr) => {
                unsafe { $self._tab.get::<ForwardsUOffset<&str>>($offset, None) }
            };
        }

        macro_rules! scalar_field {
            ($self:expr, $ty:ty, $offset:expr, $default:expr) => {
                unsafe { $self._tab.get::<$ty>($offset, Some($default)).unwrap() }
            };
        }

        #[derive(Clone, Copy)]
        pub struct Trade<'a> {
            pub _tab: Table<'a>,
        }

        impl<'a> Trade<'a> {
            pub const VT_TIME_UNIX_MS: VOffsetT = 4;
            pub const VT_UID: VOffsetT = 6;
            pub const VT_EXCHANGE: VOffsetT = 8;
            pub const VT_CONTRACT_ID: VOffsetT = 10;
            pub const VT_PRICE: VOffsetT = 12;
            pub const VT_LOCAL_TIMESTAMP_UNIX_MS: VOffsetT = 14;
            pub const VT_SYMBOL: VOffsetT = 16;
            pub const VT_SIDE: VOffsetT = 18;
            pub const VT_QUANTITY: VOffsetT = 20;
            pub const VT_AMOUNT: VOffsetT = 22;
            pub const VT_CONTRACT_META: VOffsetT = 24;

            pub fn init_from_table(table: Table<'a>) -> Self {
                Self { _tab: table }
            }

            pub fn time_unix_ms(&self) -> i64 {
                scalar_field!(self, i64, Self::VT_TIME_UNIX_MS, 0)
            }
            pub fn uid(&self) -> Option<&'a str> {
                string_field!(self, Self::VT_UID)
            }
            pub fn exchange(&self) -> Option<&'a str> {
                string_field!(self, Self::VT_EXCHANGE)
            }
            pub fn contract_id(&self) -> Option<&'a str> {
                string_field!(self, Self::VT_CONTRACT_ID)
            }
            pub fn price(&self) -> Option<&'a str> {
                string_field!(self, Self::VT_PRICE)
            }
            pub fn local_timestamp_unix_ms(&self) -> i64 {
                scalar_field!(self, i64, Self::VT_LOCAL_TIMESTAMP_UNIX_MS, 0)
            }
            pub fn symbol(&self) -> Option<&'a str> {
                string_field!(self, Self::VT_SYMBOL)
            }
            pub fn side(&self) -> Option<&'a str> {
                string_field!(self, Self::VT_SIDE)
            }
            pub fn quantity(&self) -> f64 {
                scalar_field!(self, f64, Self::VT_QUANTITY, 0.0)
            }
            pub fn amount(&self) -> f64 {
                scalar_field!(self, f64, Self::VT_AMOUNT, 0.0)
            }
            pub fn contract_meta(&self) -> Option<ContractMeta<'a>> {
                unsafe {
                    self._tab
                        .get::<ForwardsUOffset<Table<'a>>>(Self::VT_CONTRACT_META, None)
                }
                .map(ContractMeta::init_from_table)
            }
        }

        #[derive(Clone, Copy)]
        pub struct Liquidation<'a> {
            pub _tab: Table<'a>,
        }

        impl<'a> Liquidation<'a> {
            pub const VT_TIME_UNIX_MS: VOffsetT = 4;
            pub const VT_EXCHANGE: VOffsetT = 6;
            pub const VT_CONTRACT_ID: VOffsetT = 8;
            pub const VT_PRICE: VOffsetT = 10;
            pub const VT_LOCAL_TIMESTAMP_UNIX_MS: VOffsetT = 12;
            pub const VT_SYMBOL: VOffsetT = 14;
            pub const VT_SIDE: VOffsetT = 16;
            pub const VT_QUANTITY: VOffsetT = 18;
            pub const VT_AMOUNT: VOffsetT = 20;
            pub const VT_CONTRACT_META: VOffsetT = 22;

            pub fn init_from_table(table: Table<'a>) -> Self {
                Self { _tab: table }
            }

            pub fn time_unix_ms(&self) -> i64 {
                scalar_field!(self, i64, Self::VT_TIME_UNIX_MS, 0)
            }
            pub fn exchange(&self) -> Option<&'a str> {
                string_field!(self, Self::VT_EXCHANGE)
            }
            pub fn contract_id(&self) -> Option<&'a str> {
                string_field!(self, Self::VT_CONTRACT_ID)
            }
            pub fn price(&self) -> Option<&'a str> {
                string_field!(self, Self::VT_PRICE)
            }
            pub fn local_timestamp_unix_ms(&self) -> i64 {
                scalar_field!(self, i64, Self::VT_LOCAL_TIMESTAMP_UNIX_MS, 0)
            }
            pub fn symbol(&self) -> Option<&'a str> {
                string_field!(self, Self::VT_SYMBOL)
            }
            pub fn side(&self) -> Option<&'a str> {
                string_field!(self, Self::VT_SIDE)
            }
            pub fn quantity(&self) -> f64 {
                scalar_field!(self, f64, Self::VT_QUANTITY, 0.0)
            }
            pub fn amount(&self) -> f64 {
                scalar_field!(self, f64, Self::VT_AMOUNT, 0.0)
            }
            pub fn contract_meta(&self) -> Option<ContractMeta<'a>> {
                unsafe {
                    self._tab
                        .get::<ForwardsUOffset<Table<'a>>>(Self::VT_CONTRACT_META, None)
                }
                .map(ContractMeta::init_from_table)
            }
        }

        #[derive(Clone, Copy)]
        pub struct OpenInterest<'a> {
            pub _tab: Table<'a>,
        }

        impl<'a> OpenInterest<'a> {
            pub const VT_TIME_UNIX_MS: VOffsetT = 4;
            pub const VT_CONTRACT_ID: VOffsetT = 6;
            pub const VT_EXCHANGE: VOffsetT = 8;
            pub const VT_LOCAL_TIMESTAMP_UNIX_MS: VOffsetT = 10;
            pub const VT_SYMBOL: VOffsetT = 12;
            pub const VT_OPEN_INTEREST: VOffsetT = 14;
            pub const VT_CONTRACT_META: VOffsetT = 16;

            pub fn init_from_table(table: Table<'a>) -> Self {
                Self { _tab: table }
            }

            pub fn time_unix_ms(&self) -> i64 {
                scalar_field!(self, i64, Self::VT_TIME_UNIX_MS, 0)
            }
            pub fn contract_id(&self) -> Option<&'a str> {
                string_field!(self, Self::VT_CONTRACT_ID)
            }
            pub fn exchange(&self) -> Option<&'a str> {
                string_field!(self, Self::VT_EXCHANGE)
            }
            pub fn local_timestamp_unix_ms(&self) -> i64 {
                scalar_field!(self, i64, Self::VT_LOCAL_TIMESTAMP_UNIX_MS, 0)
            }
            pub fn symbol(&self) -> Option<&'a str> {
                string_field!(self, Self::VT_SYMBOL)
            }
            pub fn open_interest(&self) -> f64 {
                scalar_field!(self, f64, Self::VT_OPEN_INTEREST, 0.0)
            }
            pub fn contract_meta(&self) -> Option<ContractMeta<'a>> {
                unsafe {
                    self._tab
                        .get::<ForwardsUOffset<Table<'a>>>(Self::VT_CONTRACT_META, None)
                }
                .map(ContractMeta::init_from_table)
            }
        }

        #[derive(Clone, Copy)]
        pub struct Quote<'a> {
            pub _tab: Table<'a>,
        }

        impl<'a> Quote<'a> {
            pub const VT_TIME_UNIX_MS: VOffsetT = 4;
            pub const VT_EXCHANGE: VOffsetT = 6;
            pub const VT_CONTRACT_ID: VOffsetT = 8;
            pub const VT_BID_PRICE: VOffsetT = 10;
            pub const VT_BID_QUANTITY: VOffsetT = 12;
            pub const VT_ASK_PRICE: VOffsetT = 14;
            pub const VT_ASK_QUANTITY: VOffsetT = 16;
            pub const VT_LOCAL_TIMESTAMP_UNIX_MS: VOffsetT = 18;
            pub const VT_SYMBOL: VOffsetT = 20;
            pub const VT_CONTRACT_META: VOffsetT = 22;

            pub fn init_from_table(table: Table<'a>) -> Self {
                Self { _tab: table }
            }

            pub fn time_unix_ms(&self) -> i64 {
                scalar_field!(self, i64, Self::VT_TIME_UNIX_MS, 0)
            }
            pub fn exchange(&self) -> Option<&'a str> {
                string_field!(self, Self::VT_EXCHANGE)
            }
            pub fn contract_id(&self) -> Option<&'a str> {
                string_field!(self, Self::VT_CONTRACT_ID)
            }
            pub fn bid_price(&self) -> Option<&'a str> {
                string_field!(self, Self::VT_BID_PRICE)
            }
            pub fn bid_quantity(&self) -> f64 {
                scalar_field!(self, f64, Self::VT_BID_QUANTITY, 0.0)
            }
            pub fn ask_price(&self) -> Option<&'a str> {
                string_field!(self, Self::VT_ASK_PRICE)
            }
            pub fn ask_quantity(&self) -> f64 {
                scalar_field!(self, f64, Self::VT_ASK_QUANTITY, 0.0)
            }
            pub fn local_timestamp_unix_ms(&self) -> i64 {
                scalar_field!(self, i64, Self::VT_LOCAL_TIMESTAMP_UNIX_MS, 0)
            }
            pub fn symbol(&self) -> Option<&'a str> {
                string_field!(self, Self::VT_SYMBOL)
            }
            pub fn contract_meta(&self) -> Option<ContractMeta<'a>> {
                unsafe {
                    self._tab
                        .get::<ForwardsUOffset<Table<'a>>>(Self::VT_CONTRACT_META, None)
                }
                .map(ContractMeta::init_from_table)
            }
        }

        #[derive(Clone, Copy)]
        pub struct RecordEnvelope<'a> {
            pub _tab: Table<'a>,
        }

        impl<'a> RecordEnvelope<'a> {
            pub const VT_PAYLOAD_TYPE: VOffsetT = 4;
            pub const VT_PAYLOAD: VOffsetT = 6;

            pub fn payload_type(&self) -> RecordPayload {
                RecordPayload(scalar_field!(self, u8, Self::VT_PAYLOAD_TYPE, 0))
            }

            fn payload(&self) -> Option<Table<'a>> {
                unsafe {
                    self._tab
                        .get::<ForwardsUOffset<Table<'a>>>(Self::VT_PAYLOAD, None)
                }
            }

            pub fn payload_as_trade(&self) -> Option<Trade<'a>> {
                (self.payload_type() == RecordPayload::TRADE)
                    .then_some(())
                    .and_then(|_| self.payload().map(Trade::init_from_table))
            }

            pub fn payload_as_liquidation(&self) -> Option<Liquidation<'a>> {
                (self.payload_type() == RecordPayload::LIQUIDATION)
                    .then_some(())
                    .and_then(|_| self.payload().map(Liquidation::init_from_table))
            }

            pub fn payload_as_open_interest(&self) -> Option<OpenInterest<'a>> {
                (self.payload_type() == RecordPayload::OPEN_INTEREST)
                    .then_some(())
                    .and_then(|_| self.payload().map(OpenInterest::init_from_table))
            }

            pub fn payload_as_quote(&self) -> Option<Quote<'a>> {
                (self.payload_type() == RecordPayload::QUOTE)
                    .then_some(())
                    .and_then(|_| self.payload().map(Quote::init_from_table))
            }
        }

        pub const RECORD_ENVELOPE_IDENTIFIER: &str = "MDV1";

        pub fn record_envelope_buffer_has_identifier(buf: &[u8]) -> bool {
            flatbuffers::buffer_has_identifier(buf, RECORD_ENVELOPE_IDENTIFIER, false)
        }

        pub fn root_as_record_envelope(buf: &[u8]) -> anyhow::Result<RecordEnvelope<'_>> {
            if !record_envelope_buffer_has_identifier(buf) {
                bail!(
                    "buffer is missing live-ingestion file identifier {}",
                    RECORD_ENVELOPE_IDENTIFIER
                );
            }
            let bytes = buf
                .get(0..4)
                .ok_or_else(|| anyhow!("live-ingestion envelope is too short"))?;
            let offset = u32::from_le_bytes(bytes.try_into().unwrap()) as usize;
            if offset >= buf.len() {
                bail!(
                    "live-ingestion envelope root offset {} is outside buffer length {}",
                    offset,
                    buf.len()
                );
            }
            let table = unsafe { Table::new(buf, offset) };
            let payload_type_loc = offset + usize::from(RecordEnvelope::VT_PAYLOAD_TYPE);
            buf.get(payload_type_loc)
                .context("live-ingestion envelope payload_type is out of bounds")?;
            Ok(RecordEnvelope { _tab: table })
        }
    }
}
