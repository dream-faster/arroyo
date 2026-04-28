use std::sync::Arc;

use anyhow::{Context, anyhow};
use arrow::array::{ArrayRef, Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::time::{SystemTime, UNIX_EPOCH};

use super::live_ingestion_marketdata_generated::marketdata::v_1 as marketdata;

pub(super) fn decode_message(
    msg: &[u8],
    timestamp: SystemTime,
) -> anyhow::Result<Option<RecordBatch>> {
    if !marketdata::record_envelope_buffer_has_identifier(msg) {
        return Ok(None);
    }

    let envelope = marketdata::root_as_record_envelope(msg)
        .context("decode live-ingestion record envelope")?;

    let batch = match envelope.payload_type() {
        marketdata::RecordPayload::TRADE => decode_trade(
            envelope
                .payload_as_trade()
                .ok_or_else(|| anyhow!("trade payload missing from live-ingestion envelope"))?,
            timestamp,
        )?,
        marketdata::RecordPayload::LIQUIDATION => decode_liquidation(
            envelope.payload_as_liquidation().ok_or_else(|| {
                anyhow!("liquidation payload missing from live-ingestion envelope")
            })?,
            timestamp,
        )?,
        marketdata::RecordPayload::OPEN_INTEREST => decode_open_interest(
            envelope.payload_as_open_interest().ok_or_else(|| {
                anyhow!("open interest payload missing from live-ingestion envelope")
            })?,
            timestamp,
        )?,
        marketdata::RecordPayload::QUOTE => decode_quote(
            envelope
                .payload_as_quote()
                .ok_or_else(|| anyhow!("quote payload missing from live-ingestion envelope"))?,
            timestamp,
        )?,
        other => {
            return Err(anyhow!(
                "unsupported live-ingestion payload type {:?}",
                other
            ));
        }
    };

    Ok(Some(batch))
}

#[derive(Default)]
struct ContractMetaValues {
    original_id: Option<String>,
    delivery_time: Option<i64>,
    margin: Option<String>,
    denominator: Option<String>,
    contract_type: Option<String>,
}

fn decode_trade(
    trade: marketdata::Trade<'_>,
    timestamp: SystemTime,
) -> anyhow::Result<RecordBatch> {
    let meta = contract_meta_values(trade.contract_meta())?;
    record_batch(
        trade_schema(),
        vec![
            ts_array(Some(ns_from_ms(trade.time_unix_ms())?)),
            string_array(Some(required_string(trade.uid(), "trade.uid")?)),
            string_array(Some(required_string(trade.exchange(), "trade.exchange")?)),
            string_array(Some(required_string(
                trade.contract_id(),
                "trade.contract_id",
            )?)),
            string_array(Some(required_string(trade.price(), "trade.price")?)),
            ts_array(Some(ns_from_ms(trade.local_timestamp_unix_ms())?)),
            string_array(Some(required_string(trade.symbol(), "trade.symbol")?)),
            string_array(Some(required_string(trade.side(), "trade.side")?)),
            f64_array(Some(trade.quantity())),
            f64_array(Some(trade.amount())),
            string_array(meta.original_id),
            ts_array(meta.delivery_time),
            string_array(meta.margin),
            string_array(meta.denominator),
            string_array(meta.contract_type),
            ts_array(Some(ns_from_system_time(timestamp)?)),
        ],
    )
}

fn decode_liquidation(
    liquidation: marketdata::Liquidation<'_>,
    timestamp: SystemTime,
) -> anyhow::Result<RecordBatch> {
    let meta = contract_meta_values(liquidation.contract_meta())?;
    record_batch(
        liquidation_schema(),
        vec![
            ts_array(Some(ns_from_ms(liquidation.time_unix_ms())?)),
            string_array(Some(required_string(
                liquidation.exchange(),
                "liquidation.exchange",
            )?)),
            string_array(Some(required_string(
                liquidation.contract_id(),
                "liquidation.contract_id",
            )?)),
            string_array(Some(required_string(
                liquidation.price(),
                "liquidation.price",
            )?)),
            ts_array(Some(ns_from_ms(liquidation.local_timestamp_unix_ms())?)),
            string_array(Some(required_string(
                liquidation.symbol(),
                "liquidation.symbol",
            )?)),
            string_array(Some(required_string(
                liquidation.side(),
                "liquidation.side",
            )?)),
            f64_array(Some(liquidation.quantity())),
            f64_array(Some(liquidation.amount())),
            string_array(meta.original_id),
            ts_array(meta.delivery_time),
            string_array(meta.margin),
            string_array(meta.denominator),
            string_array(meta.contract_type),
            ts_array(Some(ns_from_system_time(timestamp)?)),
        ],
    )
}

fn decode_open_interest(
    open_interest: marketdata::OpenInterest<'_>,
    timestamp: SystemTime,
) -> anyhow::Result<RecordBatch> {
    let meta = contract_meta_values(open_interest.contract_meta())?;
    record_batch(
        open_interest_schema(),
        vec![
            ts_array(Some(ns_from_ms(open_interest.time_unix_ms())?)),
            string_array(Some(required_string(
                open_interest.contract_id(),
                "open_interest.contract_id",
            )?)),
            string_array(Some(required_string(
                open_interest.exchange(),
                "open_interest.exchange",
            )?)),
            ts_array(Some(ns_from_ms(open_interest.local_timestamp_unix_ms())?)),
            string_array(Some(required_string(
                open_interest.symbol(),
                "open_interest.symbol",
            )?)),
            f64_array(Some(open_interest.open_interest())),
            string_array(meta.original_id),
            ts_array(meta.delivery_time),
            string_array(meta.margin),
            string_array(meta.denominator),
            string_array(meta.contract_type),
            ts_array(Some(ns_from_system_time(timestamp)?)),
        ],
    )
}

fn decode_quote(
    quote: marketdata::Quote<'_>,
    timestamp: SystemTime,
) -> anyhow::Result<RecordBatch> {
    let meta = contract_meta_values(quote.contract_meta())?;
    record_batch(
        quote_schema(),
        vec![
            ts_array(Some(ns_from_ms(quote.time_unix_ms())?)),
            string_array(Some(required_string(quote.exchange(), "quote.exchange")?)),
            string_array(Some(required_string(
                quote.contract_id(),
                "quote.contract_id",
            )?)),
            string_array(Some(required_string(quote.bid_price(), "quote.bid_price")?)),
            f64_array(Some(quote.bid_quantity())),
            string_array(Some(required_string(quote.ask_price(), "quote.ask_price")?)),
            f64_array(Some(quote.ask_quantity())),
            ts_array(Some(ns_from_ms(quote.local_timestamp_unix_ms())?)),
            string_array(Some(required_string(quote.symbol(), "quote.symbol")?)),
            string_array(meta.original_id),
            ts_array(meta.delivery_time),
            string_array(meta.margin),
            string_array(meta.denominator),
            string_array(meta.contract_type),
            ts_array(Some(ns_from_system_time(timestamp)?)),
        ],
    )
}

fn contract_meta_values(
    meta: Option<marketdata::ContractMeta<'_>>,
) -> anyhow::Result<ContractMetaValues> {
    let Some(meta) = meta else {
        return Ok(ContractMetaValues::default());
    };

    Ok(ContractMetaValues {
        original_id: meta.contract_original_id().map(ToOwned::to_owned),
        delivery_time: if meta.has_contract_delivery_time_unix_ms() {
            Some(ns_from_ms(meta.contract_delivery_time_unix_ms())?)
        } else {
            None
        },
        margin: meta.contract_margin().map(ToOwned::to_owned),
        denominator: meta.contract_denominator().map(ToOwned::to_owned),
        contract_type: meta.contract_type().map(ToOwned::to_owned),
    })
}

fn required_string(value: Option<&str>, field: &str) -> anyhow::Result<String> {
    value
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("missing required live-ingestion field {}", field))
}

fn ns_from_ms(value: i64) -> anyhow::Result<i64> {
    value
        .checked_mul(1_000_000)
        .ok_or_else(|| anyhow!("timestamp {}ms overflows nanoseconds", value))
}

fn ns_from_system_time(value: SystemTime) -> anyhow::Result<i64> {
    let duration = value
        .duration_since(UNIX_EPOCH)
        .map_err(|e| anyhow!("timestamp before unix epoch: {}", e))?;
    i64::try_from(duration.as_nanos()).map_err(|_| anyhow!("system time overflows nanoseconds"))
}

fn record_batch(schema: SchemaRef, columns: Vec<ArrayRef>) -> anyhow::Result<RecordBatch> {
    RecordBatch::try_new(schema, columns).map_err(Into::into)
}

fn string_array(value: Option<String>) -> ArrayRef {
    Arc::new(StringArray::from(vec![value]))
}

fn ts_array(value: Option<i64>) -> ArrayRef {
    Arc::new(TimestampNanosecondArray::from(vec![value]))
}

fn f64_array(value: Option<f64>) -> ArrayRef {
    Arc::new(Float64Array::from(vec![value]))
}

fn timestamp_field(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        nullable,
    )
}

fn text_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Utf8, nullable)
}

fn float_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Float64, nullable)
}

fn trade_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        timestamp_field("time", true),
        text_field("uid", true),
        text_field("exchange", true),
        text_field("contract_id", true),
        text_field("price", true),
        timestamp_field("local_timestamp", true),
        text_field("symbol", true),
        text_field("side", true),
        float_field("quantity", true),
        float_field("amount", true),
        text_field("contract_original_id", true),
        timestamp_field("contract_delivery_date", true),
        text_field("contract_margin", true),
        text_field("contract_denominator", true),
        text_field("contract_type", true),
        timestamp_field("_timestamp", false),
    ]))
}

fn liquidation_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        timestamp_field("time", true),
        text_field("exchange", true),
        text_field("contract_id", true),
        text_field("price", true),
        timestamp_field("local_timestamp", true),
        text_field("symbol", true),
        text_field("side", true),
        float_field("quantity", true),
        float_field("amount", true),
        text_field("contract_original_id", true),
        timestamp_field("contract_delivery_date", true),
        text_field("contract_margin", true),
        text_field("contract_denominator", true),
        text_field("contract_type", true),
        timestamp_field("_timestamp", false),
    ]))
}

fn open_interest_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        timestamp_field("time", true),
        text_field("contract_id", true),
        text_field("exchange", true),
        timestamp_field("local_timestamp", true),
        text_field("symbol", true),
        float_field("open_interest", true),
        text_field("contract_original_id", true),
        timestamp_field("contract_delivery_date", true),
        text_field("contract_margin", true),
        text_field("contract_denominator", true),
        text_field("contract_type", true),
        timestamp_field("_timestamp", false),
    ]))
}

fn quote_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        timestamp_field("time", true),
        text_field("exchange", true),
        text_field("contract_id", true),
        text_field("bid_price", true),
        float_field("bid_quantity", true),
        text_field("ask_price", true),
        float_field("ask_quantity", true),
        timestamp_field("local_timestamp", true),
        text_field("symbol", true),
        text_field("contract_original_id", true),
        timestamp_field("contract_delivery_date", true),
        text_field("contract_margin", true),
        text_field("contract_denominator", true),
        text_field("contract_type", true),
        timestamp_field("_timestamp", false),
    ]))
}
