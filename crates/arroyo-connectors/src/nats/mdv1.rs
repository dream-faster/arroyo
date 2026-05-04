use anyhow::{Context, anyhow, bail};
use arrow::array::{
    ArrayRef, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
    TimestampSecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arroyo_rpc::TIMESTAMP_FIELD;
use arroyo_types::to_nanos;
use std::sync::Arc;
use std::time::SystemTime;

const FILE_IDENTIFIER: &[u8; 4] = b"MDV1";

#[derive(Debug)]
enum DecodedValue<'a> {
    Float64(f64),
    Int64(i64),
    String(&'a str),
    TimestampMilliseconds(i64),
    TimestampNanoseconds(i64),
}

#[derive(Debug)]
pub(super) enum PayloadKind {
    Derivatives,
    Liquidation,
    OpenInterest,
    Quote,
    Trade,
}

#[derive(Debug)]
pub(super) struct DecodedMessage {
    metadata_timestamp: Option<SystemTime>,
    payload: Payload,
}

#[derive(Debug)]
enum Payload {
    Derivatives(DerivativesPayload),
    Trade(TradePayload),
    Quote(QuotePayload),
    Liquidation(LiquidationPayload),
    OpenInterest(OpenInterestPayload),
}

#[derive(Debug)]
struct TradePayload {
    time_unix_ms: Option<i64>,
    uid: Option<String>,
    exchange: Option<String>,
    contract_id: Option<String>,
    price: Option<String>,
    local_timestamp_unix_ms: Option<i64>,
    symbol: Option<String>,
    side: Option<String>,
    quantity: Option<f64>,
    amount: Option<f64>,
}

#[derive(Debug)]
struct QuotePayload {
    time_unix_ms: Option<i64>,
    exchange: Option<String>,
    contract_id: Option<String>,
    bid_price: Option<String>,
    bid_quantity: Option<f64>,
    ask_price: Option<String>,
    ask_quantity: Option<f64>,
    local_timestamp_unix_ms: Option<i64>,
    symbol: Option<String>,
}

#[derive(Debug)]
struct LiquidationPayload {
    time_unix_ms: Option<i64>,
    exchange: Option<String>,
    contract_id: Option<String>,
    price: Option<String>,
    local_timestamp_unix_ms: Option<i64>,
    symbol: Option<String>,
    side: Option<String>,
    quantity: Option<f64>,
    amount: Option<f64>,
}

#[derive(Debug)]
struct OpenInterestPayload {
    time_unix_ms: Option<i64>,
    contract_id: Option<String>,
    exchange: Option<String>,
    local_timestamp_unix_ms: Option<i64>,
    symbol: Option<String>,
    open_interest: Option<f64>,
}

#[derive(Debug)]
struct DerivativesPayload {
    time_unix_ms: Option<i64>,
    exchange: Option<String>,
    contract_id: Option<String>,
    mark_price: Option<String>,
    index_price: Option<String>,
    funding_rate: Option<f64>,
    local_timestamp_unix_ms: Option<i64>,
    symbol: Option<String>,
}

impl DecodedMessage {
    pub(super) fn into_record_batch(self, schema: Option<&Schema>) -> anyhow::Result<RecordBatch> {
        let schema = match schema {
            Some(schema) => Arc::new(schema.clone()),
            None => Arc::new(default_schema(self.payload.kind())),
        };

        let columns = schema
            .fields()
            .iter()
            .map(|field| self.build_array(field))
            .collect::<anyhow::Result<Vec<_>>>()?;

        RecordBatch::try_new(schema, columns).map_err(Into::into)
    }

    fn build_array(&self, field: &Field) -> anyhow::Result<ArrayRef> {
        let value = self.lookup_value(field.name());

        match field.data_type() {
            DataType::Float64 => {
                let mut builder = Float64Builder::new();
                match value {
                    Some(DecodedValue::Float64(v)) => builder.append_value(v),
                    Some(other) => {
                        bail!("field '{}' expected Float64, got {:?}", field.name(), other)
                    }
                    None if field.is_nullable() => builder.append_null(),
                    None => bail!("missing required field '{}'", field.name()),
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::new();
                match value {
                    Some(DecodedValue::Int64(v)) | Some(DecodedValue::TimestampMilliseconds(v)) => {
                        builder.append_value(v)
                    }
                    Some(other) => {
                        bail!("field '{}' expected Int64, got {:?}", field.name(), other)
                    }
                    None if field.is_nullable() => builder.append_null(),
                    None => bail!("missing required field '{}'", field.name()),
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                let mut builder = TimestampSecondBuilder::new();
                append_timestamp(&mut builder, field, value)?;
                Ok(Arc::new(builder.finish()))
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                let mut builder = TimestampMillisecondBuilder::new();
                append_timestamp(&mut builder, field, value)?;
                Ok(Arc::new(builder.finish()))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let mut builder = TimestampMicrosecondBuilder::new();
                append_timestamp(&mut builder, field, value)?;
                Ok(Arc::new(builder.finish()))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let mut builder = TimestampNanosecondBuilder::new();
                append_timestamp(&mut builder, field, value)?;
                Ok(Arc::new(builder.finish()))
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::new();
                match value {
                    Some(DecodedValue::String(v)) => builder.append_value(v),
                    Some(other) => {
                        bail!("field '{}' expected Utf8, got {:?}", field.name(), other)
                    }
                    None if field.is_nullable() => builder.append_null(),
                    None => bail!("missing required field '{}'", field.name()),
                }
                Ok(Arc::new(builder.finish()))
            }
            other => bail!(
                "MDV1 decoder does not support Arrow field '{}' with type {:?}",
                field.name(),
                other
            ),
        }
    }

    fn lookup_value(&self, field_name: &str) -> Option<DecodedValue<'_>> {
        if field_name == TIMESTAMP_FIELD {
            let fallback = self.payload.time_unix_ms().map(|v| v * 1_000_000);
            return self
                .metadata_timestamp
                .map(|ts| DecodedValue::TimestampNanoseconds(to_nanos(ts) as i64))
                .or_else(|| fallback.map(DecodedValue::TimestampNanoseconds));
        }

        self.payload.lookup_value(field_name)
    }
}

impl Payload {
    fn kind(&self) -> PayloadKind {
        match self {
            Self::Derivatives(_) => PayloadKind::Derivatives,
            Self::Liquidation(_) => PayloadKind::Liquidation,
            Self::OpenInterest(_) => PayloadKind::OpenInterest,
            Self::Quote(_) => PayloadKind::Quote,
            Self::Trade(_) => PayloadKind::Trade,
        }
    }

    fn time_unix_ms(&self) -> Option<i64> {
        match self {
            Self::Derivatives(payload) => payload.time_unix_ms,
            Self::Trade(payload) => payload.time_unix_ms,
            Self::Quote(payload) => payload.time_unix_ms,
            Self::Liquidation(payload) => payload.time_unix_ms,
            Self::OpenInterest(payload) => payload.time_unix_ms,
        }
    }

    fn lookup_value(&self, field_name: &str) -> Option<DecodedValue<'_>> {
        match self {
            Self::Derivatives(payload) => match field_name {
                "time" => payload
                    .time_unix_ms
                    .map(DecodedValue::TimestampMilliseconds),
                "time_unix_ms" => payload.time_unix_ms.map(DecodedValue::Int64),
                "exchange" => payload.exchange.as_deref().map(DecodedValue::String),
                "contract_id" => payload.contract_id.as_deref().map(DecodedValue::String),
                "mark_price" => payload.mark_price.as_deref().map(DecodedValue::String),
                "index_price" => payload.index_price.as_deref().map(DecodedValue::String),
                "funding_rate" => payload.funding_rate.map(DecodedValue::Float64),
                "local_timestamp" => payload
                    .local_timestamp_unix_ms
                    .map(DecodedValue::TimestampMilliseconds),
                "local_timestamp_unix_ms" => {
                    payload.local_timestamp_unix_ms.map(DecodedValue::Int64)
                }
                "symbol" => payload.symbol.as_deref().map(DecodedValue::String),
                _ => None,
            },
            Self::Trade(payload) => match field_name {
                "time" => payload
                    .time_unix_ms
                    .map(DecodedValue::TimestampMilliseconds),
                "time_unix_ms" => payload.time_unix_ms.map(DecodedValue::Int64),
                "uid" => payload.uid.as_deref().map(DecodedValue::String),
                "exchange" => payload.exchange.as_deref().map(DecodedValue::String),
                "contract_id" => payload.contract_id.as_deref().map(DecodedValue::String),
                "price" => payload.price.as_deref().map(DecodedValue::String),
                "local_timestamp" => payload
                    .local_timestamp_unix_ms
                    .map(DecodedValue::TimestampMilliseconds),
                "local_timestamp_unix_ms" => {
                    payload.local_timestamp_unix_ms.map(DecodedValue::Int64)
                }
                "symbol" => payload.symbol.as_deref().map(DecodedValue::String),
                "side" => payload.side.as_deref().map(DecodedValue::String),
                "quantity" => payload.quantity.map(DecodedValue::Float64),
                "amount" => payload.amount.map(DecodedValue::Float64),
                _ => None,
            },
            Self::Quote(payload) => match field_name {
                "time" => payload
                    .time_unix_ms
                    .map(DecodedValue::TimestampMilliseconds),
                "time_unix_ms" => payload.time_unix_ms.map(DecodedValue::Int64),
                "exchange" => payload.exchange.as_deref().map(DecodedValue::String),
                "contract_id" => payload.contract_id.as_deref().map(DecodedValue::String),
                "bid_price" => payload.bid_price.as_deref().map(DecodedValue::String),
                "bid_quantity" => payload.bid_quantity.map(DecodedValue::Float64),
                "ask_price" => payload.ask_price.as_deref().map(DecodedValue::String),
                "ask_quantity" => payload.ask_quantity.map(DecodedValue::Float64),
                "local_timestamp" => payload
                    .local_timestamp_unix_ms
                    .map(DecodedValue::TimestampMilliseconds),
                "local_timestamp_unix_ms" => {
                    payload.local_timestamp_unix_ms.map(DecodedValue::Int64)
                }
                "symbol" => payload.symbol.as_deref().map(DecodedValue::String),
                _ => None,
            },
            Self::Liquidation(payload) => match field_name {
                "time" => payload
                    .time_unix_ms
                    .map(DecodedValue::TimestampMilliseconds),
                "time_unix_ms" => payload.time_unix_ms.map(DecodedValue::Int64),
                "exchange" => payload.exchange.as_deref().map(DecodedValue::String),
                "contract_id" => payload.contract_id.as_deref().map(DecodedValue::String),
                "price" => payload.price.as_deref().map(DecodedValue::String),
                "local_timestamp" => payload
                    .local_timestamp_unix_ms
                    .map(DecodedValue::TimestampMilliseconds),
                "local_timestamp_unix_ms" => {
                    payload.local_timestamp_unix_ms.map(DecodedValue::Int64)
                }
                "symbol" => payload.symbol.as_deref().map(DecodedValue::String),
                "side" => payload.side.as_deref().map(DecodedValue::String),
                "quantity" => payload.quantity.map(DecodedValue::Float64),
                "amount" => payload.amount.map(DecodedValue::Float64),
                _ => None,
            },
            Self::OpenInterest(payload) => match field_name {
                "time" => payload
                    .time_unix_ms
                    .map(DecodedValue::TimestampMilliseconds),
                "time_unix_ms" => payload.time_unix_ms.map(DecodedValue::Int64),
                "contract_id" => payload.contract_id.as_deref().map(DecodedValue::String),
                "exchange" => payload.exchange.as_deref().map(DecodedValue::String),
                "local_timestamp" => payload
                    .local_timestamp_unix_ms
                    .map(DecodedValue::TimestampMilliseconds),
                "local_timestamp_unix_ms" => {
                    payload.local_timestamp_unix_ms.map(DecodedValue::Int64)
                }
                "symbol" => payload.symbol.as_deref().map(DecodedValue::String),
                "open_interest" => payload.open_interest.map(DecodedValue::Float64),
                _ => None,
            },
        }
    }
}

trait TimestampBuilder {
    fn append_value(&mut self, value: i64);
    fn append_null(&mut self);
}

impl TimestampBuilder for TimestampSecondBuilder {
    fn append_value(&mut self, value: i64) {
        TimestampSecondBuilder::append_value(self, value);
    }

    fn append_null(&mut self) {
        TimestampSecondBuilder::append_null(self);
    }
}

impl TimestampBuilder for TimestampMillisecondBuilder {
    fn append_value(&mut self, value: i64) {
        TimestampMillisecondBuilder::append_value(self, value);
    }

    fn append_null(&mut self) {
        TimestampMillisecondBuilder::append_null(self);
    }
}

impl TimestampBuilder for TimestampMicrosecondBuilder {
    fn append_value(&mut self, value: i64) {
        TimestampMicrosecondBuilder::append_value(self, value);
    }

    fn append_null(&mut self) {
        TimestampMicrosecondBuilder::append_null(self);
    }
}

impl TimestampBuilder for TimestampNanosecondBuilder {
    fn append_value(&mut self, value: i64) {
        TimestampNanosecondBuilder::append_value(self, value);
    }

    fn append_null(&mut self) {
        TimestampNanosecondBuilder::append_null(self);
    }
}

fn append_timestamp(
    builder: &mut dyn TimestampBuilder,
    field: &Field,
    value: Option<DecodedValue<'_>>,
) -> anyhow::Result<()> {
    match value {
        Some(DecodedValue::TimestampMilliseconds(v)) => {
            builder.append_value(match field.data_type() {
                DataType::Timestamp(TimeUnit::Second, _) => v / 1_000,
                DataType::Timestamp(TimeUnit::Millisecond, _) => v,
                DataType::Timestamp(TimeUnit::Microsecond, _) => v * 1_000,
                DataType::Timestamp(TimeUnit::Nanosecond, _) => v * 1_000_000,
                _ => unreachable!("append_timestamp only called for timestamp fields"),
            });
        }
        Some(DecodedValue::TimestampNanoseconds(v)) => {
            builder.append_value(match field.data_type() {
                DataType::Timestamp(TimeUnit::Second, _) => v / 1_000_000_000,
                DataType::Timestamp(TimeUnit::Millisecond, _) => v / 1_000_000,
                DataType::Timestamp(TimeUnit::Microsecond, _) => v / 1_000,
                DataType::Timestamp(TimeUnit::Nanosecond, _) => v,
                _ => unreachable!("append_timestamp only called for timestamp fields"),
            });
        }
        Some(other) => bail!(
            "field '{}' expected Timestamp, got {:?}",
            field.name(),
            other
        ),
        None if field.is_nullable() => builder.append_null(),
        None => bail!("missing required field '{}'", field.name()),
    }

    Ok(())
}

pub(super) fn is_mdv1_message(msg: &[u8]) -> bool {
    msg.get(4..8) == Some(FILE_IDENTIFIER)
}

pub(super) fn decode_message(
    msg: &[u8],
    schema: Option<&Schema>,
    metadata_timestamp: Option<SystemTime>,
) -> anyhow::Result<RecordBatch> {
    let envelope_pos = root_table_position(msg)?;
    let payload_type = read_u8_field(msg, envelope_pos, 0)?
        .ok_or_else(|| anyhow!("MDV1 envelope missing payload type"))?;
    let payload_pos = read_table_field(msg, envelope_pos, 1)?
        .ok_or_else(|| anyhow!("MDV1 envelope missing payload"))?;

    let payload = match payload_type {
        1 => Payload::Trade(parse_trade(msg, payload_pos)?),
        2 => Payload::Liquidation(parse_liquidation(msg, payload_pos)?),
        3 => Payload::OpenInterest(parse_open_interest(msg, payload_pos)?),
        4 => Payload::Quote(parse_quote(msg, payload_pos)?),
        5 => Payload::Derivatives(parse_derivatives(msg, payload_pos)?),
        other => bail!("unsupported MDV1 payload type {}", other),
    };

    DecodedMessage {
        metadata_timestamp,
        payload,
    }
    .into_record_batch(schema)
}

fn default_schema(kind: PayloadKind) -> Schema {
    let mut fields = match kind {
        PayloadKind::Derivatives => vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("exchange", DataType::Utf8, false),
            Field::new("contract_id", DataType::Utf8, false),
            Field::new("mark_price", DataType::Utf8, false),
            Field::new("index_price", DataType::Utf8, false),
            Field::new("funding_rate", DataType::Float64, false),
            Field::new(
                "local_timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("symbol", DataType::Utf8, false),
        ],
        PayloadKind::Trade => vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("uid", DataType::Utf8, false),
            Field::new("exchange", DataType::Utf8, false),
            Field::new("contract_id", DataType::Utf8, false),
            Field::new("price", DataType::Utf8, false),
            Field::new(
                "local_timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("side", DataType::Utf8, false),
            Field::new("quantity", DataType::Float64, false),
            Field::new("amount", DataType::Float64, false),
        ],
        PayloadKind::Quote => vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("exchange", DataType::Utf8, false),
            Field::new("contract_id", DataType::Utf8, false),
            Field::new("bid_price", DataType::Utf8, false),
            Field::new("bid_quantity", DataType::Float64, false),
            Field::new("ask_price", DataType::Utf8, false),
            Field::new("ask_quantity", DataType::Float64, false),
            Field::new(
                "local_timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("symbol", DataType::Utf8, false),
        ],
        PayloadKind::Liquidation => vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("exchange", DataType::Utf8, false),
            Field::new("contract_id", DataType::Utf8, false),
            Field::new("price", DataType::Utf8, false),
            Field::new(
                "local_timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("side", DataType::Utf8, false),
            Field::new("quantity", DataType::Float64, false),
            Field::new("amount", DataType::Float64, false),
        ],
        PayloadKind::OpenInterest => vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("contract_id", DataType::Utf8, false),
            Field::new("exchange", DataType::Utf8, false),
            Field::new(
                "local_timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("open_interest", DataType::Float64, false),
        ],
    };
    fields.push(Field::new(
        TIMESTAMP_FIELD,
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    ));
    Schema::new(fields)
}

fn parse_trade(buf: &[u8], pos: usize) -> anyhow::Result<TradePayload> {
    Ok(TradePayload {
        time_unix_ms: read_i64_field(buf, pos, 0)?,
        uid: read_string_field(buf, pos, 1)?,
        exchange: read_string_field(buf, pos, 2)?,
        contract_id: read_string_field(buf, pos, 3)?,
        price: read_string_field(buf, pos, 4)?,
        local_timestamp_unix_ms: read_i64_field(buf, pos, 5)?,
        symbol: read_string_field(buf, pos, 6)?,
        side: read_string_field(buf, pos, 7)?,
        quantity: read_f64_field(buf, pos, 8)?,
        amount: read_f64_field(buf, pos, 9)?,
    })
}

fn parse_quote(buf: &[u8], pos: usize) -> anyhow::Result<QuotePayload> {
    Ok(QuotePayload {
        time_unix_ms: read_i64_field(buf, pos, 0)?,
        exchange: read_string_field(buf, pos, 1)?,
        contract_id: read_string_field(buf, pos, 2)?,
        bid_price: read_string_field(buf, pos, 3)?,
        bid_quantity: read_f64_field(buf, pos, 4)?,
        ask_price: read_string_field(buf, pos, 5)?,
        ask_quantity: read_f64_field(buf, pos, 6)?,
        local_timestamp_unix_ms: read_i64_field(buf, pos, 7)?,
        symbol: read_string_field(buf, pos, 8)?,
    })
}

fn parse_liquidation(buf: &[u8], pos: usize) -> anyhow::Result<LiquidationPayload> {
    Ok(LiquidationPayload {
        time_unix_ms: read_i64_field(buf, pos, 0)?,
        exchange: read_string_field(buf, pos, 1)?,
        contract_id: read_string_field(buf, pos, 2)?,
        price: read_string_field(buf, pos, 3)?,
        local_timestamp_unix_ms: read_i64_field(buf, pos, 4)?,
        symbol: read_string_field(buf, pos, 5)?,
        side: read_string_field(buf, pos, 6)?,
        quantity: read_f64_field(buf, pos, 7)?,
        amount: read_f64_field(buf, pos, 8)?,
    })
}

fn parse_open_interest(buf: &[u8], pos: usize) -> anyhow::Result<OpenInterestPayload> {
    Ok(OpenInterestPayload {
        time_unix_ms: read_i64_field(buf, pos, 0)?,
        contract_id: read_string_field(buf, pos, 1)?,
        exchange: read_string_field(buf, pos, 2)?,
        local_timestamp_unix_ms: read_i64_field(buf, pos, 3)?,
        symbol: read_string_field(buf, pos, 4)?,
        open_interest: read_f64_field(buf, pos, 5)?,
    })
}

fn parse_derivatives(buf: &[u8], pos: usize) -> anyhow::Result<DerivativesPayload> {
    Ok(DerivativesPayload {
        time_unix_ms: read_i64_field(buf, pos, 0)?,
        exchange: read_string_field(buf, pos, 1)?,
        contract_id: read_string_field(buf, pos, 2)?,
        mark_price: read_string_field(buf, pos, 3)?,
        index_price: read_string_field(buf, pos, 4)?,
        funding_rate: read_f64_field(buf, pos, 5)?,
        local_timestamp_unix_ms: read_i64_field(buf, pos, 6)?,
        symbol: read_string_field(buf, pos, 7)?,
    })
}

fn root_table_position(buf: &[u8]) -> anyhow::Result<usize> {
    let pos = read_u32(buf, 0)? as usize;
    if pos >= buf.len() {
        bail!("MDV1 root table offset {} is outside payload", pos);
    }
    Ok(pos)
}

fn read_table_field(
    buf: &[u8],
    table_pos: usize,
    field_index: usize,
) -> anyhow::Result<Option<usize>> {
    let Some(field_offset) = table_field_offset(buf, table_pos, field_index)? else {
        return Ok(None);
    };
    let field_pos = table_pos + field_offset;
    let relative = read_u32(buf, field_pos)? as usize;
    Ok(Some(field_pos.checked_add(relative).ok_or_else(|| {
        anyhow!("flatbuffers table reference overflow")
    })?))
}

fn read_string_field(
    buf: &[u8],
    table_pos: usize,
    field_index: usize,
) -> anyhow::Result<Option<String>> {
    let Some(value_pos) = read_table_field(buf, table_pos, field_index)? else {
        return Ok(None);
    };
    let len = read_u32(buf, value_pos)? as usize;
    let start = value_pos
        .checked_add(4)
        .ok_or_else(|| anyhow!("flatbuffers string start overflow"))?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| anyhow!("flatbuffers string end overflow"))?;
    let bytes = buf
        .get(start..end)
        .ok_or_else(|| anyhow!("flatbuffers string exceeds payload bounds"))?;
    Ok(Some(
        std::str::from_utf8(bytes)
            .context("flatbuffers string is not valid UTF-8")?
            .to_string(),
    ))
}

fn read_u8_field(buf: &[u8], table_pos: usize, field_index: usize) -> anyhow::Result<Option<u8>> {
    let Some(field_offset) = table_field_offset(buf, table_pos, field_index)? else {
        return Ok(None);
    };
    let field_pos = table_pos + field_offset;
    Ok(Some(*buf.get(field_pos).ok_or_else(|| {
        anyhow!(
            "flatbuffers u8 field at position {} is out of bounds",
            field_pos
        )
    })?))
}

fn read_i64_field(buf: &[u8], table_pos: usize, field_index: usize) -> anyhow::Result<Option<i64>> {
    let Some(field_offset) = table_field_offset(buf, table_pos, field_index)? else {
        return Ok(None);
    };
    Ok(Some(read_i64(buf, table_pos + field_offset)?))
}

fn read_f64_field(buf: &[u8], table_pos: usize, field_index: usize) -> anyhow::Result<Option<f64>> {
    let Some(field_offset) = table_field_offset(buf, table_pos, field_index)? else {
        return Ok(None);
    };
    Ok(Some(read_f64(buf, table_pos + field_offset)?))
}

fn table_field_offset(
    buf: &[u8],
    table_pos: usize,
    field_index: usize,
) -> anyhow::Result<Option<usize>> {
    let vtable_distance = read_i32(buf, table_pos)? as usize;
    let vtable_pos = table_pos
        .checked_sub(vtable_distance)
        .ok_or_else(|| anyhow!("flatbuffers vtable position underflow"))?;
    let vtable_len = read_u16(buf, vtable_pos)? as usize;
    let entry_pos = vtable_pos
        .checked_add(4 + field_index * 2)
        .ok_or_else(|| anyhow!("flatbuffers vtable entry overflow"))?;
    if entry_pos + 2 > vtable_pos + vtable_len {
        return Ok(None);
    }
    let offset = read_u16(buf, entry_pos)? as usize;
    if offset == 0 {
        Ok(None)
    } else {
        Ok(Some(offset))
    }
}

fn read_u16(buf: &[u8], pos: usize) -> anyhow::Result<u16> {
    let bytes = buf
        .get(pos..pos + 2)
        .ok_or_else(|| anyhow!("read_u16 out of bounds at {}", pos))?;
    Ok(u16::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u32(buf: &[u8], pos: usize) -> anyhow::Result<u32> {
    let bytes = buf
        .get(pos..pos + 4)
        .ok_or_else(|| anyhow!("read_u32 out of bounds at {}", pos))?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_i32(buf: &[u8], pos: usize) -> anyhow::Result<i32> {
    let bytes = buf
        .get(pos..pos + 4)
        .ok_or_else(|| anyhow!("read_i32 out of bounds at {}", pos))?;
    Ok(i32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_i64(buf: &[u8], pos: usize) -> anyhow::Result<i64> {
    let bytes = buf
        .get(pos..pos + 8)
        .ok_or_else(|| anyhow!("read_i64 out of bounds at {}", pos))?;
    Ok(i64::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_f64(buf: &[u8], pos: usize) -> anyhow::Result<f64> {
    let bytes = buf
        .get(pos..pos + 8)
        .ok_or_else(|| anyhow!("read_f64 out of bounds at {}", pos))?;
    Ok(f64::from_le_bytes(bytes.try_into().unwrap()))
}
