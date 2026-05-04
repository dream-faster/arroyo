use super::*;
use crate::source_field;
use arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arroyo_operator::connector::Connector;
use arroyo_rpc::TIMESTAMP_FIELD;
use arroyo_rpc::api_types::connections::{ConnectionSchema, FieldType};
use arroyo_rpc::formats::{FlatbuffersFormat, Format};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

fn schema() -> ConnectionSchema {
    ConnectionSchema {
        format: Some(Format::Flatbuffers(FlatbuffersFormat {})),
        bad_data: None,
        framing: None,
        fields: vec![source_field("value", FieldType::String)],
        definition: None,
        inferred: None,
        primary_keys: Default::default(),
    }
}

#[test]
fn sink_config_preserves_flatbuffers_format() {
    let connector = NatsConnector {};
    let connection = connector
        .from_config(
            None,
            "nats-flatbuffers",
            NatsConfig {
                authentication: NatsConfigAuthentication::None {},
                servers: VarStr::new("nats://127.0.0.1:4222".to_string()),
            },
            NatsTable {
                connector_type: ConnectorType::Sink {
                    sink_type: Some(SinkType::Subject("events".to_string())),
                },
            },
            Some(&schema()),
        )
        .unwrap();

    let config: arroyo_rpc::OperatorConfig = serde_json::from_str(&connection.config).unwrap();
    assert_eq!(
        config.format,
        Some(Format::Flatbuffers(FlatbuffersFormat {}))
    );
}

const TRADE_MDV1: &[u8] = &[
    16, 0, 0, 0, 77, 68, 86, 49, 8, 0, 12, 0, 11, 0, 4, 0, 8, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 1, 24,
    0, 60, 0, 52, 0, 48, 0, 44, 0, 40, 0, 36, 0, 28, 0, 24, 0, 20, 0, 12, 0, 4, 0, 24, 0, 0, 0, 51,
    51, 51, 51, 3, 189, 207, 64, 0, 0, 0, 0, 0, 0, 208, 63, 40, 0, 0, 0, 44, 0, 0, 0, 200, 77, 241,
    35, 142, 1, 0, 0, 44, 0, 0, 0, 56, 0, 0, 0, 64, 0, 0, 0, 80, 0, 0, 0, 123, 76, 241, 35, 142, 1,
    0, 0, 3, 0, 0, 0, 98, 117, 121, 0, 7, 0, 0, 0, 66, 84, 67, 85, 83, 68, 84, 0, 8, 0, 0, 0, 54,
    53, 48, 48, 48, 46, 49, 48, 0, 0, 0, 0, 7, 0, 0, 0, 66, 84, 67, 85, 83, 68, 84, 0, 12, 0, 0, 0,
    98, 105, 110, 97, 110, 99, 101, 45, 112, 101, 114, 112, 0, 0, 0, 0, 7, 0, 0, 0, 116, 114, 97,
    100, 101, 45, 49, 0,
];
const QUOTE_MDV1: &[u8] = &[
    16, 0, 0, 0, 77, 68, 86, 49, 8, 0, 10, 0, 9, 0, 4, 0, 8, 0, 0, 0, 28, 0, 0, 0, 0, 4, 22, 0, 68,
    0, 56, 0, 52, 0, 48, 0, 44, 0, 32, 0, 28, 0, 16, 0, 8, 0, 4, 0, 22, 0, 0, 0, 64, 0, 0, 0, 176,
    81, 241, 35, 142, 1, 0, 0, 0, 0, 0, 0, 0, 0, 2, 64, 0, 0, 0, 0, 52, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    248, 63, 0, 0, 0, 0, 48, 0, 0, 0, 56, 0, 0, 0, 64, 0, 0, 0, 99, 80, 241, 35, 142, 1, 0, 0, 0,
    0, 0, 0, 7, 0, 0, 0, 69, 84, 72, 85, 83, 68, 84, 0, 7, 0, 0, 0, 51, 50, 48, 48, 46, 52, 48, 0,
    7, 0, 0, 0, 51, 50, 48, 48, 46, 49, 48, 0, 7, 0, 0, 0, 69, 84, 72, 85, 83, 68, 84, 0, 12, 0, 0,
    0, 98, 105, 110, 97, 110, 99, 101, 45, 112, 101, 114, 112, 0, 0, 0, 0,
];
const LIQUIDATION_MDV1: &[u8] = &[
    20, 0, 0, 0, 77, 68, 86, 49, 0, 0, 0, 0, 8, 0, 10, 0, 9, 0, 4, 0, 8, 0, 0, 0, 28, 0, 0, 0, 0,
    2, 22, 0, 64, 0, 52, 0, 48, 0, 44, 0, 40, 0, 28, 0, 24, 0, 20, 0, 12, 0, 4, 0, 22, 0, 0, 0, 0,
    0, 0, 0, 128, 166, 152, 64, 0, 0, 0, 0, 0, 0, 37, 64, 44, 0, 0, 0, 52, 0, 0, 0, 152, 85, 241,
    35, 142, 1, 0, 0, 0, 0, 0, 0, 48, 0, 0, 0, 56, 0, 0, 0, 64, 0, 0, 0, 75, 84, 241, 35, 142, 1,
    0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 115, 101, 108, 108, 0, 0, 0, 0, 7, 0, 0, 0, 83, 79, 76, 85, 83,
    68, 84, 0, 6, 0, 0, 0, 49, 53, 48, 46, 50, 53, 0, 0, 7, 0, 0, 0, 83, 79, 76, 85, 83, 68, 84, 0,
    12, 0, 0, 0, 98, 105, 110, 97, 110, 99, 101, 45, 112, 101, 114, 112, 0, 0, 0, 0,
];
const OPEN_INTEREST_MDV1: &[u8] = &[
    16, 0, 0, 0, 77, 68, 86, 49, 8, 0, 12, 0, 11, 0, 4, 0, 8, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 3, 16,
    0, 48, 0, 36, 0, 32, 0, 28, 0, 20, 0, 16, 0, 4, 0, 16, 0, 0, 0, 138, 176, 225, 233, 214, 28,
    248, 64, 0, 0, 0, 0, 32, 0, 0, 0, 128, 89, 241, 35, 142, 1, 0, 0, 32, 0, 0, 0, 48, 0, 0, 0, 51,
    88, 241, 35, 142, 1, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 88, 82, 80, 85, 83, 68, 84, 0, 12, 0, 0, 0,
    98, 105, 110, 97, 110, 99, 101, 45, 112, 101, 114, 112, 0, 0, 0, 0, 7, 0, 0, 0, 88, 82, 80, 85,
    83, 68, 84, 0,
];
const DERIVATIVES_MDV1: &[u8] = &[
    16, 0, 0, 0, 77, 68, 86, 49, 8, 0, 12, 0, 11, 0, 4, 0, 8, 0, 0, 0, 28, 0, 0, 0, 0, 0, 0, 5, 20,
    0, 52, 0, 40, 0, 36, 0, 32, 0, 28, 0, 24, 0, 16, 0, 8, 0, 4, 0, 20, 0, 0, 0, 48, 0, 0, 0, 104,
    93, 241, 35, 142, 1, 0, 0, 252, 169, 241, 210, 77, 98, 32, 63, 40, 0, 0, 0, 52, 0, 0, 0, 64, 0,
    0, 0, 72, 0, 0, 0, 27, 92, 241, 35, 142, 1, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 66, 84, 67, 85, 83,
    68, 84, 0, 8, 0, 0, 0, 54, 53, 48, 48, 53, 46, 55, 53, 0, 0, 0, 0, 8, 0, 0, 0, 54, 53, 48, 49,
    48, 46, 50, 53, 0, 0, 0, 0, 7, 0, 0, 0, 66, 84, 67, 85, 83, 68, 84, 0, 12, 0, 0, 0, 98, 105,
    110, 97, 110, 99, 101, 45, 112, 101, 114, 112, 0, 0, 0, 0,
];

fn trade_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
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
        Field::new(
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]))
}

fn quote_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
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
        Field::new(
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]))
}

fn liquidation_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
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
        Field::new(
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]))
}

fn open_interest_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
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
        Field::new(
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]))
}

fn derivatives_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
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
        Field::new(
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]))
}

#[test]
fn mdv1_trade_payload_decodes_to_source_schema() {
    let metadata_timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(42);
    let decoded = decode_flatbuffers_message(
        TRADE_MDV1,
        Some(trade_schema().as_ref()),
        Some(metadata_timestamp),
    )
    .unwrap();
    let batch = &decoded[0];

    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "trade-1"
    );
    assert_eq!(
        batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "65000.10"
    );
    assert_eq!(
        batch
            .column(8)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        0.25
    );
    assert_eq!(
        batch
            .column(9)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        16250.025
    );
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(0),
        1_710_000_000_123_000_000
    );
    assert_eq!(
        batch
            .column(10)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(0),
        42_000_000_000
    );
}

#[test]
fn mdv1_quote_payload_decodes_to_source_schema() {
    let decoded = decode_flatbuffers_message(
        QUOTE_MDV1,
        Some(quote_schema().as_ref()),
        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(7)),
    )
    .unwrap();
    let batch = &decoded[0];

    assert_eq!(
        batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "3200.10"
    );
    assert_eq!(
        batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        1.5
    );
    assert_eq!(
        batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "3200.40"
    );
    assert_eq!(
        batch
            .column(6)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        2.25
    );
}

#[test]
fn mdv1_liquidation_payload_decodes_to_source_schema() {
    let decoded = decode_flatbuffers_message(
        LIQUIDATION_MDV1,
        Some(liquidation_schema().as_ref()),
        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(8)),
    )
    .unwrap();
    let batch = &decoded[0];

    assert_eq!(
        batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "150.25"
    );
    assert_eq!(
        batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "sell"
    );
    assert_eq!(
        batch
            .column(7)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        10.5
    );
}

#[test]
fn mdv1_open_interest_payload_decodes_to_source_schema() {
    let decoded = decode_flatbuffers_message(
        OPEN_INTEREST_MDV1,
        Some(open_interest_schema().as_ref()),
        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(9)),
    )
    .unwrap();
    let batch = &decoded[0];

    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "XRPUSDT"
    );
    assert_eq!(
        batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        98_765.432_1
    );
}

#[test]
fn mdv1_derivatives_payload_decodes_to_source_schema() {
    let decoded = decode_flatbuffers_message(
        DERIVATIVES_MDV1,
        Some(derivatives_schema().as_ref()),
        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(10)),
    )
    .unwrap();
    let batch = &decoded[0];

    assert_eq!(
        batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "65010.25"
    );
    assert_eq!(
        batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "65005.75"
    );
    assert_eq!(
        batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        0.000_125
    );
}
