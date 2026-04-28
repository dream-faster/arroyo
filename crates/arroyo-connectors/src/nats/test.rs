use super::*;
use crate::source_field;
use arrow::array::{Array, Float64Array, StringArray, TimestampNanosecondArray};
use arroyo_operator::connector::Connector;
use arroyo_rpc::api_types::connections::{ConnectionSchema, FieldType};
use arroyo_rpc::formats::{FlatbuffersFormat, Format};
use std::time::{Duration, UNIX_EPOCH};

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

const LIVE_INGESTION_TRADE_MESSAGE: &[u8] = &[
    16, 0, 0, 0, 77, 68, 86, 49, 8, 0, 12, 0, 11, 0, 4, 0, 8, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 1, 24,
    0, 64, 0, 52, 0, 48, 0, 44, 0, 40, 0, 36, 0, 28, 0, 24, 0, 20, 0, 12, 0, 4, 0, 24, 0, 0, 0, 0,
    0, 0, 0, 202, 235, 242, 64, 0, 0, 0, 0, 0, 0, 244, 63, 44, 0, 0, 0, 48, 0, 0, 0, 200, 77, 241,
    35, 142, 1, 0, 0, 56, 0, 0, 0, 64, 0, 0, 0, 80, 0, 0, 0, 96, 0, 0, 0, 123, 76, 241, 35, 142, 1,
    0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 98, 117, 121, 0, 13, 0, 0, 0, 66, 84, 67, 45, 85, 83, 68, 84, 45,
    83, 87, 65, 80, 0, 0, 0, 7, 0, 0, 0, 54, 50, 48, 48, 48, 46, 53, 0, 13, 0, 0, 0, 66, 84, 67,
    45, 85, 83, 68, 84, 45, 83, 87, 65, 80, 0, 0, 0, 13, 0, 0, 0, 111, 107, 120, 45, 112, 101, 114,
    112, 101, 116, 117, 97, 108, 0, 0, 0, 7, 0, 0, 0, 116, 114, 97, 100, 101, 45, 49, 0,
];

const LIVE_INGESTION_QUOTE_MESSAGE: &[u8] = &[
    16, 0, 0, 0, 77, 68, 86, 49, 8, 0, 12, 0, 11, 0, 4, 0, 8, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 4, 24,
    0, 68, 0, 60, 0, 56, 0, 52, 0, 48, 0, 36, 0, 32, 0, 20, 0, 12, 0, 8, 0, 4, 0, 24, 0, 0, 0, 80,
    0, 0, 0, 168, 0, 0, 0, 85, 145, 0, 36, 142, 1, 0, 0, 0, 0, 0, 0, 0, 0, 14, 64, 0, 0, 0, 0, 164,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 64, 0, 0, 0, 0, 160, 0, 0, 0, 168, 0, 0, 0, 184, 0, 0, 0, 187,
    142, 0, 36, 142, 1, 0, 0, 16, 0, 36, 0, 32, 0, 31, 0, 16, 0, 12, 0, 8, 0, 4, 0, 16, 0, 0, 0,
    32, 0, 0, 0, 44, 0, 0, 0, 48, 0, 0, 0, 0, 45, 231, 41, 142, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
    40, 0, 0, 0, 9, 0, 0, 0, 112, 101, 114, 112, 101, 116, 117, 97, 108, 0, 0, 0, 3, 0, 0, 0, 66,
    84, 67, 0, 4, 0, 0, 0, 85, 83, 68, 84, 0, 0, 0, 0, 13, 0, 0, 0, 66, 84, 67, 45, 85, 83, 68, 84,
    45, 83, 87, 65, 80, 0, 0, 0, 13, 0, 0, 0, 66, 84, 67, 45, 85, 83, 68, 84, 45, 83, 87, 65, 80,
    0, 0, 0, 7, 0, 0, 0, 54, 50, 48, 48, 48, 46, 49, 0, 7, 0, 0, 0, 54, 49, 57, 57, 57, 46, 57, 0,
    13, 0, 0, 0, 66, 84, 67, 45, 85, 83, 68, 84, 45, 83, 87, 65, 80, 0, 0, 0, 13, 0, 0, 0, 111,
    107, 120, 45, 112, 101, 114, 112, 101, 116, 117, 97, 108, 0, 0, 0,
];

#[test]
fn decode_live_ingestion_trade_payload_into_sql_schema() {
    let timestamp = UNIX_EPOCH + Duration::from_secs(1_800_000_000) + Duration::from_nanos(123);
    let batches =
        crate::nats::decode_flatbuffers_message(LIVE_INGESTION_TRADE_MESSAGE, timestamp).unwrap();
    assert_eq!(batches.len(), 1);

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 16);
    assert_eq!(batch.schema().field(0).name(), "time");
    assert_eq!(timestamp_value(batch, 0), Some(1_710_000_000_123_000_000));
    assert_eq!(string_value(batch, 1), Some("trade-1"));
    assert_eq!(string_value(batch, 2), Some("okx-perpetual"));
    assert_eq!(string_value(batch, 3), Some("BTC-USDT-SWAP"));
    assert_eq!(string_value(batch, 4), Some("62000.5"));
    assert_eq!(timestamp_value(batch, 5), Some(1_710_000_000_456_000_000));
    assert_eq!(string_value(batch, 7), Some("buy"));
    assert_eq!(f64_value(batch, 8), Some(1.25));
    assert_eq!(f64_value(batch, 9), Some(77_500.625));
    assert_eq!(string_value(batch, 10), None);
    assert_eq!(timestamp_value(batch, 11), None);
    assert_eq!(timestamp_value(batch, 15), Some(1_800_000_000_000_000_123));
}

#[test]
fn decode_live_ingestion_quote_payload_with_contract_meta() {
    let timestamp = UNIX_EPOCH + Duration::from_secs(1_800_000_100) + Duration::from_nanos(456);
    let batches =
        crate::nats::decode_flatbuffers_message(LIVE_INGESTION_QUOTE_MESSAGE, timestamp).unwrap();
    assert_eq!(batches.len(), 1);

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 15);
    assert_eq!(timestamp_value(batch, 0), Some(1_710_001_000_123_000_000));
    assert_eq!(string_value(batch, 1), Some("okx-perpetual"));
    assert_eq!(string_value(batch, 2), Some("BTC-USDT-SWAP"));
    assert_eq!(string_value(batch, 3), Some("61999.9"));
    assert_eq!(f64_value(batch, 4), Some(2.5));
    assert_eq!(string_value(batch, 5), Some("62000.1"));
    assert_eq!(f64_value(batch, 6), Some(3.75));
    assert_eq!(timestamp_value(batch, 7), Some(1_710_001_000_789_000_000));
    assert_eq!(string_value(batch, 8), Some("BTC-USDT-SWAP"));
    assert_eq!(string_value(batch, 9), Some("BTC-USDT-SWAP"));
    assert_eq!(timestamp_value(batch, 10), Some(1_710_100_000_000_000_000));
    assert_eq!(string_value(batch, 11), Some("USDT"));
    assert_eq!(string_value(batch, 12), Some("BTC"));
    assert_eq!(string_value(batch, 13), Some("perpetual"));
    assert_eq!(timestamp_value(batch, 14), Some(1_800_000_100_000_000_456));
}

fn string_value(batch: &arrow::array::RecordBatch, column: usize) -> Option<&str> {
    let array = batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    if array.is_null(0) {
        None
    } else {
        Some(array.value(0))
    }
}

fn timestamp_value(batch: &arrow::array::RecordBatch, column: usize) -> Option<i64> {
    let array = batch
        .column(column)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    if array.is_null(0) {
        None
    } else {
        Some(array.value(0))
    }
}

fn f64_value(batch: &arrow::array::RecordBatch, column: usize) -> Option<f64> {
    let array = batch
        .column(column)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    if array.is_null(0) {
        None
    } else {
        Some(array.value(0))
    }
}
