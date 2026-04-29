#![allow(clippy::unnecessary_mut_passed)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow::array::{RecordBatch, UInt32Array};
use arrow::datatypes::Field;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::context::OperatorContext;
use arroyo_operator::operator::ArrowOperator;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::formats::{Format, JsonFormat};
use arroyo_types::CheckpointBarrier;
use arroyo_types::*;
use itertools::Itertools;
use rskafka::client::consumer::StartOffset;
use serde::Deserialize;
use tokio::sync::mpsc::channel;

use super::{ConsistencyMode, KafkaSinkFunc};
use crate::kafka::client_utils::{
    build_client, create_topic, delete_topic, partition_client, spawn_partition_consumers,
};
use crate::kafka::{Context, KafkaConfig, KafkaConfigAuthentication};
use crate::test::DummyCollector;

pub struct KafkaTopicTester {
    topic: String,
    server: String,
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt32,
        false,
    )]))
}

#[derive(Deserialize)]
struct TestData {
    value: u32,
}

fn test_config(server: &str) -> KafkaConfig {
    KafkaConfig {
        authentication: KafkaConfigAuthentication::None {},
        bootstrap_servers: crate::kafka::BootstrapServers(server.to_string()),
        schema_registry_enum: None,
        connection_properties: HashMap::new(),
    }
}

impl KafkaTopicTester {
    async fn create_topic(&self, num_partitions: i32) {
        let client = build_client(
            &self.server,
            &HashMap::new(),
            &Context::new(Some(test_config(&self.server))),
        )
        .await
        .unwrap();
        let _ = delete_topic(&client, &self.topic).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        create_topic(&client, &self.topic, num_partitions)
            .await
            .unwrap();
    }

    async fn get_sink_with_writes(&self) -> KafkaSinkWithWrites {
        let mut kafka = KafkaSinkFunc {
            topic: self.topic.to_string(),
            bootstrap_servers: self.server.to_string(),
            producer: None,
            consistency_mode: ConsistencyMode::AtLeastOnce,
            timestamp_field: None,
            timestamp_col: None,
            key_field: None,
            client_config: HashMap::new(),
            context: Context::new(Some(test_config(&self.server))),
            serializer: ArrowSerializer::new(Format::Json(JsonFormat::default())),
            key_col: None,
        };

        let (command_tx, _) = channel(128);

        let task_info = Arc::new(get_test_task_info());

        let mut ctx = OperatorContext::new(
            task_info,
            None,
            command_tx,
            1,
            vec![Arc::new(ArroyoSchema::new_unkeyed(schema(), 0))],
            None,
            HashMap::new(),
        )
        .await;

        kafka.on_start(&mut ctx).await.unwrap();

        KafkaSinkWithWrites { sink: kafka, ctx }
    }

    async fn get_consumer(&self, partitions: i32) -> KafkaTopicConsumer {
        let client = build_client(
            &self.server,
            &HashMap::new(),
            &Context::new(Some(test_config(&self.server))),
        )
        .await
        .unwrap();

        let mut consumers = Vec::new();
        for partition in 0..partitions {
            consumers.push((
                partition,
                partition_client(&client, &self.topic, partition)
                    .await
                    .unwrap(),
                StartOffset::Earliest,
            ));
        }

        let (rx, handles) = spawn_partition_consumers(&self.topic, consumers);
        KafkaTopicConsumer { rx, handles }
    }
}

struct KafkaTopicConsumer {
    rx: tokio::sync::mpsc::Receiver<anyhow::Result<crate::kafka::client_utils::ConsumedRecord>>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for KafkaTopicConsumer {
    fn drop(&mut self) {
        for handle in self.handles.drain(..) {
            handle.abort();
        }
    }
}

async fn get_data(consumer: &mut KafkaTopicConsumer) -> String {
    let message = consumer
        .rx
        .recv()
        .await
        .expect("should receive a kafka record")
        .expect("shouldn't have errored");
    String::from_utf8(message.record.record.value.unwrap()).unwrap()
}

struct KafkaSinkWithWrites {
    sink: KafkaSinkFunc,
    ctx: OperatorContext,
}

#[tokio::test]
async fn test_kafka_checkpoint_flushes() {
    let kafka_topic_tester = KafkaTopicTester {
        topic: "arroyo-sink-checkpoint".to_string(),
        server: "0.0.0.0:9092".to_string(),
    };

    kafka_topic_tester.create_topic(1).await;
    let mut sink_with_writes = kafka_topic_tester.get_sink_with_writes().await;
    let mut consumer = kafka_topic_tester.get_consumer(1).await;

    for chunk in &(1u32..200).chunks(7) {
        let array = UInt32Array::from_iter_values(chunk.into_iter());
        let batch = RecordBatch::try_new(schema(), vec![Arc::new(array)]).unwrap();

        sink_with_writes
            .sink
            .process_batch(batch, &mut sink_with_writes.ctx, &mut DummyCollector {})
            .await
            .unwrap();
    }
    let barrier = CheckpointBarrier {
        epoch: 2,
        min_epoch: 0,
        timestamp: SystemTime::now(),
        then_stop: false,
    };
    sink_with_writes
        .sink
        .handle_checkpoint(barrier, &mut sink_with_writes.ctx, &mut DummyCollector {})
        .await
        .unwrap();

    for message in 1u32..200 {
        let record = get_data(&mut consumer).await;
        let result: TestData = serde_json::from_str(&record).unwrap();
        assert_eq!(message, result.value, "{message} {record:?}");
    }
}

#[tokio::test]
async fn test_kafka() {
    let kafka_topic_tester = KafkaTopicTester {
        topic: "arroyo-sink".to_string(),
        server: "0.0.0.0:9092".to_string(),
    };

    kafka_topic_tester.create_topic(2).await;
    let mut sink_with_writes = kafka_topic_tester.get_sink_with_writes().await;
    let mut consumer = kafka_topic_tester.get_consumer(2).await;

    for message in 1u32..20 {
        let data = UInt32Array::from_iter_values(vec![message].into_iter());
        let batch = RecordBatch::try_new(schema(), vec![Arc::new(data)]).unwrap();

        sink_with_writes
            .sink
            .process_batch(batch, &mut sink_with_writes.ctx, &mut DummyCollector {})
            .await
            .unwrap();

        let result: TestData = serde_json::from_str(&get_data(&mut consumer).await).unwrap();
        assert_eq!(message, result.value);
    }
}
