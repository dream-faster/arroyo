use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType::UInt64;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arroyo_operator::context::{
    ArrowCollector, BatchReceiver, OperatorContext, SourceCollector, SourceContext, batch_bounded,
};
use arroyo_operator::operator::SourceOperator;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::formats::{Format, RawStringFormat};
use arroyo_rpc::grpc::rpc::{CheckpointMetadata, OperatorCheckpointMetadata, OperatorMetadata};
use arroyo_rpc::{CheckpointCompleted, ControlMessage, ControlResp, MetadataField};
use arroyo_state::tables::global_keyed_map::GlobalKeyedTable;
use arroyo_state::{BackingStore, StateBackend, tables::ErasedTable};
use arroyo_types::{
    ArrowMessage, ChainInfo, CheckpointBarrier, SignalMessage, TaskInfo, single_item_hash_map,
    to_micros,
};
use rand::random;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc::{Receiver, Sender, channel};

use super::KafkaSourceFunc;
use crate::kafka::client_utils::{
    build_client, create_topic, delete_topic, kafka_record, partition_client, produce_records,
};
use crate::kafka::{Context, KafkaConfig, KafkaConfigAuthentication, SourceOffset};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestData {
    i: u64,
}

fn test_config(server: &str) -> KafkaConfig {
    KafkaConfig {
        authentication: KafkaConfigAuthentication::None {},
        bootstrap_servers: crate::kafka::BootstrapServers(server.to_string()),
        schema_registry_enum: None,
        connection_properties: HashMap::new(),
    }
}

pub struct KafkaTopicTester {
    topic: String,
    server: String,
    group_id: Option<String>,
}

impl KafkaTopicTester {
    async fn create_topic(&self) {
        let config = test_config(&self.server);
        let client = build_client(&self.server, &HashMap::new(), &Context::new(Some(config)))
            .await
            .unwrap();
        let _ = delete_topic(&client, &self.topic).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        create_topic(&client, &self.topic, 1).await.unwrap();
    }

    async fn get_source_with_reader(
        &self,
        task_info: TaskInfo,
        restore_from: Option<u32>,
    ) -> KafkaSourceWithReads {
        let mut kafka = Box::new(KafkaSourceFunc {
            bootstrap_servers: self.server.clone(),
            topic: self.topic.clone(),
            group_id: self.group_id.clone(),
            group_id_prefix: None,
            offset_mode: SourceOffset::Earliest,
            format: Format::RawString(RawStringFormat {}),
            framing: None,
            bad_data: None,
            schema_resolver: None,
            client_configs: HashMap::new(),
            context: Context::new(Some(test_config(&self.server))),
            messages_per_second: NonZeroU32::new(100).unwrap(),
            metadata_fields: vec![],
        });

        let (to_control_tx, control_rx) = channel(128);
        let (command_tx, from_control_rx) = channel(128);
        let (data_tx, recv) = batch_bounded(128);

        let checkpoint_metadata = restore_from.map(|epoch| CheckpointMetadata {
            job_id: task_info.job_id.to_string(),
            epoch,
            min_epoch: 1,
            start_time: to_micros(SystemTime::now()),
            finish_time: to_micros(SystemTime::now()),
            operator_ids: vec![task_info.operator_id.clone()],
        });

        let out_schema = Some(Arc::new(ArroyoSchema::new_unkeyed(
            Arc::new(Schema::new(vec![
                Field::new(
                    "_timestamp",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("value", DataType::Utf8, false),
            ])),
            0,
        )));

        let task_info = Arc::new(task_info);

        let ctx = OperatorContext::new(
            task_info.clone(),
            checkpoint_metadata.as_ref(),
            command_tx.clone(),
            1,
            vec![],
            out_schema,
            kafka.tables(),
        )
        .await;

        let chain_info = Arc::new(ChainInfo {
            job_id: ctx.task_info.job_id.clone(),
            task_id: ctx.task_info.operator_idx,
            description: "kafka source".to_string(),
            task_index: ctx.task_info.task_index,
        });

        let mut ctx = SourceContext::from_operator(ctx, chain_info.clone(), control_rx);
        let arrow_collector = ArrowCollector::new(
            chain_info.clone(),
            Some(ctx.out_schema.clone()),
            vec![vec![data_tx]],
        );
        let mut collector = SourceCollector::new(
            ctx.out_schema.clone(),
            arrow_collector,
            command_tx,
            &task_info,
        );

        tokio::spawn(async move {
            kafka.run(&mut ctx, &mut collector).await.unwrap();
        });
        KafkaSourceWithReads {
            to_control_tx,
            from_control_rx,
            data_recv: recv,
        }
    }

    async fn get_producer(&self) -> KafkaTopicProducer {
        let config = test_config(&self.server);
        let client = build_client(&self.server, &HashMap::new(), &Context::new(Some(config)))
            .await
            .unwrap();
        KafkaTopicProducer {
            partition_client: partition_client(&client, &self.topic, 0).await.unwrap(),
        }
    }
}

struct KafkaTopicProducer {
    partition_client: Arc<rskafka::client::partition::PartitionClient>,
}

impl KafkaTopicProducer {
    async fn send_data(&mut self, data: TestData) {
        let json = serde_json::to_string(&data).unwrap();
        produce_records(
            &self.partition_client,
            vec![kafka_record(None, json.into_bytes(), 0)],
        )
        .await
        .expect("could not send message");
    }
}

struct KafkaSourceWithReads {
    to_control_tx: Sender<ControlMessage>,
    from_control_rx: Receiver<ControlResp>,
    data_recv: BatchReceiver,
}

impl KafkaSourceWithReads {
    async fn assert_next_message_record_values(&mut self, mut expected_values: VecDeque<String>) {
        while !expected_values.is_empty() {
            match self.data_recv.recv().await {
                Some(item) => {
                    if let ArrowMessage::Data(record) = item {
                        let array = record.columns()[1]
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();

                        for value in array {
                            assert_eq!(
                                expected_values
                                    .pop_front()
                                    .expect("found more elements than expected"),
                                value.unwrap()
                            );
                        }
                    } else {
                        unreachable!("expected data, got {:?}", item);
                    }
                }
                None => unreachable!("option shouldn't be missing"),
            }
        }
    }

    async fn assert_next_message_checkpoint(&mut self, expected_epoch: u32) {
        match self.data_recv.recv().await {
            Some(item) => {
                if let ArrowMessage::Signal(SignalMessage::Barrier(barrier)) = item {
                    assert_eq!(expected_epoch, barrier.epoch);
                } else {
                    unreachable!("expected a record, got {:?}", item);
                }
            }
            None => unreachable!("option shouldn't be missing"),
        }
    }

    async fn assert_control_checkpoint(&mut self, expected_epoch: u32) -> CheckpointCompleted {
        loop {
            let control_response = self
                .from_control_rx
                .recv()
                .await
                .expect("should be a valid message");

            if let ControlResp::CheckpointCompleted(checkpoint) = control_response {
                assert_eq!(expected_epoch, checkpoint.checkpoint_epoch);
                return checkpoint;
            }
        }
    }
}

#[tokio::test]
async fn test_kafka() {
    let kafka_topic_tester = KafkaTopicTester {
        topic: "__arroyo-source-test".to_string(),
        server: "0.0.0.0:9092".to_string(),
        group_id: Some("test-consumer-group".to_string()),
    };

    let mut task_info = arroyo_types::get_test_task_info();
    task_info.job_id = format!("kafka-job-{}", random::<u64>());

    kafka_topic_tester.create_topic().await;
    let mut reader = kafka_topic_tester
        .get_source_with_reader(task_info.clone(), None)
        .await;
    let mut producer = kafka_topic_tester.get_producer().await;

    let mut expected = vec![];
    for message in 1u64..20 {
        let data = TestData { i: message };
        expected.push(serde_json::to_string(&data).unwrap());
        producer.send_data(data).await;
    }

    reader
        .assert_next_message_record_values(expected.into())
        .await;

    let barrier = ControlMessage::Checkpoint(CheckpointBarrier {
        epoch: 1,
        min_epoch: 0,
        timestamp: SystemTime::now(),
        then_stop: false,
    });
    reader.to_control_tx.send(barrier).await.unwrap();
    let checkpoint_completed = reader.assert_control_checkpoint(1).await;
    producer.send_data(TestData { i: 20 }).await;

    reader.assert_next_message_checkpoint(1).await;
    let subtask_metadata = checkpoint_completed.subtask_metadata;
    let table_metadata = GlobalKeyedTable::merge_checkpoint_metadata(
        subtask_metadata.table_configs.get("k").unwrap().clone(),
        single_item_hash_map(
            0u32,
            subtask_metadata.table_metadata.get("k").unwrap().clone(),
        ),
    )
    .unwrap()
    .unwrap();

    StateBackend::write_operator_checkpoint_metadata(OperatorCheckpointMetadata {
        start_time: 0,
        finish_time: 0,
        table_checkpoint_metadata: single_item_hash_map("k", table_metadata),
        table_configs: subtask_metadata.table_configs,
        operator_metadata: Some(OperatorMetadata {
            job_id: task_info.job_id.clone(),
            operator_id: task_info.operator_id.clone(),
            epoch: 1,
            min_watermark: Some(0),
            max_watermark: Some(0),
            parallelism: 1,
        }),
    })
    .await
    .unwrap();

    StateBackend::write_checkpoint_metadata(CheckpointMetadata {
        job_id: task_info.job_id.clone(),
        epoch: 1,
        min_epoch: 1,
        start_time: 0,
        finish_time: 0,
        operator_ids: vec![task_info.operator_id.clone()],
    })
    .await
    .unwrap();

    reader
        .assert_next_message_record_values(
            vec![serde_json::to_string(&TestData { i: 20 }).unwrap()].into(),
        )
        .await;

    reader
        .to_control_tx
        .send(ControlMessage::Stop {
            mode: arroyo_rpc::grpc::rpc::StopMode::Graceful,
        })
        .await
        .unwrap();

    let mut reader = kafka_topic_tester
        .get_source_with_reader(task_info, Some(1))
        .await;

    reader
        .assert_next_message_record_values(
            vec![serde_json::to_string(&TestData { i: 20 }).unwrap()].into(),
        )
        .await;

    producer.send_data(TestData { i: 21 }).await;
    reader
        .assert_next_message_record_values(
            vec![serde_json::to_string(&TestData { i: 21 }).unwrap()].into(),
        )
        .await;
}

#[tokio::test]
async fn test_kafka_with_metadata_fields() {
    let kafka_topic_tester = KafkaTopicTester {
        topic: "__arroyo-source-test_metadata".to_string(),
        server: "0.0.0.0:9092".to_string(),
        group_id: Some("test-consumer-group".to_string()),
    };

    let mut task_info = arroyo_types::get_test_task_info();
    task_info.job_id = format!("kafka-job-{}", random::<u64>());
    let task_info = Arc::new(task_info);

    kafka_topic_tester.create_topic().await;

    let metadata_fields = vec![MetadataField {
        field_name: "offset".to_string(),
        key: "offset_id".to_string(),
        data_type: Some(UInt64),
    }];

    let mut kafka = KafkaSourceFunc {
        bootstrap_servers: kafka_topic_tester.server.clone(),
        topic: kafka_topic_tester.topic.clone(),
        group_id: kafka_topic_tester.group_id.clone(),
        group_id_prefix: None,
        offset_mode: SourceOffset::Earliest,
        format: Format::RawString(RawStringFormat {}),
        framing: None,
        bad_data: None,
        schema_resolver: None,
        client_configs: HashMap::new(),
        context: Context::new(Some(test_config(&kafka_topic_tester.server))),
        messages_per_second: NonZeroU32::new(100).unwrap(),
        metadata_fields,
    };

    let (_to_control_tx, control_rx) = channel(128);
    let (command_tx, _from_control_rx) = channel(128);
    let (data_tx, _recv) = batch_bounded(128);

    let checkpoint_metadata = None;

    let ctx = OperatorContext::new(
        task_info.clone(),
        checkpoint_metadata.as_ref(),
        command_tx.clone(),
        1,
        vec![],
        Some(Arc::new(ArroyoSchema::new_unkeyed(
            Arc::new(Schema::new(vec![
                Field::new(
                    "_timestamp",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("value", DataType::Utf8, false),
                Field::new("offset", DataType::Int64, false),
            ])),
            0,
        ))),
        kafka.tables(),
    )
    .await;

    let chain_info = Arc::new(ChainInfo {
        job_id: ctx.task_info.job_id.clone(),
        task_id: ctx.task_info.operator_idx,
        description: "kafka source".to_string(),
        task_index: ctx.task_info.task_index,
    });

    let mut ctx = SourceContext::from_operator(ctx, chain_info.clone(), control_rx);
    let arrow_collector = ArrowCollector::new(
        chain_info.clone(),
        Some(ctx.out_schema.clone()),
        vec![vec![data_tx]],
    );
    let mut collector = SourceCollector::new(
        ctx.out_schema.clone(),
        arrow_collector,
        command_tx,
        &task_info,
    );

    tokio::spawn(async move {
        kafka.run(&mut ctx, &mut collector).await.unwrap();
    });

    let mut reader = kafka_topic_tester
        .get_source_with_reader((*task_info).clone(), None)
        .await;
    let mut producer = kafka_topic_tester.get_producer().await;

    let mut expected_messages = Vec::new();
    for i in 1u64..=21 {
        let data = TestData { i };
        producer.send_data(data.clone()).await;
        expected_messages.push(serde_json::to_string(&data).unwrap());
    }

    reader
        .assert_next_message_record_values(expected_messages.into())
        .await;

    reader
        .to_control_tx
        .send(ControlMessage::Stop {
            mode: arroyo_rpc::grpc::rpc::StopMode::Graceful,
        })
        .await
        .unwrap();
}
