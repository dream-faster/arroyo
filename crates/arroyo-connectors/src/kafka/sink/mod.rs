use anyhow::{Result, bail};
use std::borrow::Cow;

use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::rpc::{GlobalKeyedTableConfig, TableConfig, TableEnum};
use arroyo_rpc::{CheckpointEvent, ControlResp};
use arroyo_types::*;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use tracing::warn;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, TimeUnit};
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::{ArrowOperator, AsDisplayable, DisplayableOperator};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_types::CheckpointBarrier;
use async_trait::async_trait;
use prost::Message;
use rskafka::client::partition::PartitionClient;
use std::sync::Arc;
use std::time::SystemTime;

use super::client_utils::{
    build_client, kafka_record, partition_client, produce_records, topic_metadata,
};
use super::{Context, SinkCommitMode};

#[cfg(test)]
mod test;

pub struct KafkaSinkFunc {
    pub topic: String,
    pub bootstrap_servers: String,
    pub consistency_mode: ConsistencyMode,
    pub timestamp_field: Option<String>,
    pub timestamp_col: Option<usize>,
    pub key_field: Option<String>,
    pub key_col: Option<usize>,
    pub producer: Option<KafkaProducer>,
    pub client_config: HashMap<String, String>,
    pub context: Context,
    pub serializer: ArrowSerializer,
}

#[derive(Debug)]
pub struct KafkaProducer {
    partition_ids: Vec<i32>,
    partition_clients: HashMap<i32, Arc<PartitionClient>>,
    next_partition: usize,
}

pub enum ConsistencyMode {
    AtLeastOnce,
    ExactlyOnce,
}

impl Display for ConsistencyMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsistencyMode::AtLeastOnce => write!(f, "AtLeastOnce"),
            ConsistencyMode::ExactlyOnce => write!(f, "ExactlyOnce"),
        }
    }
}

impl From<SinkCommitMode> for ConsistencyMode {
    fn from(commit_mode: SinkCommitMode) -> Self {
        match commit_mode {
            SinkCommitMode::AtLeastOnce => ConsistencyMode::AtLeastOnce,
            SinkCommitMode::ExactlyOnce => ConsistencyMode::ExactlyOnce,
        }
    }
}

impl KafkaSinkFunc {
    fn set_timestamp_col(&mut self, schema: &ArroyoSchema) {
        if let Some(field_name) = &self.timestamp_field {
            if let Ok(field) = schema.schema.field_with_name(field_name) {
                match field.data_type() {
                    DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                        self.timestamp_col = Some(schema.schema.index_of(field.name()).unwrap());
                        return;
                    }
                    _ => {
                        warn!(
                            "Kafka sink configured with timestamp_field '{field_name}', but it has type {}, not TIMESTAMP... ignoring",
                            field.data_type()
                        );
                    }
                }
            } else {
                warn!(
                    "Kafka sink configured with timestamp_field '{field_name}', but that does not appear in the schema... ignoring"
                );
            }
        }

        self.timestamp_col = Some(schema.timestamp_index);
    }

    fn set_key_col(&mut self, schema: &ArroyoSchema) {
        if let Some(field_name) = &self.key_field {
            if let Ok(field) = schema.schema.field_with_name(field_name) {
                if matches!(field.data_type(), DataType::Utf8) {
                    self.key_col = Some(schema.schema.index_of(field.name()).unwrap());
                } else {
                    warn!(
                        "Kafka sink configured with key_field '{field_name}', but it has type {}, not TEXT... ignoring",
                        field.data_type()
                    );
                }
            } else {
                warn!(
                    "Kafka sink configured with key_field '{field_name}', but that does not appear in the schema... ignoring"
                );
            }
        }
    }

    async fn init_producer(&mut self, task_info: &TaskInfo) -> Result<()> {
        if matches!(self.consistency_mode, ConsistencyMode::ExactlyOnce) {
            bail!(
                "Kafka sinks configured with sink.commit_mode='exactly_once' are not supported by the rskafka client"
            );
        }

        let client =
            build_client(&self.bootstrap_servers, &self.client_config, &self.context).await?;
        let metadata = topic_metadata(&client, &self.topic)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Kafka topic '{}' does not exist", self.topic))?;
        let mut partitions = metadata.partitions;
        partitions.sort_unstable();

        let mut partition_ids = Vec::with_capacity(partitions.len());
        let mut partition_clients = HashMap::with_capacity(partitions.len());
        for partition in partitions {
            partition_ids.push(partition);
            partition_clients.insert(
                partition,
                partition_client(&client, &self.topic, partition).await?,
            );
        }
        if partition_ids.is_empty() {
            bail!("Kafka topic '{}' has no partitions", self.topic);
        }

        self.producer = Some(KafkaProducer {
            partition_ids,
            partition_clients,
            next_partition: task_info.task_index as usize,
        });
        Ok(())
    }

    async fn flush(&mut self, _ctx: &mut OperatorContext) {}

    async fn publish_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
    ) -> DataflowResult<()> {
        let values = self.serializer.serialize(&batch);
        let timestamps = batch
            .column(
                self.timestamp_col
                    .expect("timestamp column not initialized!"),
            )
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>();
        let keys = self
            .key_col
            .map(|index| batch.column(index).as_string::<i32>());

        let producer = self
            .producer
            .as_mut()
            .expect("Kafka producer not initialized");
        let mut by_partition: HashMap<i32, Vec<rskafka::record::Record>> = HashMap::new();

        for (index, value) in values.enumerate() {
            let timestamp = timestamps.map(|ts| {
                if ts.is_null(index) {
                    0
                } else {
                    ts.value(index) / 1_000_000
                }
            });
            let key = keys.map(|column| column.value(index).as_bytes().to_vec());
            let partition = producer.select_partition(key.as_deref());
            by_partition
                .entry(partition)
                .or_default()
                .push(kafka_record(key, value, timestamp.unwrap_or(0)));
        }

        for (partition, records) in by_partition {
            if let Err(error) = produce_records(producer.partition_client(partition), records).await
            {
                ctx.error_reporter
                    .report_error("Could not write to Kafka", error.to_string())
                    .await;
                panic!("Failed to write to Kafka: {error}");
            }
        }

        Ok(())
    }
}

impl KafkaProducer {
    fn select_partition(&mut self, key: Option<&[u8]>) -> i32 {
        match key {
            Some(key) => self.partition_ids[kafka_partition_index(key, self.partition_ids.len())],
            None => self.next_round_robin_partition(),
        }
    }

    fn next_round_robin_partition(&mut self) -> i32 {
        let partition = self.partition_ids[self.next_partition % self.partition_ids.len()];
        self.next_partition = (self.next_partition + 1) % self.partition_ids.len();
        partition
    }

    fn partition_client(&self, partition: i32) -> &Arc<PartitionClient> {
        self.partition_clients
            .get(&partition)
            .expect("selected Kafka partition is not initialized")
    }
}

fn kafka_partition_index(key: &[u8], partition_count: usize) -> usize {
    (to_positive(kafka_murmur2(key)) as usize) % partition_count
}

fn to_positive(value: u32) -> u32 {
    value & 0x7fff_ffff
}

fn kafka_murmur2(data: &[u8]) -> u32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    let len = data.len() as u32;
    let mut h = SEED ^ len;
    let mut i = 0;

    while i + 4 <= data.len() {
        let mut k = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
        i += 4;
    }

    match data.len() - i {
        3 => {
            h ^= (data[i + 2] as u32) << 16;
            h ^= (data[i + 1] as u32) << 8;
            h ^= data[i] as u32;
            h = h.wrapping_mul(M);
        }
        2 => {
            h ^= (data[i + 1] as u32) << 8;
            h ^= data[i] as u32;
            h = h.wrapping_mul(M);
        }
        1 => {
            h ^= data[i] as u32;
            h = h.wrapping_mul(M);
        }
        _ => {}
    }

    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h
}

#[async_trait]
impl ArrowOperator for KafkaSinkFunc {
    fn name(&self) -> String {
        format!("kafka-producer-{}", self.topic)
    }

    fn display(&self) -> DisplayableOperator<'_> {
        DisplayableOperator {
            name: Cow::Borrowed("KafkaSinkFunc"),
            fields: vec![
                ("topic", self.topic.as_str().into()),
                ("bootstrap_servers", self.bootstrap_servers.as_str().into()),
                (
                    "consistency_mode",
                    AsDisplayable::Display(&self.consistency_mode),
                ),
                (
                    "timestamp_field",
                    AsDisplayable::Debug(&self.timestamp_field),
                ),
                ("key_field", AsDisplayable::Debug(&self.key_field)),
                ("client_config", AsDisplayable::Debug(&self.client_config)),
            ],
        }
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        if self.is_committing() {
            single_item_hash_map(
                "i".to_string(),
                TableConfig {
                    table_type: TableEnum::GlobalKeyValue.into(),
                    config: GlobalKeyedTableConfig {
                        table_name: "i".to_string(),
                        description: "index for transactional ids".to_string(),
                        uses_two_phase_commit: true,
                    }
                    .encode_to_vec(),
                    state_version: 0,
                },
            )
        } else {
            HashMap::new()
        }
    }

    fn is_committing(&self) -> bool {
        matches!(self.consistency_mode, ConsistencyMode::ExactlyOnce)
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        self.set_timestamp_col(&ctx.in_schemas[0]);
        self.set_key_col(&ctx.in_schemas[0]);
        self.init_producer(&ctx.task_info)
            .await
            .expect("Producer creation failed");
        Ok(())
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.publish_batch(batch, ctx).await
    }

    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.flush(ctx).await;
        Ok(())
    }

    async fn handle_commit(
        &mut self,
        epoch: u32,
        _commit_data: &HashMap<String, HashMap<u32, Vec<u8>>>,
        ctx: &mut OperatorContext,
    ) -> DataflowResult<()> {
        if !self.is_committing() {
            warn!("received commit but consistency mode is not exactly once");
            return Ok(());
        }

        let checkpoint_event = ControlResp::CheckpointEvent(CheckpointEvent {
            checkpoint_epoch: epoch,
            operator_idx: ctx.task_info.operator_idx,
            operator_id: ctx.task_info.operator_id.clone(),
            subtask_idx: ctx.task_info.task_index,
            time: SystemTime::now(),
            event_type: arroyo_rpc::grpc::rpc::TaskCheckpointEventType::FinishedCommit,
        });
        ctx.control_tx
            .send(checkpoint_event)
            .await
            .expect("sent commit event");
        Ok(())
    }

    async fn on_close(
        &mut self,
        _: &Option<SignalMessage>,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.flush(ctx).await;
        Ok(())
    }
}
