use anyhow::{Context as _, bail};
use async_trait::async_trait;
use bincode::{Decode, Encode};
use governor::{Quota, RateLimiter as GovernorRateLimiter};
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};

use arroyo_formats::de::FieldValueType;
use arroyo_operator::SourceFinishType;
use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_rpc::schema_resolver::SchemaResolver;
use arroyo_rpc::{ControlMessage, MetadataField, grpc::rpc::StopMode};
use arroyo_types::*;

use super::client_utils::{
    build_client, partition_client, spawn_partition_consumers, start_offset, topic_metadata,
};
use super::{Context, SourceOffset};

#[cfg(test)]
mod test;

pub struct KafkaSourceFunc {
    pub topic: String,
    pub bootstrap_servers: String,
    pub group_id: Option<String>,
    pub group_id_prefix: Option<String>,
    pub offset_mode: SourceOffset,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub schema_resolver: Option<Arc<dyn SchemaResolver + Sync>>,
    pub client_configs: HashMap<String, String>,
    pub context: Context,
    pub messages_per_second: NonZeroU32,
    pub metadata_fields: Vec<MetadataField>,
}

#[derive(Copy, Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub struct KafkaState {
    partition: i32,
    offset: i64,
}

impl KafkaSourceFunc {
    async fn build_partition_consumers(
        &self,
        ctx: &mut SourceContext,
    ) -> anyhow::Result<(
        tokio::sync::mpsc::Receiver<anyhow::Result<super::client_utils::ConsumedRecord>>,
        Vec<tokio::task::JoinHandle<()>>,
    )> {
        info!("Creating kafka consumer for {}", self.bootstrap_servers);

        if self.offset_mode == SourceOffset::Group {
            bail!(
                "Kafka sources configured with source.offset='group' are not supported by the rskafka client"
            );
        }
        if self.group_id.is_some() || self.group_id_prefix.is_some() {
            warn!(
                "Kafka source group_id/group_id_prefix are ignored with the rskafka client because Arroyo manages partition assignment and offsets itself"
            );
        }

        let client = build_client(&self.bootstrap_servers, &self.client_configs, &self.context)
            .await
            .context("creating Kafka client")?;
        let metadata = topic_metadata(&client, &self.topic)
            .await
            .context("reading Kafka topic metadata")?
            .ok_or_else(|| anyhow::anyhow!("Kafka topic '{}' does not exist", self.topic))?;

        let state: Vec<_> = ctx
            .table_manager
            .get_global_keyed_state::<i32, KafkaState>("k")
            .await?
            .get_all()
            .values()
            .collect();

        let has_state = !state.is_empty();
        let state: HashMap<i32, KafkaState> = state.iter().map(|s| (s.partition, **s)).collect();

        let our_partitions: Vec<_> = metadata
            .partitions
            .iter()
            .enumerate()
            .filter(|(index, _)| {
                index % ctx.task_info.parallelism as usize == ctx.task_info.task_index as usize
            })
            .map(|(_, partition)| *partition)
            .collect();

        info!(
            "partition map for {}-{}: {:?}",
            self.topic, ctx.task_info.task_index, our_partitions
        );

        let mut consumers = Vec::with_capacity(our_partitions.len());
        for partition in our_partitions {
            let start_offset = match state.get(&partition) {
                Some(restored_state) => {
                    start_offset(self.offset_mode, Some(restored_state.offset))?
                }
                None if has_state => {
                    super::client_utils::start_offset(SourceOffset::Earliest, None)?
                }
                None => start_offset(self.offset_mode, None)?,
            };
            consumers.push((
                partition,
                partition_client(&client, &self.topic, partition)
                    .await
                    .with_context(|| format!("creating partition client for {}", partition))?,
                start_offset,
            ));
        }

        Ok(spawn_partition_consumers(&self.topic, consumers))
    }

    async fn run_int(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> DataflowResult<SourceFinishType> {
        let (mut records_rx, consumer_handles) = self
            .build_partition_consumers(ctx)
            .await
            .context("creating kafka consumer")?;

        let rate_limiter = GovernorRateLimiter::direct(Quota::per_second(self.messages_per_second));
        let mut offsets = HashMap::new();

        if consumer_handles.is_empty() {
            warn!(
                "Kafka Consumer {}-{} is subscribed to no partitions, as there are more subtasks than partitions... setting idle",
                ctx.task_info.operator_id, ctx.task_info.task_index
            );
            collector
                .broadcast(SignalMessage::Watermark(Watermark::Idle))
                .await;
        }

        if let Some(schema_resolver) = &self.schema_resolver {
            collector.initialize_deserializer_with_resolver(
                self.format.clone(),
                self.framing.clone(),
                self.bad_data.clone(),
                &self.metadata_fields,
                schema_resolver.clone(),
            );
        } else {
            collector.initialize_deserializer(
                self.format.clone(),
                self.framing.clone(),
                self.bad_data.clone(),
                &self.metadata_fields,
            );
        }

        let mut flush_ticker = tokio::time::interval(Duration::from_millis(50));
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let finish = loop {
            select! {
                message = records_rx.recv() => {
                    match message {
                        Some(Ok(msg)) => {
                            if let Some(value) = msg.record.record.value.as_ref() {
                                let timestamp = msg.record.record.timestamp.timestamp_millis();

                                let connector_metadata = if !self.metadata_fields.is_empty() {
                                    let mut connector_metadata = HashMap::new();
                                    for field in &self.metadata_fields {
                                        connector_metadata.insert(field.field_name.as_str(), match field.key.as_str() {
                                            "key" => FieldValueType::Bytes(msg.record.record.key.as_deref()),
                                            "offset_id" => FieldValueType::Int64(Some(msg.record.offset)),
                                            "partition" => FieldValueType::Int32(Some(msg.partition)),
                                            "topic" => FieldValueType::String(Some(self.topic.as_str())),
                                            "timestamp" => FieldValueType::Int64(Some(timestamp)),
                                            key => unreachable!("Invalid metadata key '{}'", key),
                                        });
                                    }
                                    Some(connector_metadata)
                                } else {
                                    None
                                };

                                collector
                                    .deserialize_slice(
                                        value,
                                        from_millis(timestamp.max(0) as u64),
                                        connector_metadata.as_ref(),
                                    )
                                    .await?;

                                if collector.should_flush() {
                                    collector.flush_buffer().await?;
                                }

                                offsets.insert(msg.partition, msg.record.offset);
                                rate_limiter.until_ready().await;
                            }
                        }
                        Some(Err(err)) => {
                            error!("encountered error {}", err);
                        }
                        None => {}
                    }
                }
                _ = flush_ticker.tick() => {
                    if collector.should_flush() {
                        collector.flush_buffer().await?;
                    }
                }
                control_message = ctx.control_rx.recv() => {
                    match control_message {
                        Some(ControlMessage::Checkpoint(c)) => {
                            debug!("starting checkpointing {}", ctx.task_info.task_index);
                            let state = ctx.table_manager.get_global_keyed_state("k").await?;
                            for (partition, offset) in &offsets {
                                state.insert(*partition, KafkaState {
                                    partition: *partition,
                                    offset: *offset + 1,
                                }).await;
                            }

                            if self.start_checkpoint(c, ctx, collector).await {
                                break SourceFinishType::Immediate;
                            }
                        },
                        Some(ControlMessage::Stop { mode }) => {
                            info!("Stopping kafka source: {:?}", mode);

                            break match mode {
                                StopMode::Graceful => SourceFinishType::Graceful,
                                StopMode::Immediate => SourceFinishType::Immediate,
                            };
                        }
                        Some(ControlMessage::Commit { .. }) => {
                            unreachable!("sources shouldn't receive commit messages");
                        }
                        Some(ControlMessage::LoadCompacted {compacted}) => {
                            ctx.load_compacted(compacted).await;
                        }
                        Some(ControlMessage::NoOp) => {}
                        None => {}
                    }
                }
            }
        };

        for handle in consumer_handles {
            handle.abort();
        }

        Ok(finish)
    }
}

#[async_trait]
impl SourceOperator for KafkaSourceFunc {
    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> DataflowResult<SourceFinishType> {
        self.run_int(ctx, collector).await
    }

    fn name(&self) -> String {
        format!("kafka-{}", self.topic)
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config("k", "kafka offsets")
    }
}
