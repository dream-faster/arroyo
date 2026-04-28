use arrow::compute::concat_batches;
use arrow_array::builder::TimestampNanosecondBuilder;
use arrow_array::{
    Array, ArrayRef, RecordBatch, TimestampNanosecondArray, UInt32Array, new_null_array,
};
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::{
    ArrowOperator, AsDisplayable, ConstructedOperator, DisplayableOperator, OperatorConstructor,
    Registry,
};
use arroyo_rpc::Converter;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::{api, rpc::TableConfig};
use arroyo_state::timestamp_table_config;
use arroyo_types::{CheckpointBarrier, Watermark, from_nanos};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub struct AsofJoin {
    ttl: Duration,
    left_input_schema: ArroyoSchema,
    right_input_schema: ArroyoSchema,
    left_unkeyed_schema: ArroyoSchema,
    right_unkeyed_schema: ArroyoSchema,
    output_schema: ArroyoSchema,
    key_converter: Converter,
    left_time_index: usize,
    right_time_index: usize,
    left_gte_right: bool,
}

impl AsofJoin {
    async fn emit_left_batch(
        &mut self,
        record_batch: RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let right_table = ctx
            .table_manager
            .get_key_time_table("right", ctx.last_present_watermark())
            .await?;

        let sorted_left = self.left_input_schema.sort(record_batch, false)?;
        let left_unkeyed = self.left_input_schema.unkeyed_batch(&sorted_left)?;
        let left_times = left_unkeyed
            .column(self.left_time_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ASOF join time column should be a timestamp");

        let mut right_batches: Vec<RecordBatch> = vec![];
        let mut matched_indices = vec![None; left_unkeyed.num_rows()];
        let key_fields = self.left_input_schema.storage_keys().cloned();

        for range in self.left_input_schema.partition(&sorted_left, false)? {
            let key_columns = key_fields
                .as_ref()
                .map(|fields| sorted_left.slice(range.start, 1).project(fields))
                .transpose()?
                .map(|batch| batch.columns().to_vec())
                .unwrap_or_default();
            let key_row = self.key_converter.convert_columns(&key_columns)?;

            let Some(batch) = right_table.get_batch(key_row.as_ref())?.cloned() else {
                continue;
            };

            let sorted_right = sort_by_timestamp(batch, self.right_time_index)?;
            let offset = right_batches
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>() as u32;
            let right_times = sorted_right
                .column(self.right_time_index)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("ASOF join time column should be a timestamp");

            for index in range.start..range.end {
                let left_time = left_times.value(index);
                if let Some(local_index) = find_match(right_times, left_time, self.left_gte_right) {
                    matched_indices[index] = Some(offset + local_index as u32);
                }
            }

            right_batches.push(sorted_right);
        }

        let right_columns = if right_batches.is_empty() {
            self.right_unkeyed_schema
                .schema
                .fields()
                .iter()
                .map(|field| new_null_array(field.data_type(), left_unkeyed.num_rows()))
                .collect()
        } else {
            let right_batch =
                concat_batches(&self.right_unkeyed_schema.schema, right_batches.iter())?;
            let indices = UInt32Array::from(matched_indices);
            right_batch
                .columns()
                .iter()
                .map(|column| arrow::compute::take(column.as_ref(), &indices, None))
                .collect::<Result<Vec<_>, _>>()?
        };

        let mut output_columns: Vec<ArrayRef> = vec![];
        for (index, column) in left_unkeyed.columns().iter().enumerate() {
            if index != self.left_unkeyed_schema.timestamp_index {
                output_columns.push(column.clone());
            }
        }
        for (index, column) in right_columns.iter().enumerate() {
            if index != self.right_unkeyed_schema.timestamp_index {
                output_columns.push(column.clone());
            }
        }

        let right_output_times = right_columns[self.right_unkeyed_schema.timestamp_index]
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("right output timestamp should be a timestamp");
        let left_output_times = left_unkeyed
            .column(self.left_unkeyed_schema.timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("left output timestamp should be a timestamp");

        let mut builder = TimestampNanosecondBuilder::with_capacity(left_unkeyed.num_rows());
        for index in 0..left_unkeyed.num_rows() {
            let left_time = left_output_times.value(index);
            if right_output_times.is_null(index) {
                builder.append_value(left_time);
            } else {
                let right_time = right_output_times.value(index);
                builder.append_value(left_time.max(right_time));
            }
        }
        output_columns.push(Arc::new(builder.finish()));

        if left_unkeyed.num_rows() > 0 {
            collector
                .collect(RecordBatch::try_new(
                    self.output_schema.schema.clone(),
                    output_columns,
                )?)
                .await?;
        }

        Ok(())
    }

    async fn process_right(
        &mut self,
        record_batch: RecordBatch,
        ctx: &mut OperatorContext,
    ) -> DataflowResult<()> {
        ctx.table_manager
            .get_key_time_table("right", ctx.last_present_watermark())
            .await?
            .insert(record_batch)
            .await?;
        Ok(())
    }

    async fn process_left(
        &mut self,
        record_batch: RecordBatch,
        ctx: &mut OperatorContext,
    ) -> DataflowResult<()> {
        let time_column = record_batch
            .column(self.left_input_schema.timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("left timestamp should be a timestamp");
        let max_timestamp = (0..time_column.len())
            .map(|index| time_column.value(index))
            .max()
            .expect("left batch should not be empty");

        ctx.table_manager
            .get_expiring_time_key_table("left", ctx.last_present_watermark())
            .await?
            .insert(from_nanos(max_timestamp as u128), record_batch);

        Ok(())
    }
}

fn sort_by_timestamp(batch: RecordBatch, timestamp_index: usize) -> DataflowResult<RecordBatch> {
    let timestamps = batch
        .column(timestamp_index)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("ASOF join time column should be a timestamp");
    let indices = arrow::compute::sort_to_indices(timestamps, None, None)?;
    let columns = batch
        .columns()
        .iter()
        .map(|column| arrow::compute::take(column.as_ref(), &indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

fn find_match(
    right_times: &TimestampNanosecondArray,
    left_time: i64,
    left_gte_right: bool,
) -> Option<usize> {
    if right_times.is_empty() {
        return None;
    }

    if left_gte_right {
        let mut low = 0usize;
        let mut high = right_times.len();
        while low < high {
            let mid = (low + high) / 2;
            if right_times.value(mid) <= left_time {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        low.checked_sub(1)
    } else {
        let mut low = 0usize;
        let mut high = right_times.len();
        while low < high {
            let mid = (low + high) / 2;
            if right_times.value(mid) < left_time {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        (low < right_times.len()).then_some(low)
    }
}

#[async_trait::async_trait]
impl ArrowOperator for AsofJoin {
    fn name(&self) -> String {
        "AsofJoin".to_string()
    }

    fn display(&self) -> DisplayableOperator<'_> {
        DisplayableOperator {
            name: Cow::Borrowed("AsofJoin"),
            fields: vec![
                ("ttl", AsDisplayable::Debug(&self.ttl)),
                (
                    "left_time_index",
                    AsDisplayable::Debug(&self.left_time_index),
                ),
                (
                    "right_time_index",
                    AsDisplayable::Debug(&self.right_time_index),
                ),
                ("left_gte_right", AsDisplayable::Debug(&self.left_gte_right)),
            ],
        }
    }

    async fn process_batch(
        &mut self,
        _record_batch: RecordBatch,
        _ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        unreachable!();
    }

    async fn process_batch_index(
        &mut self,
        index: usize,
        total_inputs: usize,
        record_batch: RecordBatch,
        ctx: &mut OperatorContext,
        _collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        match index / (total_inputs / 2) {
            0 => self.process_left(record_batch, ctx).await?,
            1 => self.process_right(record_batch, ctx).await?,
            _ => unreachable!(),
        }
        Ok(())
    }

    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<Option<Watermark>> {
        let Some(current_watermark) = ctx.last_present_watermark() else {
            return Ok(Some(watermark));
        };

        loop {
            let expired_batches = {
                let left_table = ctx
                    .table_manager
                    .get_expiring_time_key_table("left", Some(current_watermark))
                    .await?;

                let Some(timestamp) = left_table.get_min_time() else {
                    break;
                };

                if timestamp >= current_watermark {
                    break;
                }

                left_table.expire_timestamp(timestamp)
            };

            for batch in expired_batches {
                self.emit_left_batch(batch, ctx, collector).await?;
            }
        }

        Ok(Some(Watermark::EventTime(current_watermark)))
    }

    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let watermark = ctx.last_present_watermark();
        ctx.table_manager
            .get_expiring_time_key_table("left", watermark)
            .await?
            .flush(watermark)
            .await?;
        Ok(())
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        let mut tables = HashMap::new();
        tables.insert(
            "left".to_string(),
            timestamp_table_config(
                "left",
                "left asof join data",
                Duration::ZERO,
                false,
                self.left_input_schema.clone(),
            ),
        );
        tables.insert(
            "right".to_string(),
            timestamp_table_config(
                "right",
                "right asof join data",
                self.ttl,
                false,
                self.right_input_schema.clone(),
            ),
        );
        tables
    }
}

pub struct AsofJoinConstructor;

impl OperatorConstructor for AsofJoinConstructor {
    type ConfigT = api::JoinOperator;

    fn with_config(
        &self,
        config: Self::ConfigT,
        _registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        let left_input_schema: ArroyoSchema = config.left_schema.unwrap().try_into()?;
        let right_input_schema: ArroyoSchema = config.right_schema.unwrap().try_into()?;
        let output_schema: ArroyoSchema = config.output_schema.unwrap().try_into()?;
        let ttl = Duration::from_micros(config.ttl_micros.unwrap_or(24 * 60 * 60 * 1_000_000));
        let left_unkeyed_schema = left_input_schema.schema_without_keys()?;
        let right_unkeyed_schema = right_input_schema.schema_without_keys()?;

        Ok(ConstructedOperator::from_operator(Box::new(AsofJoin {
            ttl,
            key_converter: left_input_schema.converter(false)?,
            left_input_schema,
            right_input_schema,
            left_unkeyed_schema,
            right_unkeyed_schema,
            output_schema,
            left_time_index: config
                .asof_left_time_index
                .expect("ASOF join must set left time index") as usize,
            right_time_index: config
                .asof_right_time_index
                .expect("ASOF join must set right time index")
                as usize,
            left_gte_right: config
                .asof_left_gte_right
                .expect("ASOF join must set comparison direction"),
        })))
    }
}
