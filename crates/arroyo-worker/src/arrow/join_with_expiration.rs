use anyhow::anyhow;
use arrow::compute::{concat_batches, partition, sort_to_indices, take};
use arrow::row::{RowConverter, SortField};
use arrow_array::cast::AsArray;
use arrow_array::types::TimestampNanosecondType;
use arrow_array::{Array, RecordBatch};
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::{
    ArrowOperator, AsDisplayable, ConstructedOperator, DisplayableOperator, OperatorConstructor,
    Registry,
};
use arroyo_planner::physical::{ArroyoPhysicalExtensionCodec, DecodingContext};
use arroyo_rpc::{
    Converter,
    df::ArroyoSchema,
    errors::DataflowResult,
    grpc::{api, rpc::TableConfig},
};
use arroyo_state::timestamp_table_config;
use arroyo_types::{Watermark, from_nanos};
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use futures::StreamExt;
use prost::Message;
use std::borrow::Cow;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};
use tracing::warn;

/// Indices (in the *unkeyed* left/right schemas) of the timestamp columns used
/// to enforce ASOF semantics: a left row matches the single most recent right
/// row whose key matches and whose timestamp is `<=` the left row's timestamp.
#[derive(Copy, Clone, Debug)]
struct AsofConfig {
    left_ts_index: usize,
    right_ts_index: usize,
    inequality: api::AsofInequality,
    left_outer: bool,
}

pub struct JoinWithExpiration {
    left_expiration: Duration,
    right_expiration: Duration,
    left_input_schema: ArroyoSchema,
    right_input_schema: ArroyoSchema,
    left_schema: ArroyoSchema,
    right_schema: ArroyoSchema,
    left_key_converter: Converter,
    left_passer: Arc<RwLock<Option<RecordBatch>>>,
    right_passer: Arc<RwLock<Option<RecordBatch>>>,
    join_execution_plan: Arc<dyn ExecutionPlan>,
    asof: Option<AsofConfig>,
    pending_left_schema: ArroyoSchema,
    task_ctx: Arc<datafusion::execution::context::TaskContext>,
}

impl JoinWithExpiration {
    async fn process_left(
        &mut self,
        record_batch: RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if self.asof.is_some() {
            return self.process_left_asof(record_batch, ctx, collector).await;
        }

        let left_table = ctx
            .table_manager
            .get_key_time_table("left", ctx.last_present_watermark())
            .await?;
        let left_rows = left_table.insert(record_batch.clone()).await?;
        let right_table = ctx
            .table_manager
            .get_key_time_table("right", ctx.last_present_watermark())
            .await?;

        let mut right_batches = vec![];
        for row in left_rows {
            if let Some(batch) = right_table.get_batch(row.as_ref())? {
                right_batches.push(batch.clone());
            }
        }
        let right_batch = concat_batches(&self.right_schema.schema, right_batches.iter())?;
        self.compute_pair(
            self.left_input_schema.unkeyed_batch(&record_batch)?,
            right_batch,
            collector,
        )
        .await?;

        Ok(())
    }

    async fn process_right(
        &mut self,
        right_batch: RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if self.asof.is_some() {
            return self.process_right_asof(right_batch, ctx, collector).await;
        }

        let right_table = ctx
            .table_manager
            .get_key_time_table("right", ctx.last_present_watermark())
            .await?;

        let right_rows = right_table.insert(right_batch.clone()).await?;

        let left_table = ctx
            .table_manager
            .get_key_time_table("left", ctx.last_present_watermark())
            .await?;

        let mut left_batches = vec![];
        for row in right_rows {
            if let Some(batch) = left_table.get_batch(row.as_ref())? {
                left_batches.push(batch.clone());
            }
        }
        let left_batch = concat_batches(&self.left_schema.schema, left_batches.iter())?;
        self.compute_pair(
            left_batch,
            self.right_input_schema.unkeyed_batch(&right_batch)?,
            collector,
        )
        .await?;

        Ok(())
    }

    /// ASOF semantics, left arrival: for each new left row, look up matching
    /// right rows by key, pick the single right row with the largest
    /// `right_ts <= left_ts`, and feed that pair to the inner-join physical
    /// plan.
    async fn process_left_asof(
        &mut self,
        record_batch: RecordBatch,
        ctx: &mut OperatorContext,
        _collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.insert_pending_left_batch(record_batch, ctx).await?;
        Ok(())
    }

    /// ASOF semantics, right arrival: for each new right row, look up matching
    /// left rows by key. The new right row is the ASOF match for a buffered
    /// left row iff `right_ts <= left_ts` and no other already-buffered right
    /// row falls in `(right_ts, left_ts]`.
    async fn process_right_asof(
        &mut self,
        right_batch: RecordBatch,
        ctx: &mut OperatorContext,
        _collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let right_table = ctx
            .table_manager
            .get_key_time_table("right", ctx.last_present_watermark())
            .await?;
        right_table.insert(right_batch).await?;
        Ok(())
    }

    async fn insert_pending_left_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
    ) -> DataflowResult<()> {
        let pending_left = ctx
            .table_manager
            .get_expiring_time_key_table("left_pending", ctx.last_present_watermark())
            .await?;

        let time_column = batch
            .column(self.pending_left_schema.timestamp_index)
            .as_any()
            .downcast_ref::<arrow_array::TimestampNanosecondArray>()
            .ok_or_else(|| {
                arrow::error::ArrowError::InvalidArgumentError(
                    "ASOF JOIN expected Timestamp(Nanosecond) left input".into(),
                )
            })?;

        let max_timestamp = arrow::compute::max(time_column).ok_or_else(|| {
            arrow::error::ArrowError::InvalidArgumentError(
                "ASOF JOIN left batch must have a max timestamp".into(),
            )
        })?;
        let min_timestamp = arrow::compute::min(time_column).ok_or_else(|| {
            arrow::error::ArrowError::InvalidArgumentError(
                "ASOF JOIN left batch must have a min timestamp".into(),
            )
        })?;

        if max_timestamp == min_timestamp {
            pending_left.insert(from_nanos(max_timestamp as u128), batch);
            return Ok(());
        }

        let indices = sort_to_indices(time_column, None, None)?;
        let columns = batch
            .columns()
            .iter()
            .map(|c| take(c, &indices, None))
            .collect::<Result<Vec<_>, _>>()?;
        let sorted = RecordBatch::try_new(batch.schema(), columns)?;
        let sorted_timestamps = take(time_column, &indices, None)?;
        let ranges = partition(std::slice::from_ref(&sorted_timestamps))?.ranges();
        let typed_timestamps = sorted_timestamps
            .as_any()
            .downcast_ref::<arrow_array::TimestampNanosecondArray>()
            .ok_or_else(|| {
                arrow::error::ArrowError::InvalidArgumentError(
                    "ASOF JOIN failed to read sorted left timestamps".into(),
                )
            })?;

        for range in ranges {
            let timestamp = from_nanos(typed_timestamps.value(range.start) as u128);
            pending_left.insert(
                timestamp,
                sorted.slice(range.start, range.end - range.start),
            );
        }

        Ok(())
    }

    async fn process_finalized_left_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let cfg = self.asof.expect("asof config required");
        let empty_right = RecordBatch::new_empty(self.right_schema.schema.clone());
        let right_table = ctx
            .table_manager
            .get_key_time_table("right", ctx.last_present_watermark())
            .await?;

        for i in 0..batch.num_rows() {
            let keyed_row = batch.slice(i, 1);
            let unkeyed_row = self.left_input_schema.unkeyed_batch(&keyed_row)?;
            let Some(left_ts) = ts_value(&unkeyed_row, cfg.left_ts_index)? else {
                if cfg.left_outer {
                    self.compute_pair(unkeyed_row, empty_right.clone(), collector)
                        .await?;
                }
                continue;
            };

            let key_row = storage_key(
                &keyed_row,
                &self.left_input_schema,
                &self.left_key_converter,
            )?;

            let candidates = right_table.get_batch(&key_row)?.cloned();

            let best_right = match candidates {
                Some(candidates) => {
                    pick_asof_right(&candidates, cfg.right_ts_index, cfg.inequality, left_ts)?
                        .map(|idx| candidates.slice(idx, 1))
                }
                None => None,
            };

            match best_right {
                Some(best_right) => {
                    self.compute_pair(unkeyed_row, best_right, collector)
                        .await?;
                }
                None if cfg.left_outer => {
                    self.compute_pair(unkeyed_row, empty_right.clone(), collector)
                        .await?;
                }
                None => {}
            }
        }

        Ok(())
    }

    async fn compute_pair(
        &mut self,
        left: RecordBatch,
        right: RecordBatch,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        {
            self.right_passer.write().unwrap().replace(right);
            self.left_passer.write().unwrap().replace(left);
        }
        self.join_execution_plan.reset().unwrap();
        let mut records = self
            .join_execution_plan
            .execute(0, self.task_ctx.clone())
            .expect("successfully computed?");
        while let Some(batch) = records.next().await {
            collector.collect(batch?).await?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl ArrowOperator for JoinWithExpiration {
    fn name(&self) -> String {
        "JoinWithExpiration".to_string()
    }

    fn display(&self) -> DisplayableOperator<'_> {
        DisplayableOperator {
            name: Cow::Borrowed("JoinWithExpiration"),
            fields: vec![
                (
                    "left_expiration",
                    AsDisplayable::Debug(&self.left_expiration),
                ),
                (
                    "right_expiration",
                    AsDisplayable::Debug(&self.right_expiration),
                ),
                (
                    "join_execution_plan",
                    self.join_execution_plan.as_ref().into(),
                ),
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
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        match index / (total_inputs / 2) {
            0 => self.process_left(record_batch, ctx, collector).await?,
            1 => self.process_right(record_batch, ctx, collector).await?,
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
        if self.asof.is_none() {
            return Ok(Some(watermark));
        }

        let finalize_action =
            asof_finalize_watermark_action(watermark, ctx.last_present_watermark());

        loop {
            let next_time = {
                let pending_left = ctx
                    .table_manager
                    .get_expiring_time_key_table("left_pending", ctx.last_present_watermark())
                    .await?;
                pending_left.get_min_time()
            };

            let Some(next_time) = next_time else {
                break;
            };
            match finalize_action {
                AsofFinalizeWatermarkAction::None => break,
                AsofFinalizeWatermarkAction::Through(watermark_time)
                    if next_time >= watermark_time =>
                {
                    break;
                }
                AsofFinalizeWatermarkAction::DrainAll | AsofFinalizeWatermarkAction::Through(_) => {
                }
            }

            let batches = {
                let pending_left = ctx
                    .table_manager
                    .get_expiring_time_key_table("left_pending", ctx.last_present_watermark())
                    .await?;
                pending_left.expire_timestamp(next_time)
            };

            for batch in batches {
                self.process_finalized_left_batch(batch, ctx, collector)
                    .await?;
            }
        }

        Ok(Some(watermark))
    }

    async fn handle_checkpoint(
        &mut self,
        _: arroyo_types::CheckpointBarrier,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if self.asof.is_none() {
            return Ok(());
        }

        let watermark = ctx.last_present_watermark();
        ctx.table_manager
            .get_expiring_time_key_table("left_pending", watermark)
            .await?
            .flush(watermark)
            .await?;
        Ok(())
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        let mut tables = HashMap::new();
        let left_table_name = if self.asof.is_some() {
            "left_pending"
        } else {
            "left"
        };
        let left_table_description = if self.asof.is_some() {
            "pending ASOF left join data"
        } else {
            "left join data"
        };
        tables.insert(
            left_table_name.to_string(),
            timestamp_table_config(
                left_table_name,
                left_table_description,
                self.left_expiration,
                false,
                self.pending_left_schema.clone(),
            ),
        );
        tables.insert(
            "right".to_string(),
            timestamp_table_config(
                "right",
                "right join data",
                self.right_expiration,
                false,
                self.right_input_schema.clone(),
            ),
        );
        tables
    }
}

pub struct JoinWithExpirationConstructor;
impl OperatorConstructor for JoinWithExpirationConstructor {
    type ConfigT = api::JoinOperator;
    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        let left_passer = Arc::new(RwLock::new(None));
        let right_passer = Arc::new(RwLock::new(None));

        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::LockedJoinPair {
                left: left_passer.clone(),
                right: right_passer.clone(),
            },
        };
        let join_physical_plan_node = PhysicalPlanNode::decode(&mut config.join_plan.as_slice())?;
        let join_execution_plan = join_physical_plan_node.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnvBuilder::new().build()?,
            &codec,
        )?;

        let left_input_schema: ArroyoSchema = config.left_schema.unwrap().try_into()?;
        let right_input_schema: ArroyoSchema = config.right_schema.unwrap().try_into()?;
        let left_schema = left_input_schema.schema_without_keys()?;
        let right_schema = right_input_schema.schema_without_keys()?;
        let left_key_converter = left_input_schema.converter(false)?;
        let task_ctx = SessionContext::new().task_ctx();

        let raw_ttl_micros = config
            .ttl_micros
            .expect("ttl must be set for non-instant join");
        let mut ttl = Duration::from_micros(raw_ttl_micros);

        if ttl == Duration::ZERO {
            warn!("TTL was not set for join with expiration");
            ttl = Duration::from_secs(24 * 60 * 60);
        }

        let asof = if let Some(a) = config.asof {
            Some(AsofConfig {
                left_ts_index: a.left_ts_index as usize,
                right_ts_index: a.right_ts_index as usize,
                inequality: decode_asof_inequality(a.inequality)?,
                left_outer: a.left_outer,
            })
        } else {
            None
        };

        if asof.is_some() && raw_ttl_micros == 0 {
            warn!(
                "ASOF JOIN TTL was not configured; defaulting to 24h which also bounds right-side ASOF lookback"
            );
        }

        let pending_left_schema = pending_left_schema(&left_input_schema, asof);

        Ok(ConstructedOperator::from_operator(Box::new(
            JoinWithExpiration {
                left_expiration: ttl,
                right_expiration: ttl,
                left_input_schema,
                right_input_schema,
                left_schema,
                right_schema,
                left_key_converter,
                left_passer,
                right_passer,
                join_execution_plan,
                asof,
                pending_left_schema,
                task_ctx,
            },
        )))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AsofFinalizeWatermarkAction {
    None,
    Through(std::time::SystemTime),
    DrainAll,
}

fn asof_finalize_watermark_action(
    watermark: Watermark,
    last_present_watermark: Option<std::time::SystemTime>,
) -> AsofFinalizeWatermarkAction {
    match watermark {
        Watermark::EventTime(_) => last_present_watermark
            .map(AsofFinalizeWatermarkAction::Through)
            .unwrap_or(AsofFinalizeWatermarkAction::None),
        Watermark::Idle => AsofFinalizeWatermarkAction::DrainAll,
    }
}

fn decode_asof_inequality(value: i32) -> anyhow::Result<api::AsofInequality> {
    api::AsofInequality::try_from(value)
        .map_err(|_| anyhow!("invalid JoinOperator.asof.inequality enum value {value}"))
}

fn pending_left_schema(schema: &ArroyoSchema, asof: Option<AsofConfig>) -> ArroyoSchema {
    let mut pending = schema.clone();
    if let Some(asof) = asof {
        pending.timestamp_index = asof.left_ts_index;
    }
    pending
}

/// Read the `i64` value at row 0 of the timestamp column of a single-row
/// (unkeyed) batch. ASOF joins require the timestamp column to be a
/// `Timestamp(Nanosecond)` (the canonical Arroyo event-time representation).
fn ts_value(batch: &RecordBatch, idx: usize) -> DataflowResult<Option<i64>> {
    let arr = batch.column(idx);
    let ts = arr
        .as_primitive_opt::<TimestampNanosecondType>()
        .ok_or_else(|| {
            arrow::error::ArrowError::InvalidArgumentError(format!(
                "ASOF JOIN expected a Timestamp(Nanosecond) column at index {idx}, got {:?}",
                arr.data_type()
            ))
        })?;
    if ts.is_null(0) {
        return Ok(None);
    }
    Ok(Some(ts.value(0)))
}

fn storage_key(
    batch: &RecordBatch,
    schema: &ArroyoSchema,
    converter: &Converter,
) -> DataflowResult<Vec<u8>> {
    let Some(storage_keys) = schema.storage_keys() else {
        return Ok(vec![]);
    };
    let key_columns = batch.project(storage_keys)?.columns().to_vec();
    Ok(converter.convert_columns(&key_columns)?.as_ref().to_vec())
}

/// Return the index in `candidates` of the single right row that satisfies the
/// configured ASOF inequality and best matches the left timestamp. For `>=` and
/// `>`, this is the latest qualifying right row; for `<=` and `<`, it is the
/// earliest qualifying right row. NULL timestamps are skipped.
fn pick_asof_right(
    candidates: &RecordBatch,
    right_ts_index: usize,
    inequality: api::AsofInequality,
    left_ts: i64,
) -> DataflowResult<Option<usize>> {
    let arr = candidates.column(right_ts_index);
    let ts = arr
        .as_primitive_opt::<TimestampNanosecondType>()
        .ok_or_else(|| {
            arrow::error::ArrowError::InvalidArgumentError(format!(
                "ASOF JOIN expected a Timestamp(Nanosecond) column at index {right_ts_index}, got {:?}",
                arr.data_type()
            ))
        })?;

    let row_converter = RowConverter::new(
        candidates
            .schema()
            .fields()
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect(),
    )?;
    let rows = row_converter.convert_columns(candidates.columns())?;

    let mut best: Option<(usize, i64)> = None;
    for i in 0..ts.len() {
        if ts.is_null(i) {
            continue;
        }
        let v = ts.value(i);
        let qualifies = match inequality {
            api::AsofInequality::Gte => v <= left_ts,
            api::AsofInequality::Gt => v < left_ts,
            api::AsofInequality::Lte => v >= left_ts,
            api::AsofInequality::Lt => v > left_ts,
        };
        if !qualifies {
            continue;
        }

        match (inequality, best) {
            (api::AsofInequality::Gte, Some((best_idx, b)))
            | (api::AsofInequality::Gt, Some((best_idx, b)))
                if b > v || (b == v && rows.row(best_idx).as_ref() >= rows.row(i).as_ref()) => {}
            (api::AsofInequality::Lte, Some((best_idx, b)))
            | (api::AsofInequality::Lt, Some((best_idx, b)))
                if b < v || (b == v && rows.row(best_idx).as_ref() >= rows.row(i).as_ref()) => {}
            _ => {
                // Break timestamp ties by the encoded row payload so selection is
                // deterministic across batch replay and recovery.
                best = Some((i, v));
            }
        }
    }
    Ok(best.map(|(i, _)| i))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow_array::{Int64Array, TimestampNanosecondArray};

    fn ts_batch(values: Vec<Option<i64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));
        let v = Arc::new(Int64Array::from(
            (0..values.len() as i64).collect::<Vec<_>>(),
        ));
        let t = Arc::new(TimestampNanosecondArray::from(values));
        RecordBatch::try_new(schema, vec![v, t]).unwrap()
    }

    #[test]
    fn pick_asof_right_returns_largest_le_left_ts() {
        let batch = ts_batch(vec![Some(10), Some(20), Some(30), Some(40)]);
        // left_ts = 25 → best index is 1 (value 20)
        assert_eq!(
            pick_asof_right(&batch, 1, api::AsofInequality::Gte, 25).unwrap(),
            Some(1)
        );
        // left_ts = 40 → best is the largest <= 40 (index 3)
        assert_eq!(
            pick_asof_right(&batch, 1, api::AsofInequality::Gte, 40).unwrap(),
            Some(3)
        );
        // left_ts = 5 → no candidate
        assert_eq!(
            pick_asof_right(&batch, 1, api::AsofInequality::Gte, 5).unwrap(),
            None
        );
    }

    #[test]
    fn pick_asof_right_skips_nulls() {
        let batch = ts_batch(vec![None, Some(10), None, Some(20), None]);
        assert_eq!(
            pick_asof_right(&batch, 1, api::AsofInequality::Gte, 15).unwrap(),
            Some(1)
        );
        assert_eq!(
            pick_asof_right(&batch, 1, api::AsofInequality::Gte, 25).unwrap(),
            Some(3)
        );
        // All-null candidates → None
        let null_batch = ts_batch(vec![None, None, None]);
        assert_eq!(
            pick_asof_right(&null_batch, 1, api::AsofInequality::Gte, 100).unwrap(),
            None
        );
    }

    #[test]
    fn pick_asof_right_prefers_later_row_on_ties() {
        let batch = ts_batch(vec![Some(10), Some(10), Some(5)]);
        let got = pick_asof_right(&batch, 1, api::AsofInequality::Gte, 12).unwrap();
        assert_eq!(got, Some(1));
    }

    #[test]
    fn pick_asof_right_supports_all_inequalities() {
        let batch = ts_batch(vec![Some(10), Some(20), Some(30), Some(40)]);
        assert_eq!(
            pick_asof_right(&batch, 1, api::AsofInequality::Gt, 20).unwrap(),
            Some(0)
        );
        assert_eq!(
            pick_asof_right(&batch, 1, api::AsofInequality::Lte, 25).unwrap(),
            Some(2)
        );
        assert_eq!(
            pick_asof_right(&batch, 1, api::AsofInequality::Lt, 30).unwrap(),
            Some(3)
        );
    }

    #[test]
    fn pick_asof_right_rejects_non_timestamp_column() {
        // Column 0 is Int64, not Timestamp → kernel must error out.
        let batch = ts_batch(vec![Some(1)]);
        let err = pick_asof_right(&batch, 0, api::AsofInequality::Gte, 0)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Timestamp"),
            "expected Timestamp type error, got: {err}"
        );
    }

    #[test]
    fn ts_value_reads_first_row() {
        let batch = ts_batch(vec![Some(123), Some(456)]);
        assert_eq!(ts_value(&batch, 1).unwrap(), Some(123));
    }

    #[test]
    fn ts_value_returns_none_on_null() {
        let batch = ts_batch(vec![None]);
        assert_eq!(ts_value(&batch, 1).unwrap(), None);
    }

    #[test]
    fn ts_value_errors_on_wrong_type() {
        let batch = ts_batch(vec![Some(1)]);
        let err = ts_value(&batch, 0).unwrap_err().to_string();
        assert!(
            err.contains("Timestamp"),
            "expected Timestamp type error, got: {err}"
        );
    }

    #[test]
    fn idle_watermark_drains_all_pending_left() {
        assert_eq!(
            asof_finalize_watermark_action(Watermark::Idle, Some(from_nanos(5))),
            AsofFinalizeWatermarkAction::DrainAll
        );
    }

    #[test]
    fn event_time_watermark_uses_last_present_value() {
        let watermark = from_nanos(5);
        assert_eq!(
            asof_finalize_watermark_action(Watermark::EventTime(watermark), Some(watermark)),
            AsofFinalizeWatermarkAction::Through(watermark)
        );
    }

    #[test]
    fn rejects_unknown_asof_inequality_enum() {
        let err = decode_asof_inequality(-1).unwrap_err().to_string();
        assert!(err.contains("JoinOperator.asof.inequality"), "got {err}");
    }

    #[test]
    fn pending_left_schema_uses_configured_asof_timestamp_index() {
        let mut schema = ArroyoSchema::new_unkeyed(
            Arc::new(Schema::new(vec![
                Field::new(
                    "event_ts",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
                Field::new(
                    "match_ts",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
            ])),
            0,
        );
        schema.timestamp_index = 0;

        let pending = pending_left_schema(
            &schema,
            Some(AsofConfig {
                left_ts_index: 1,
                right_ts_index: 0,
                inequality: api::AsofInequality::Gte,
                left_outer: false,
            }),
        );

        assert_eq!(pending.timestamp_index, 1);
    }
}
