use arrow::compute::concat_batches;
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
    df::ArroyoSchema,
    errors::DataflowResult,
    grpc::{api, rpc::TableConfig},
};
use arroyo_state::timestamp_table_config;
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
}

pub struct JoinWithExpiration {
    left_expiration: Duration,
    right_expiration: Duration,
    left_input_schema: ArroyoSchema,
    right_input_schema: ArroyoSchema,
    left_schema: ArroyoSchema,
    right_schema: ArroyoSchema,
    left_passer: Arc<RwLock<Option<RecordBatch>>>,
    right_passer: Arc<RwLock<Option<RecordBatch>>>,
    join_execution_plan: Arc<dyn ExecutionPlan>,
    asof: Option<AsofConfig>,
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
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let cfg = self.asof.expect("asof config required");

        for i in 0..record_batch.num_rows() {
            let keyed_row = record_batch.slice(i, 1);
            let unkeyed_row = self.left_input_schema.unkeyed_batch(&keyed_row)?;
            let left_ts = ts_value(&unkeyed_row, cfg.left_ts_index)?;

            // Insert the left row and capture the storage key for the lookup.
            let left_table = ctx
                .table_manager
                .get_key_time_table("left", ctx.last_present_watermark())
                .await?;
            let mut key_rows = left_table.insert(keyed_row).await?;
            let Some(key_row) = key_rows.pop() else {
                continue;
            };

            let right_table = ctx
                .table_manager
                .get_key_time_table("right", ctx.last_present_watermark())
                .await?;
            let Some(candidates) = right_table.get_batch(key_row.as_ref())? else {
                continue;
            };
            let candidates = candidates.clone();

            if let Some(best_idx) = pick_asof_right(&candidates, cfg.right_ts_index, left_ts)? {
                let best_right = candidates.slice(best_idx, 1);
                self.compute_pair(unkeyed_row, best_right, collector)
                    .await?;
            }
        }

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
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let cfg = self.asof.expect("asof config required");

        for i in 0..right_batch.num_rows() {
            let keyed_row = right_batch.slice(i, 1);
            let unkeyed_row = self.right_input_schema.unkeyed_batch(&keyed_row)?;
            let new_right_ts = ts_value(&unkeyed_row, cfg.right_ts_index)?;

            let right_table = ctx
                .table_manager
                .get_key_time_table("right", ctx.last_present_watermark())
                .await?;
            let mut key_rows = right_table.insert(keyed_row).await?;
            let Some(key_row) = key_rows.pop() else {
                continue;
            };

            // After insertion, look up the full buffered right state for this
            // key and the buffered left rows.
            let right_table = ctx
                .table_manager
                .get_key_time_table("right", ctx.last_present_watermark())
                .await?;
            let Some(all_right_for_key) = right_table.get_batch(key_row.as_ref())? else {
                continue;
            };
            let all_right_for_key = all_right_for_key.clone();

            let left_table = ctx
                .table_manager
                .get_key_time_table("left", ctx.last_present_watermark())
                .await?;
            let Some(left_candidates) = left_table.get_batch(key_row.as_ref())? else {
                continue;
            };
            let left_candidates = left_candidates.clone();

            for j in 0..left_candidates.num_rows() {
                let left_row = left_candidates.slice(j, 1);
                let left_ts = ts_value(&left_row, cfg.left_ts_index)?;

                if new_right_ts > left_ts {
                    continue;
                }
                let Some(best_idx) =
                    pick_asof_right(&all_right_for_key, cfg.right_ts_index, left_ts)?
                else {
                    continue;
                };
                let best_ts = ts_value(&all_right_for_key.slice(best_idx, 1), cfg.right_ts_index)?;
                if best_ts != new_right_ts {
                    // A previously-buffered right row is still the ASOF
                    // match; emitting now would duplicate an earlier
                    // emission.
                    continue;
                }
                self.compute_pair(left_row, unkeyed_row.clone(), collector)
                    .await?;
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
            .execute(0, SessionContext::new().task_ctx())
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

    fn tables(&self) -> HashMap<String, TableConfig> {
        let mut tables = HashMap::new();
        tables.insert(
            "left".to_string(),
            timestamp_table_config(
                "left",
                "left join data",
                self.left_expiration,
                false,
                self.left_input_schema.clone(),
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

        let mut ttl = Duration::from_micros(
            config
                .ttl_micros
                .expect("ttl must be set for non-instant join"),
        );

        if ttl == Duration::ZERO {
            warn!("TTL was not set for join with expiration");
            ttl = Duration::from_secs(24 * 60 * 60);
        }

        Ok(ConstructedOperator::from_operator(Box::new(
            JoinWithExpiration {
                left_expiration: ttl,
                right_expiration: ttl,
                left_input_schema,
                right_input_schema,
                left_schema,
                right_schema,
                left_passer,
                right_passer,
                join_execution_plan,
                asof: config.asof.map(|a| AsofConfig {
                    left_ts_index: a.left_ts_index as usize,
                    right_ts_index: a.right_ts_index as usize,
                }),
            },
        )))
    }
}

/// Read the `i64` value at row 0 of the timestamp column of a single-row
/// (unkeyed) batch. ASOF joins require the timestamp column to be a
/// `Timestamp(Nanosecond)` (the canonical Arroyo event-time representation).
fn ts_value(batch: &RecordBatch, idx: usize) -> DataflowResult<i64> {
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
        return Err(arrow::error::ArrowError::InvalidArgumentError(
            "ASOF JOIN does not support NULL timestamps".into(),
        )
        .into());
    }
    Ok(ts.value(0))
}

/// Return the index in `candidates` of the row with the largest `right_ts`
/// value that is still `<=` `left_ts`, or `None` if no such row exists. NULL
/// timestamps are skipped.
fn pick_asof_right(
    candidates: &RecordBatch,
    right_ts_index: usize,
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

    let mut best: Option<(usize, i64)> = None;
    for i in 0..ts.len() {
        if ts.is_null(i) {
            continue;
        }
        let v = ts.value(i);
        if v > left_ts {
            continue;
        }
        match best {
            Some((_, b)) if b >= v => {}
            _ => best = Some((i, v)),
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
        assert_eq!(pick_asof_right(&batch, 1, 25).unwrap(), Some(1));
        // left_ts = 40 → best is the largest <= 40 (index 3)
        assert_eq!(pick_asof_right(&batch, 1, 40).unwrap(), Some(3));
        // left_ts = 5 → no candidate
        assert_eq!(pick_asof_right(&batch, 1, 5).unwrap(), None);
    }

    #[test]
    fn pick_asof_right_skips_nulls() {
        let batch = ts_batch(vec![None, Some(10), None, Some(20), None]);
        assert_eq!(pick_asof_right(&batch, 1, 15).unwrap(), Some(1));
        assert_eq!(pick_asof_right(&batch, 1, 25).unwrap(), Some(3));
        // All-null candidates → None
        let null_batch = ts_batch(vec![None, None, None]);
        assert_eq!(pick_asof_right(&null_batch, 1, 100).unwrap(), None);
    }

    #[test]
    fn pick_asof_right_picks_first_when_tied_on_max() {
        // Two rows tie on the same max value <= left_ts. The kernel must
        // return one (current implementation prefers the earlier index).
        let batch = ts_batch(vec![Some(10), Some(10), Some(5)]);
        let got = pick_asof_right(&batch, 1, 12).unwrap();
        assert!(got == Some(0) || got == Some(1));
    }

    #[test]
    fn pick_asof_right_rejects_non_timestamp_column() {
        // Column 0 is Int64, not Timestamp → kernel must error out.
        let batch = ts_batch(vec![Some(1)]);
        let err = pick_asof_right(&batch, 0, 0).unwrap_err().to_string();
        assert!(
            err.contains("Timestamp"),
            "expected Timestamp type error, got: {err}"
        );
    }

    #[test]
    fn ts_value_reads_first_row() {
        let batch = ts_batch(vec![Some(123), Some(456)]);
        assert_eq!(ts_value(&batch, 1).unwrap(), 123);
    }

    #[test]
    fn ts_value_errors_on_null() {
        let batch = ts_batch(vec![None]);
        let err = ts_value(&batch, 1).unwrap_err().to_string();
        assert!(err.contains("NULL"), "expected NULL error, got: {err}");
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
}
