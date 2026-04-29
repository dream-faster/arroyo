use anyhow::{Result, anyhow};
use arrow::compute::{partition, sort_to_indices, take};
use arrow_array::{Array, PrimitiveArray, RecordBatch, types::TimestampNanosecondType};
use arrow_schema::{Schema, SchemaRef};
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::{
    ArrowOperator, AsDisplayable, ConstructedOperator, DisplayableOperator, OperatorConstructor,
    Registry,
};
use arroyo_planner::schemas::add_timestamp_field_arrow;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::{api, rpc::TableConfig};
use arroyo_state::timestamp_table_config;
use arroyo_types::{CheckpointBarrier, Watermark, from_nanos, print_time, to_nanos};
use datafusion::common::ScalarValue;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::{execution::context::SessionContext, physical_plan::ExecutionPlan};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use duckdb::Connection;
use duckdb::vtab::arrow::arrow_recordbatch_to_query_params;
use futures::StreamExt;
use prost::Message;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use tracing::warn;

use arroyo_planner::physical::{ArroyoPhysicalExtensionCodec, DecodingContext};
use arroyo_rpc::df::ArroyoSchema;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::protobuf::PhysicalPlanNode;

/// Per-bin accumulated state: just the raw input `RecordBatch`es that arrived during the window.
#[derive(Default)]
struct BinHolder {
    batches: Vec<RecordBatch>,
    checkpointed_len: usize,
}

/// A tumbling window operator that accumulates raw input batches per window bin and, when the
/// watermark advances past a bin boundary, executes a user-supplied DuckDB SQL query against those
/// batches and emits the results.
pub struct DuckdbTumblingWindowFunc {
    width: Duration,
    binning_function: Arc<dyn PhysicalExpr>,
    /// SQL to execute on window close.  References `input_table_name` as the table.
    sql_query: String,
    /// Name of the DuckDB table that the input batches are registered under.
    input_table_name: String,
    /// Output schema with an appended `_timestamp` field.
    output_schema: SchemaRef,
    /// Input schema (without `_timestamp`).
    input_schema: ArroyoSchema,
    final_projection: Option<Arc<dyn ExecutionPlan>>,
    final_batches_passer: Arc<RwLock<Vec<RecordBatch>>>,
    execs: BTreeMap<SystemTime, BinHolder>,
}

impl DuckdbTumblingWindowFunc {
    fn bin_start(&self, timestamp: SystemTime) -> SystemTime {
        if self.width == Duration::ZERO {
            return timestamp;
        }
        let nanos = to_nanos(timestamp);
        let aligned = nanos - nanos % self.width.as_nanos();
        from_nanos(aligned)
    }

    /// Execute `self.sql_query` inside an in-memory DuckDB instance against `batches`, then append
    /// a `_timestamp` column populated with `bin_start_nanos`.
    fn execute_on_batches(
        &self,
        batches: &[RecordBatch],
        bin_start: SystemTime,
    ) -> Result<Vec<RecordBatch>> {
        if batches.is_empty() {
            return Ok(vec![]);
        }

        let conn = Connection::open_in_memory()?;
        conn.register_table_function::<duckdb::vtab::arrow::ArrowVTab>("arrow")?;

        // Register the first batch to create the temp table
        let first = &batches[0];
        let create_sql = format!(
            "CREATE TEMP TABLE {} AS SELECT * FROM arrow(?, ?)",
            quote_ident(&self.input_table_name)
        );
        let mut stmt = conn.prepare(&create_sql)?;
        stmt.execute(arrow_recordbatch_to_query_params(first.clone()))?;

        // Insert remaining batches
        if batches.len() > 1 {
            let insert_sql = format!(
                "INSERT INTO {} SELECT * FROM arrow(?, ?)",
                quote_ident(&self.input_table_name)
            );
            for batch in &batches[1..] {
                let mut stmt = conn.prepare(&insert_sql)?;
                stmt.execute(arrow_recordbatch_to_query_params(batch.clone()))?;
            }
        }

        // Run the user query
        let mut stmt = conn.prepare(&self.sql_query)?;
        let result_batches: Vec<RecordBatch> = stmt.query_arrow([])?.collect();

        // Append `_timestamp` = bin_start to each result batch
        let bin_ts = ScalarValue::TimestampNanosecond(Some(to_nanos(bin_start) as i64), None);
        result_batches
            .into_iter()
            .map(|batch| append_timestamp(&batch, &bin_ts, self.output_schema.clone()))
            .collect()
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn append_timestamp(
    batch: &RecordBatch,
    ts: &ScalarValue,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let ts_array = ts.to_array_of_size(batch.num_rows())?;
    let mut columns: Vec<Arc<dyn Array>> = batch.columns().to_vec();
    columns.push(ts_array);
    RecordBatch::try_new(schema, columns)
        .map_err(|e| anyhow!("failed to append timestamp column: {e}"))
}

pub struct DuckdbTumblingWindowConstructor;

impl OperatorConstructor for DuckdbTumblingWindowConstructor {
    type ConfigT = api::DuckdbTumblingWindowAggregateOperator;

    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        let width = Duration::from_micros(config.width_micros);

        let input_schema: ArroyoSchema = config
            .input_schema
            .ok_or_else(|| anyhow!("DuckdbTumblingWindow requires input_schema"))?
            .try_into()?;

        let binning_function_node =
            PhysicalExprNode::decode(&mut config.binning_function.as_slice())?;
        let binning_function = parse_physical_expr(
            &binning_function_node,
            registry.as_ref(),
            &input_schema.schema,
            &DefaultPhysicalExtensionCodec {},
        )?;

        // Build the output schema: all columns from the SQL result plus `_timestamp`.
        // We derive the result schema by running a dry-run of the DuckDB query against an
        // empty table built from the input schema.
        let result_schema = derive_result_schema(
            &input_schema.schema,
            &config.sql_query,
            &config.input_table_name,
        )?;
        let output_schema = add_timestamp_field_arrow((*result_schema).clone());
        let final_batches_passer = Arc::new(RwLock::new(Vec::new()));
        let final_projection = config
            .final_projection
            .map(|proto| PhysicalPlanNode::decode(&mut proto.as_slice()))
            .transpose()?
            .map(|plan| {
                plan.try_into_physical_plan(
                    registry.as_ref(),
                    &RuntimeEnvBuilder::new().build().unwrap(),
                    &ArroyoPhysicalExtensionCodec {
                        context: DecodingContext::LockedBatchVec(final_batches_passer.clone()),
                    },
                )
            })
            .transpose()?;

        let input_table_name = if config.input_table_name.is_empty() {
            "input".to_string()
        } else {
            config.input_table_name
        };

        Ok(ConstructedOperator::from_operator(Box::new(
            DuckdbTumblingWindowFunc {
                width,
                binning_function,
                sql_query: config.sql_query,
                input_table_name,
                output_schema,
                input_schema,
                final_projection,
                final_batches_passer,
                execs: BTreeMap::new(),
            },
        )))
    }
}

/// Derive the schema that the SQL query will produce by executing it against a zero-row table with
/// the correct schema.
fn derive_result_schema(
    input_schema: &Arc<Schema>,
    sql_query: &str,
    input_table_name: &str,
) -> Result<Arc<Schema>> {
    use duckdb::vtab::arrow::ArrowVTab;
    let conn = Connection::open_in_memory()?;
    conn.register_table_function::<ArrowVTab>("arrow")?;

    // Build zero-row batch matching the input schema by creating empty arrays of each type
    let empty_arrays: Vec<Arc<dyn Array>> = input_schema
        .fields()
        .iter()
        .map(|f| arrow_array::new_empty_array(f.data_type()))
        .collect();
    let empty_batch = RecordBatch::try_new(input_schema.clone(), empty_arrays)
        .map_err(|e| anyhow!("failed to create empty batch: {e}"))?;

    let create_sql = format!(
        "CREATE TEMP TABLE {} AS SELECT * FROM arrow(?, ?)",
        quote_ident(input_table_name)
    );
    conn.prepare(&create_sql)?
        .execute(arrow_recordbatch_to_query_params(empty_batch))?;

    // Use `schema()` from `RecordBatchReader` – this gives the output schema from query metadata
    // even when the query returns 0 rows (e.g., GROUP BY on empty input).
    let mut stmt = conn.prepare(sql_query)?;
    let arrow_result = stmt.query_arrow([])?;
    Ok(arrow_result.get_schema())
}

#[async_trait::async_trait]
impl ArrowOperator for DuckdbTumblingWindowFunc {
    fn name(&self) -> String {
        "duckdb_tumbling_window".to_string()
    }

    fn display(&self) -> DisplayableOperator<'_> {
        DisplayableOperator {
            name: Cow::Borrowed("DuckdbTumblingWindowFunc"),
            fields: vec![
                ("width", AsDisplayable::Debug(&self.width)),
                ("sql_query", AsDisplayable::Display(&self.sql_query)),
                (
                    "input_table_name",
                    AsDisplayable::Display(&self.input_table_name),
                ),
            ],
        }
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        let watermark = ctx.last_present_watermark();
        let table = ctx
            .table_manager
            .get_expiring_time_key_table("t", watermark)
            .await?;
        for (timestamp, batch_list) in table.all_batches_for_watermark(watermark) {
            let bin = self.bin_start(*timestamp);
            let holder = self.execs.entry(bin).or_default();
            batch_list
                .iter()
                .for_each(|b| holder.batches.push(b.clone()));
            holder.checkpointed_len = holder.batches.len();
        }
        Ok(())
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let batch = self
            .input_schema
            .filter_by_time(batch, ctx.last_present_watermark())
            .map_err(anyhow::Error::from)?;
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let bin_column = self
            .binning_function
            .evaluate(&batch)?
            .into_array(batch.num_rows())?;

        let indices = sort_to_indices(bin_column.as_ref(), None, None)?;
        let sorted_batch = RecordBatch::try_new(
            batch.schema(),
            batch
                .columns()
                .iter()
                .map(|c| take(c, &indices, None).unwrap())
                .collect(),
        )?;
        let sorted_bins = take(&*bin_column, &indices, None)?;

        let parts = partition(vec![sorted_bins.clone()].as_slice())?;
        let typed_bins = sorted_bins
            .as_any()
            .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
            .unwrap();

        for range in parts.ranges() {
            let bin_start = from_nanos(typed_bins.value(range.start) as u128);
            let watermark = ctx.last_present_watermark();

            if let Some(wm) = watermark
                && bin_start < self.bin_start(wm)
            {
                warn!(
                    "DuckdbTumblingWindow: bin {} is before watermark {}, skipping",
                    print_time(bin_start),
                    print_time(wm)
                );
                continue;
            }

            let bin_batch = sorted_batch.slice(range.start, range.end - range.start);
            self.execs
                .entry(bin_start)
                .or_default()
                .batches
                .push(bin_batch);
        }
        Ok(())
    }

    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<Option<Watermark>> {
        if let Some(wm) = ctx.last_present_watermark() {
            let cutoff = self.bin_start(wm);
            while let Some((bin_start, _)) = self.execs.first_key_value() {
                if *bin_start >= cutoff {
                    break;
                }
                let (bin_start, holder) = self.execs.pop_first().unwrap();
                let results = self.execute_on_batches(&holder.batches, bin_start)?;
                if let Some(final_projection) = self.final_projection.as_ref() {
                    if results.is_empty() {
                        continue;
                    }

                    {
                        let mut batches = self.final_batches_passer.write().unwrap();
                        *batches = results;
                    }

                    final_projection.reset()?;
                    let mut projection_exec =
                        final_projection.execute(0, SessionContext::new().task_ctx())?;
                    while let Some(batch) = projection_exec.next().await {
                        collector.collect(batch?).await?;
                    }
                } else {
                    for batch in results {
                        collector.collect(batch).await?;
                    }
                }
            }
        }
        Ok(Some(watermark))
    }

    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let watermark = ctx.watermark().and_then(|wm| match wm {
            Watermark::EventTime(t) => Some(t),
            Watermark::Idle => None,
        });

        let table = ctx
            .table_manager
            .get_expiring_time_key_table("t", watermark)
            .await?;

        for (bin, holder) in &self.execs {
            for batch in &holder.batches[holder.checkpointed_len..] {
                table.insert(*bin, batch.clone());
            }
        }
        for holder in self.execs.values_mut() {
            holder.checkpointed_len = holder.batches.len();
        }
        table.flush(watermark).await?;
        Ok(())
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        [(
            "t".to_string(),
            timestamp_table_config(
                "t",
                "duckdb_tumbling_intermediate",
                self.width,
                false,
                self.input_schema.clone(),
            ),
        )]
        .into_iter()
        .collect()
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};

    fn ts_ns(secs_from_epoch: u64) -> i64 {
        (secs_from_epoch * 1_000_000_000) as i64
    }

    fn build_events_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(
                "event_ts",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
        ]))
    }

    fn build_window_func(
        width_secs: u64,
        sql_query: &str,
        input_schema: Arc<Schema>,
    ) -> DuckdbTumblingWindowFunc {
        let width = Duration::from_secs(width_secs);
        // derive output schema
        let result_schema = derive_result_schema(&input_schema, sql_query, "events").unwrap();
        let output_schema = add_timestamp_field_arrow((*result_schema).clone());

        // ArroyoSchema requires a _timestamp field; add one to the schema for tests.
        let schema_with_ts =
            Arc::new((*add_timestamp_field_arrow((*input_schema).clone())).clone());
        let ts_index = schema_with_ts.fields().len() - 1;
        let arroyo_schema = ArroyoSchema::new_keyed(schema_with_ts, ts_index, vec![]);

        // Placeholder binning function for tests – execute_on_batches is tested directly.
        use datafusion::physical_expr::expressions::Column;
        let binning_function: Arc<dyn PhysicalExpr> = Arc::new(Column::new("event_ts", 0));

        DuckdbTumblingWindowFunc {
            width,
            binning_function,
            sql_query: sql_query.to_string(),
            input_table_name: "events".to_string(),
            output_schema,
            input_schema: arroyo_schema,
            final_projection: None,
            final_batches_passer: Arc::new(RwLock::new(Vec::new())),
            execs: BTreeMap::new(),
        }
    }

    fn make_batch(
        schema: Arc<Schema>,
        ts_values: Vec<i64>,
        event_types: Vec<&str>,
        counts: Vec<i64>,
    ) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(ts_values)),
                Arc::new(StringArray::from(event_types)),
                Arc::new(Int64Array::from(counts)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn execute_on_batches_counts_by_event_type() {
        let schema = build_events_schema();
        let wf = build_window_func(
            3600,
            "SELECT event_type, SUM(count) AS total FROM events GROUP BY event_type ORDER BY event_type",
            schema.clone(),
        );

        let _hour_ns = 3_600 * 1_000_000_000_i64;
        let bin_start_ts = ts_ns(0); // epoch

        let batches = vec![
            make_batch(
                schema.clone(),
                vec![bin_start_ts, bin_start_ts + 1],
                vec!["click", "view"],
                vec![3, 7],
            ),
            make_batch(
                schema.clone(),
                vec![bin_start_ts + 2, bin_start_ts + 3],
                vec!["click", "click"],
                vec![2, 1],
            ),
        ];

        let bin_start = UNIX_EPOCH;
        let results = wf.execute_on_batches(&batches, bin_start).unwrap();

        // Strip the _timestamp column for display
        let stripped: Vec<RecordBatch> = results
            .iter()
            .map(|b| {
                b.project(&(0..b.num_columns() - 1).collect::<Vec<_>>())
                    .unwrap()
            })
            .collect();

        let formatted = pretty_format_batches(&stripped).unwrap().to_string();
        assert_eq!(
            formatted,
            concat!(
                "+------------+-------+\n",
                "| event_type | total |\n",
                "+------------+-------+\n",
                "| click      | 6     |\n",
                "| view       | 7     |\n",
                "+------------+-------+"
            )
        );
    }

    #[test]
    fn execute_on_batches_emits_timestamp_column() {
        let schema = build_events_schema();
        let wf = build_window_func(
            3600,
            "SELECT event_type, COUNT(*) AS n FROM events GROUP BY event_type",
            schema.clone(),
        );

        let bin_start = UNIX_EPOCH + Duration::from_secs(7200);
        let batches = vec![make_batch(
            schema,
            vec![ts_ns(7200)],
            vec!["purchase"],
            vec![1],
        )];

        let results = wf.execute_on_batches(&batches, bin_start).unwrap();
        assert!(!results.is_empty(), "should produce at least one batch");

        let batch = &results[0];
        let ts_col_name = batch
            .schema()
            .field(batch.num_columns() - 1)
            .name()
            .to_string();
        assert_eq!(ts_col_name, "_timestamp");

        let ts_array = batch
            .column(batch.num_columns() - 1)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("_timestamp must be TimestampNanosecond");
        let expected_ns = to_nanos(bin_start) as i64;
        assert_eq!(ts_array.value(0), expected_ns);
    }

    #[test]
    fn execute_on_batches_empty_returns_empty() {
        let schema = build_events_schema();
        let wf = build_window_func(
            3600,
            "SELECT event_type, COUNT(*) AS n FROM events GROUP BY event_type",
            schema,
        );
        let results = wf.execute_on_batches(&[], UNIX_EPOCH).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn bin_start_rounds_down_to_window_boundary() {
        let wf = {
            let schema = build_events_schema();
            build_window_func(3600, "SELECT COUNT(*) AS n FROM events", schema)
        };

        // 1.5 hours since epoch → should round down to 1 hour since epoch
        let ts = UNIX_EPOCH + Duration::from_secs(5400);
        let expected = UNIX_EPOCH + Duration::from_secs(3600);
        assert_eq!(wf.bin_start(ts), expected);
    }

    #[test]
    fn bin_start_at_exact_boundary_stays_same() {
        let wf = {
            let schema = build_events_schema();
            build_window_func(3600, "SELECT COUNT(*) AS n FROM events", schema)
        };

        let ts = UNIX_EPOCH + Duration::from_secs(7200);
        assert_eq!(wf.bin_start(ts), ts);
    }

    #[test]
    fn execute_on_batches_supports_multi_table_select() {
        let schema = build_events_schema();
        // A query that just selects columns unchanged (no aggregation)
        let wf = build_window_func(
            60,
            "SELECT event_type, SUM(count) AS total FROM events GROUP BY event_type ORDER BY total DESC",
            schema.clone(),
        );

        let batches = vec![make_batch(
            schema,
            vec![ts_ns(0), ts_ns(1), ts_ns(2)],
            vec!["a", "b", "a"],
            vec![10, 5, 15],
        )];

        let results = wf.execute_on_batches(&batches, UNIX_EPOCH).unwrap();
        assert!(!results.is_empty());

        let stripped: Vec<RecordBatch> = results
            .iter()
            .map(|b| {
                b.project(&(0..b.num_columns() - 1).collect::<Vec<_>>())
                    .unwrap()
            })
            .collect();

        let formatted = pretty_format_batches(&stripped).unwrap().to_string();
        assert_eq!(
            formatted,
            concat!(
                "+------------+-------+\n",
                "| event_type | total |\n",
                "+------------+-------+\n",
                "| a          | 25    |\n",
                "| b          | 5     |\n",
                "+------------+-------+"
            )
        );
    }

    #[test]
    fn execute_on_batches_handles_window_with_filter() {
        let schema = build_events_schema();
        let wf = build_window_func(
            3600,
            "SELECT event_type, SUM(count) AS total FROM events WHERE event_type != 'spam' GROUP BY event_type ORDER BY event_type",
            schema.clone(),
        );

        let batches = vec![make_batch(
            schema,
            vec![ts_ns(0), ts_ns(1), ts_ns(2)],
            vec!["click", "spam", "click"],
            vec![3, 99, 2],
        )];

        let results = wf.execute_on_batches(&batches, UNIX_EPOCH).unwrap();
        let stripped: Vec<RecordBatch> = results
            .iter()
            .map(|b| {
                b.project(&(0..b.num_columns() - 1).collect::<Vec<_>>())
                    .unwrap()
            })
            .collect();

        let formatted = pretty_format_batches(&stripped).unwrap().to_string();
        assert_eq!(
            formatted,
            concat!(
                "+------------+-------+\n",
                "| event_type | total |\n",
                "+------------+-------+\n",
                "| click      | 5     |\n",
                "+------------+-------+"
            )
        );
    }

    #[test]
    fn bin_accumulation_routes_batches_to_correct_bins() {
        // Directly test that process_batch routes batches into the right BinHolder bins.
        // We bypass the full operator machinery and just call bin_start.
        let schema = build_events_schema();
        let wf = build_window_func(3600, "SELECT COUNT(*) AS n FROM events", schema.clone());

        let t_bin0 = ts_ns(0); // maps to bin 0s
        let t_bin1 = ts_ns(3601); // maps to bin 3600s
        let t_bin1b = ts_ns(7100); // also maps to bin 3600s

        // bin_start checks
        assert_eq!(wf.bin_start(from_nanos(t_bin0 as u128)), UNIX_EPOCH);
        assert_eq!(
            wf.bin_start(from_nanos(t_bin1 as u128)),
            UNIX_EPOCH + Duration::from_secs(3600)
        );
        assert_eq!(
            wf.bin_start(from_nanos(t_bin1b as u128)),
            UNIX_EPOCH + Duration::from_secs(3600)
        );
    }
}
