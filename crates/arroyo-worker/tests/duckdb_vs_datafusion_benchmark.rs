use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use arrow::util::pretty::pretty_format_batches;
use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use duckdb::Connection;
use duckdb::params_from_iter;
use duckdb::vtab::arrow::{ArrowVTab, arrow_recordbatch_to_query_params};

const QUERY: &str = "SELECT event_type, SUM(count) AS total \
    FROM events \
    GROUP BY event_type \
    ORDER BY event_type";

#[derive(Clone, Copy)]
struct BenchmarkCase {
    name: &'static str,
    batch_count: usize,
    rows_per_batch: usize,
    distinct_event_types: usize,
    warmup_iterations: usize,
    measured_iterations: usize,
}

#[derive(Clone, Copy)]
struct BenchmarkStats {
    avg: Duration,
    min: Duration,
    max: Duration,
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

fn benchmark_cases() -> [BenchmarkCase; 4] {
    [
        BenchmarkCase {
            name: "medium_few_keys",
            batch_count: 64,
            rows_per_batch: 2_048,
            distinct_event_types: 8,
            warmup_iterations: 1,
            measured_iterations: 5,
        },
        BenchmarkCase {
            name: "medium_many_keys",
            batch_count: 64,
            rows_per_batch: 2_048,
            distinct_event_types: 128,
            warmup_iterations: 1,
            measured_iterations: 5,
        },
        BenchmarkCase {
            name: "large_few_batches",
            batch_count: 256,
            rows_per_batch: 2_048,
            distinct_event_types: 32,
            warmup_iterations: 1,
            measured_iterations: 5,
        },
        BenchmarkCase {
            name: "large_many_batches",
            batch_count: 1_024,
            rows_per_batch: 512,
            distinct_event_types: 32,
            warmup_iterations: 1,
            measured_iterations: 5,
        },
    ]
}

fn make_batches(case: BenchmarkCase) -> Vec<RecordBatch> {
    let schema = build_events_schema();
    let mut batches = Vec::with_capacity(case.batch_count);

    for batch_idx in 0..case.batch_count {
        let mut timestamps = Vec::with_capacity(case.rows_per_batch);
        let mut event_types = Vec::with_capacity(case.rows_per_batch);
        let mut counts = Vec::with_capacity(case.rows_per_batch);

        for row_idx in 0..case.rows_per_batch {
            let global_idx = batch_idx * case.rows_per_batch + row_idx;
            timestamps.push(global_idx as i64 * 1_000_000);
            event_types.push(format!(
                "event_type_{:03}",
                global_idx % case.distinct_event_types
            ));
            counts.push((global_idx % 17 + 1) as i64);
        }

        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampNanosecondArray::from(timestamps)),
                    Arc::new(StringArray::from(event_types)),
                    Arc::new(Int64Array::from(counts)),
                ],
            )
            .unwrap(),
        );
    }

    batches
}

fn arrow_select_sql(batch_count: usize) -> String {
    let mut queries = std::iter::repeat_n("SELECT * FROM arrow(?, ?)".to_string(), batch_count)
        .collect::<Vec<_>>();

    while queries.len() > 1 {
        let mut combined = Vec::with_capacity(queries.len().div_ceil(2));
        let mut iter = queries.into_iter();

        while let Some(left) = iter.next() {
            if let Some(right) = iter.next() {
                combined.push(format!(
                    "SELECT * FROM ({left}) AS __arroyo_left UNION ALL SELECT * FROM ({right}) AS __arroyo_right"
                ));
            } else {
                combined.push(left);
            }
        }

        queries = combined;
    }

    queries.pop().unwrap_or_default()
}

fn arrow_params(batches: &[RecordBatch]) -> Vec<usize> {
    batches
        .iter()
        .flat_map(|batch| arrow_recordbatch_to_query_params(batch.clone()))
        .collect()
}

fn run_duckdb_query(batches: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
    let conn = Connection::open_in_memory()?;
    conn.register_table_function::<ArrowVTab>("arrow")?;
    let max_expression_depth = (batches.len() * 4).max(1_000);
    conn.execute(
        &format!("SET max_expression_depth TO {max_expression_depth}"),
        [],
    )?;
    let query_sql = format!(
        "WITH events AS ({}) {}",
        arrow_select_sql(batches.len()),
        QUERY
    );
    let mut stmt = conn.prepare(&query_sql)?;
    Ok(stmt
        .query_arrow(params_from_iter(arrow_params(batches)))?
        .collect())
}

async fn run_datafusion_query(batches: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
    let schema = batches[0].schema();
    let table = MemTable::try_new(schema, vec![batches.to_vec()])?;
    let ctx = SessionContext::new();
    ctx.register_table("events", Arc::new(table))?;
    let df = ctx.sql(QUERY).await?;
    Ok(df.collect().await?)
}

fn formatted_batches(batches: &[RecordBatch]) -> Result<String> {
    Ok(pretty_format_batches(batches)?.to_string())
}

async fn assert_same_results(case: BenchmarkCase, batches: &[RecordBatch]) -> Result<()> {
    let duckdb_results = run_duckdb_query(batches)?;
    let datafusion_results = run_datafusion_query(batches).await?;

    assert_eq!(
        formatted_batches(&duckdb_results)?,
        formatted_batches(&datafusion_results)?,
        "DuckDB and DataFusion diverged for benchmark case {}",
        case.name
    );

    Ok(())
}

async fn benchmark_duckdb(batches: &[RecordBatch], iterations: usize) -> Result<BenchmarkStats> {
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        let _ = run_duckdb_query(batches)?;
        samples.push(start.elapsed());
    }
    Ok(duration_stats(&samples))
}

async fn benchmark_datafusion(
    batches: &[RecordBatch],
    iterations: usize,
) -> Result<BenchmarkStats> {
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        let _ = run_datafusion_query(batches).await?;
        samples.push(start.elapsed());
    }
    Ok(duration_stats(&samples))
}

fn duration_stats(samples: &[Duration]) -> BenchmarkStats {
    let total = samples
        .iter()
        .copied()
        .fold(Duration::ZERO, |acc, sample| acc + sample);
    BenchmarkStats {
        avg: total / samples.len() as u32,
        min: *samples.iter().min().unwrap(),
        max: *samples.iter().max().unwrap(),
    }
}

fn fmt_duration(duration: Duration) -> String {
    format!("{:.2} ms", duration.as_secs_f64() * 1_000.0)
}

#[tokio::test]
async fn duckdb_and_datafusion_match_grouped_query() -> Result<()> {
    let case = BenchmarkCase {
        name: "correctness_check",
        batch_count: 8,
        rows_per_batch: 2_048,
        distinct_event_types: 16,
        warmup_iterations: 0,
        measured_iterations: 0,
    };
    let batches = make_batches(case);
    assert_same_results(case, &batches).await
}

#[tokio::test]
#[ignore = "benchmark"]
async fn benchmark_duckdb_vs_datafusion_grouped_query() -> Result<()> {
    println!(
        "| case | rows | batches | keys | duckdb avg | duckdb min | duckdb max | datafusion avg | datafusion min | datafusion max | speedup |"
    );
    println!("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |");

    for case in benchmark_cases() {
        let batches = make_batches(case);
        assert_same_results(case, &batches).await?;

        for _ in 0..case.warmup_iterations {
            let _ = run_duckdb_query(&batches)?;
            let _ = run_datafusion_query(&batches).await?;
        }

        let duckdb = benchmark_duckdb(&batches, case.measured_iterations).await?;
        let datafusion = benchmark_datafusion(&batches, case.measured_iterations).await?;
        let speedup = datafusion.avg.as_secs_f64() / duckdb.avg.as_secs_f64();
        let total_rows = case.batch_count * case.rows_per_batch;

        println!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.2}x |",
            case.name,
            total_rows,
            case.batch_count,
            case.distinct_event_types,
            fmt_duration(duckdb.avg),
            fmt_duration(duckdb.min),
            fmt_duration(duckdb.max),
            fmt_duration(datafusion.avg),
            fmt_duration(datafusion.min),
            fmt_duration(datafusion.max),
            speedup,
        );
    }

    Ok(())
}
