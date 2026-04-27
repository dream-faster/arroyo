use std::collections::HashMap;

use anyhow::{Result, anyhow};
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use duckdb::Connection;
use duckdb::vtab::arrow::{ArrowVTab, arrow_recordbatch_to_query_params};

pub(crate) fn execute_query(
    query: &str,
    tables: &HashMap<&str, Vec<RecordBatch>>,
) -> Result<Vec<RecordBatch>> {
    let connection = Connection::open_in_memory()?;
    connection.register_table_function::<ArrowVTab>("arrow")?;

    for (name, batches) in tables {
        register_table(&connection, name, batches)?;
    }

    let mut statement = connection.prepare(query)?;
    Ok(statement.query_arrow([])?.collect())
}

fn register_table(connection: &Connection, name: &str, batches: &[RecordBatch]) -> Result<()> {
    let first_batch = batches
        .first()
        .ok_or_else(|| anyhow!("duckdb test table '{name}' must contain at least one batch"))?;

    let create_table = format!(
        "CREATE TEMP TABLE {} AS SELECT * FROM arrow(?, ?)",
        quote_identifier(name)
    );
    let mut create_statement = connection.prepare(&create_table)?;
    create_statement.execute(arrow_recordbatch_to_query_params(first_batch.clone()))?;

    let insert_sql = format!(
        "INSERT INTO {} SELECT * FROM arrow(?, ?)",
        quote_identifier(name)
    );

    for batch in &batches[1..] {
        let mut insert_statement = connection.prepare(&insert_sql)?;
        insert_statement.execute(arrow_recordbatch_to_query_params(batch.clone()))?;
    }

    Ok(())
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

#[test]
fn executes_query_against_multiple_arrow_batches() {
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("city", DataType::Utf8, false),
        Field::new("count", DataType::Int32, false),
    ]));

    let batches = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![
                std::sync::Arc::new(StringArray::from(vec!["NYC", "SF"])),
                std::sync::Arc::new(Int32Array::from(vec![1, 1])),
            ],
        )
        .unwrap(),
        RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(StringArray::from(vec!["NYC", "LA"])),
                std::sync::Arc::new(Int32Array::from(vec![2, 5])),
            ],
        )
        .unwrap(),
    ];

    let results = execute_query(
        "SELECT city, SUM(count) AS total_count FROM events GROUP BY city ORDER BY city",
        &HashMap::from([("events", batches)]),
    )
    .unwrap();

    let formatted = pretty_format_batches(&results).unwrap().to_string();
    assert_eq!(
        formatted,
        concat!(
            "+------+-------------+\n",
            "| city | total_count |\n",
            "+------+-------------+\n",
            "| LA   | 5           |\n",
            "| NYC  | 3           |\n",
            "| SF   | 1           |\n",
            "+------+-------------+"
        )
    );
}

#[test]
fn executes_query_with_multiple_registered_tables() {
    let users = RecordBatch::try_new(
        std::sync::Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ])),
        vec![
            std::sync::Arc::new(Int32Array::from(vec![1, 2])),
            std::sync::Arc::new(StringArray::from(vec!["Ada", "Grace"])),
        ],
    )
    .unwrap();
    let scores = RecordBatch::try_new(
        std::sync::Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int32, false),
            Field::new("score", DataType::Int32, false),
        ])),
        vec![
            std::sync::Arc::new(Int32Array::from(vec![1, 2])),
            std::sync::Arc::new(Int32Array::from(vec![10, 20])),
        ],
    )
    .unwrap();

    let results = execute_query(
        "SELECT u.name, s.score FROM users u JOIN scores s USING (user_id) ORDER BY s.score DESC",
        &HashMap::from([("users", vec![users]), ("scores", vec![scores])]),
    )
    .unwrap();

    let formatted = pretty_format_batches(&results).unwrap().to_string();
    assert_eq!(
        formatted,
        concat!(
            "+-------+-------+\n",
            "| name  | score |\n",
            "+-------+-------+\n",
            "| Grace | 20    |\n",
            "| Ada   | 10    |\n",
            "+-------+-------+"
        )
    );
}
