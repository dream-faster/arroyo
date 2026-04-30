CREATE TABLE quotes (
    symbol TEXT,
    price DOUBLE,
    ts TIMESTAMP
) WITH (
    connector = 'kafka',
    topic = 'quotes',
    type = 'source',
    format = 'json',
    bootstrap_servers = 'broker:9092',
    event_time_field = ts
);

CREATE TABLE trades (
    symbol TEXT,
    qty BIGINT,
    ts TIMESTAMP
) WITH (
    connector = 'kafka',
    topic = 'trades',
    type = 'source',
    format = 'json',
    bootstrap_servers = 'broker:9092',
    event_time_field = ts
);

SELECT t.symbol, t.qty, t.ts, q.price
FROM trades t ASOF JOIN quotes q USING (symbol, ts);
