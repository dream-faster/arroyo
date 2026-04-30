CREATE TABLE quotes (
    exchange TEXT,
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
    exchange TEXT,
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

SELECT t.exchange, t.symbol, t.qty, q.price
FROM trades t ASOF JOIN quotes q
MATCH_CONDITION (t.ts >= q.ts)
ON t.exchange = q.exchange AND t.symbol = q.symbol;
