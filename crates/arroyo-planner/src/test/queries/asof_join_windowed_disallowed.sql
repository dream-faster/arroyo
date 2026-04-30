--fail=ASOF JOIN does not support windowed inputs
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

SELECT t.window, t.qty_sum, q.price
FROM (
    SELECT TUMBLE(INTERVAL '1' minute) AS window, symbol, SUM(qty) AS qty_sum
    FROM trades
    GROUP BY 1, 2
) t
ASOF JOIN quotes q
MATCH_CONDITION (t.window.end >= q.ts)
ON t.symbol = q.symbol;
