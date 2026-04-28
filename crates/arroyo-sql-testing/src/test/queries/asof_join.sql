CREATE TABLE trades (
  timestamp TIMESTAMP,
  symbol TEXT,
  quantity BIGINT
) WITH (
  connector = 'single_file',
  path = '$input_dir/trades.json',
  format = 'json',
  type = 'source',
  event_time_field = 'timestamp'
);

CREATE TABLE quotes (
  timestamp TIMESTAMP,
  symbol TEXT,
  price BIGINT
) WITH (
  connector = 'single_file',
  path = '$input_dir/quotes.json',
  format = 'json',
  type = 'source',
  event_time_field = 'timestamp'
);

CREATE TABLE output (
  trade_time TIMESTAMP,
  symbol TEXT,
  quantity BIGINT,
  quote_time TIMESTAMP,
  price BIGINT
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);

INSERT INTO output
SELECT
  t.timestamp AS trade_time,
  t.symbol,
  t.quantity,
  q.timestamp AS quote_time,
  q.price
FROM trades t
ASOF JOIN quotes q
  MATCH_CONDITION (t.timestamp >= q.timestamp)
  ON t.symbol = q.symbol;
