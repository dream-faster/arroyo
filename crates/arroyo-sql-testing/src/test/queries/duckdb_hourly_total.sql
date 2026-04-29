--duckdb
CREATE TABLE cars(
  timestamp TIMESTAMP,
  driver_id BIGINT,
  event_type TEXT,
  location TEXT
) WITH (
  connector = 'single_file',
  path = '$input_dir/cars.json',
  format = 'json',
  type = 'source',
  event_time_field = 'timestamp'
);
CREATE TABLE duckdb_hourly_total (
  hour TIMESTAMP,
  count BIGINT
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);
INSERT INTO duckdb_hourly_total
SELECT window.start as hour, count
FROM (
SELECT TUMBLE(INTERVAL '1' HOUR) as window, COUNT(*) as count
FROM cars
GROUP BY 1);
