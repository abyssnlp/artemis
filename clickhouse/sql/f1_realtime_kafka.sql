CREATE DATABASE IF NOT EXISTS f1_rt;

CREATE TABLE IF NOT EXISTS f1_rt.driver_list_kafka
(
    event_time DateTime64(3, 'UTC'),
    source_topic LowCardinality(String),
    session_key Nullable(UInt64),
    driver_no String,
    broadcast_name Nullable(String),
    full_name Nullable(String),
    tla Nullable(String),
    team_name Nullable(String),
    team_colour Nullable(String),
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'f1.driver_list',
    kafka_group_name = 'clickhouse_f1_driver_list',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS f1_rt.driver_list
(
    event_time DateTime64(3, 'UTC'),
    source_topic LowCardinality(String),
    session_key Nullable(UInt64),
    driver_no String,
    broadcast_name Nullable(String),
    full_name Nullable(String),
    tla Nullable(String),
    team_name Nullable(String),
    team_colour Nullable(String),
    raw String
)
ENGINE = MergeTree
ORDER BY (driver_no, event_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS f1_rt.driver_list_mv
TO f1_rt.driver_list
AS SELECT * FROM f1_rt.driver_list_kafka;

CREATE TABLE IF NOT EXISTS f1_rt.driver_timing_kafka
(
    event_time DateTime64(3, 'UTC'),
    source_topic LowCardinality(String),
    session_key Nullable(UInt64),
    driver_no String,
    position Nullable(UInt8),
    line Nullable(UInt8),
    lap Nullable(UInt16),
    gap_to_leader Nullable(String),
    interval_to_ahead Nullable(String),
    catching Nullable(UInt8),
    in_pit Nullable(UInt8),
    retired Nullable(UInt8),
    pit_stops Nullable(UInt8),
    last_lap_time Nullable(String),
    best_lap_time Nullable(String),
    best_lap Nullable(UInt16),
    sector1_time Nullable(String),
    sector1_status Nullable(String),
    sector2_time Nullable(String),
    sector2_status Nullable(String),
    sector3_time Nullable(String),
    sector3_status Nullable(String),
    speed_i1 Nullable(UInt16),
    speed_i2 Nullable(UInt16),
    speed_fl Nullable(UInt16),
    speed_st Nullable(UInt16),
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'f1.driver_timing',
    kafka_group_name = 'clickhouse_f1_driver_timing',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 2,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS f1_rt.driver_timing
(
    event_time DateTime64(3, 'UTC'),
    source_topic LowCardinality(String),
    session_key Nullable(UInt64),
    driver_no String,
    position Nullable(UInt8),
    line Nullable(UInt8),
    lap Nullable(UInt16),
    gap_to_leader Nullable(String),
    interval_to_ahead Nullable(String),
    catching Nullable(UInt8),
    in_pit Nullable(UInt8),
    retired Nullable(UInt8),
    pit_stops Nullable(UInt8),
    last_lap_time Nullable(String),
    best_lap_time Nullable(String),
    best_lap Nullable(UInt16),
    sector1_time Nullable(String),
    sector1_status Nullable(String),
    sector2_time Nullable(String),
    sector2_status Nullable(String),
    sector3_time Nullable(String),
    sector3_status Nullable(String),
    speed_i1 Nullable(UInt16),
    speed_i2 Nullable(UInt16),
    speed_fl Nullable(UInt16),
    speed_st Nullable(UInt16),
    raw String
)
ENGINE = MergeTree
ORDER BY (event_time, driver_no);

CREATE MATERIALIZED VIEW IF NOT EXISTS f1_rt.driver_timing_mv
TO f1_rt.driver_timing
AS SELECT * FROM f1_rt.driver_timing_kafka;

CREATE TABLE IF NOT EXISTS f1_rt.car_position_kafka
(
    event_time DateTime64(3, 'UTC'),
    source_topic LowCardinality(String),
    session_key Nullable(UInt64),
    driver_no String,
    utc DateTime64(3, 'UTC'),
    x Nullable(Float64),
    y Nullable(Float64),
    z Nullable(Float64),
    status Nullable(String),
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'f1.car_position',
    kafka_group_name = 'clickhouse_f1_car_position',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 2,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS f1_rt.car_position
(
    event_time DateTime64(3, 'UTC'),
    source_topic LowCardinality(String),
    session_key Nullable(UInt64),
    driver_no String,
    utc DateTime64(3, 'UTC'),
    x Nullable(Float64),
    y Nullable(Float64),
    z Nullable(Float64),
    status Nullable(String),
    raw String
)
ENGINE = MergeTree
ORDER BY (event_time, driver_no);

CREATE MATERIALIZED VIEW IF NOT EXISTS f1_rt.car_position_mv
TO f1_rt.car_position
AS SELECT * FROM f1_rt.car_position_kafka;

CREATE TABLE IF NOT EXISTS f1_rt.tyres_kafka
(
    event_time DateTime64(3, 'UTC'),
    source_topic LowCardinality(String),
    session_key Nullable(UInt64),
    feed LowCardinality(String),
    driver_no String,
    pit_count Nullable(UInt8),
    compound Nullable(String),
    is_new Nullable(UInt8),
    tyres_not_changed Nullable(UInt8),
    total_laps Nullable(UInt16),
    start_laps Nullable(UInt16),
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'f1.tyres',
    kafka_group_name = 'clickhouse_f1_tyres',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS f1_rt.tyres
(
    event_time DateTime64(3, 'UTC'),
    source_topic LowCardinality(String),
    session_key Nullable(UInt64),
    feed LowCardinality(String),
    driver_no String,
    pit_count Nullable(UInt8),
    compound Nullable(String),
    is_new Nullable(UInt8),
    tyres_not_changed Nullable(UInt8),
    total_laps Nullable(UInt16),
    start_laps Nullable(UInt16),
    raw String
)
ENGINE = MergeTree
ORDER BY (event_time, driver_no, feed);

CREATE MATERIALIZED VIEW IF NOT EXISTS f1_rt.tyres_mv
TO f1_rt.tyres
AS SELECT * FROM f1_rt.tyres_kafka;

CREATE TABLE IF NOT EXISTS f1_rt.team_radio_kafka
(
    event_time DateTime64(3, 'UTC'),
    source_topic LowCardinality(String),
    session_key Nullable(UInt64),
    driver_no String,
    utc DateTime64(3, 'UTC'),
    message Nullable(String),
    audio_url Nullable(String),
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'f1.team_radio',
    kafka_group_name = 'clickhouse_f1_team_radio',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS f1_rt.team_radio
(
    event_time DateTime64(3, 'UTC'),
    source_topic LowCardinality(String),
    session_key Nullable(UInt64),
    driver_no String,
    utc DateTime64(3, 'UTC'),
    message Nullable(String),
    audio_url Nullable(String),
    raw String
)
ENGINE = MergeTree
ORDER BY (event_time, driver_no);

CREATE MATERIALIZED VIEW IF NOT EXISTS f1_rt.team_radio_mv
TO f1_rt.team_radio
AS SELECT * FROM f1_rt.team_radio_kafka;

CREATE TABLE IF NOT EXISTS f1_rt.heartbeat_kafka
(
    event_time DateTime64(3, 'UTC'),
    source_topic LowCardinality(String),
    session_key Nullable(UInt64),
    utc DateTime64(3, 'UTC'),
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'f1.heartbeat',
    kafka_group_name = 'clickhouse_f1_heartbeat',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS f1_rt.heartbeat
(
    event_time DateTime64(3, 'UTC'),
    source_topic LowCardinality(String),
    session_key Nullable(UInt64),
    utc DateTime64(3, 'UTC'),
    raw String
)
ENGINE = MergeTree
ORDER BY event_time;

CREATE MATERIALIZED VIEW IF NOT EXISTS f1_rt.heartbeat_mv
TO f1_rt.heartbeat
AS SELECT * FROM f1_rt.heartbeat_kafka;

CREATE VIEW IF NOT EXISTS f1_rt.v_driver_dim
AS
SELECT
    driver_no,
    argMax(broadcast_name, event_time) AS broadcast_name,
    argMax(full_name, event_time) AS full_name,
    argMax(tla, event_time) AS tla,
    argMax(team_name, event_time) AS team_name,
    argMax(team_colour, event_time) AS team_colour
FROM f1_rt.driver_list
GROUP BY driver_no;

CREATE VIEW IF NOT EXISTS f1_rt.v_live_positions
AS
SELECT
    t.event_time,
    t.driver_no,
    d.tla,
    d.full_name,
    d.team_name,
    t.position,
    t.lap,
    t.gap_to_leader,
    t.interval_to_ahead,
    t.in_pit,
    t.retired,
    t.pit_stops
FROM
(
    SELECT *
    FROM f1_rt.driver_timing
    ORDER BY event_time DESC
    LIMIT 1 BY driver_no
) AS t
LEFT JOIN f1_rt.v_driver_dim AS d ON t.driver_no = d.driver_no
ORDER BY t.position ASC;

CREATE VIEW IF NOT EXISTS f1_rt.v_current_sector_times
AS
SELECT
    t.event_time,
    t.driver_no,
    d.tla,
    d.team_name,
    t.position,
    t.lap,
    t.sector1_time,
    t.sector1_status,
    t.sector2_time,
    t.sector2_status,
    t.sector3_time,
    t.sector3_status,
    t.last_lap_time,
    t.best_lap_time
FROM
(
    SELECT *
    FROM f1_rt.driver_timing
    ORDER BY event_time DESC
    LIMIT 1 BY driver_no
) AS t
LEFT JOIN f1_rt.v_driver_dim AS d ON t.driver_no = d.driver_no
ORDER BY t.position ASC;

CREATE VIEW IF NOT EXISTS f1_rt.v_latest_car_position
AS
SELECT
    p.event_time,
    p.driver_no,
    d.tla,
    d.team_name,
    p.x,
    p.y,
    p.z,
    p.status
FROM
(
    SELECT *
    FROM f1_rt.car_position
    ORDER BY event_time DESC
    LIMIT 1 BY driver_no
) AS p
LEFT JOIN f1_rt.v_driver_dim AS d ON p.driver_no = d.driver_no;

CREATE VIEW IF NOT EXISTS f1_rt.v_tyre_strategy
AS
SELECT
    t.event_time,
    t.driver_no,
    d.tla,
    d.team_name,
    t.feed,
    t.pit_count,
    t.compound,
    t.is_new,
    t.tyres_not_changed,
    t.total_laps,
    t.start_laps
FROM f1_rt.tyres AS t
LEFT JOIN f1_rt.v_driver_dim AS d ON t.driver_no = d.driver_no
ORDER BY t.driver_no, t.event_time DESC;

CREATE VIEW IF NOT EXISTS f1_rt.v_team_radio_stream
AS
SELECT
    r.event_time,
    r.driver_no,
    d.tla,
    d.full_name AS driver,
    d.team_name AS team,
    coalesce(r.message, r.audio_url) AS message,
    r.audio_url
FROM f1_rt.team_radio AS r
LEFT JOIN f1_rt.v_driver_dim AS d ON r.driver_no = d.driver_no
ORDER BY r.event_time DESC;
