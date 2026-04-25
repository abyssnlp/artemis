{{
    config(
        materialized="table",
        engine="MergeTree()",
        tags=["marts"]
    )
}}

select
    session_key,
    compound,
    year,
    circuit_name,
    session_type,
    count()              as sample_size,
    min(lap_duration)    as fastest_lap,
    avg(lap_duration)    as avg_lap_duration,
    stddevSamp(lap_duration) as stddev_lap
from {{ ref('fct_laps') }}
where lap_duration is not null
  and compound is not null
  and not is_pit_out_lap
group by
    session_key,
    compound,
    year,
    circuit_name,
    session_type
