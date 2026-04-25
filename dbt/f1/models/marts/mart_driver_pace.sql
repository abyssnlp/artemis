{{
    config(
        materialized="table",
        engine="MergeTree()",
        tags=["marts"]
    )
}}

select
    session_key,
    driver_number,
    name_acronym,
    team_name,
    year,
    circuit_name,
    session_type,
    count()                     as lap_count,
    min(lap_duration)           as fastest_lap,
    median(lap_duration)        as median_lap,
    stddevSamp(lap_duration)    as stddev_lap,
    min(duration_sector_1)      as best_sector_1,
    min(duration_sector_2)      as best_sector_2,
    min(duration_sector_3)      as best_sector_3
from {{ ref('fct_laps') }}
where lap_duration is not null
  and not is_pit_out_lap
group by
    session_key,
    driver_number,
    name_acronym,
    team_name,
    year,
    circuit_name,
    session_type
