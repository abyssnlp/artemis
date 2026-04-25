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
    stint_number,
    compound,
    year,
    circuit_name,
    session_type,
    count()                                                         as lap_count,
    min(lap_duration)                                               as best_lap,
    avg(lap_duration)                                               as avg_lap,
    tupleElement(simpleLinearRegression(tyre_age, lap_duration), 1) as degradation_slope
from {{ ref('fct_laps') }}
where lap_duration is not null
  and tyre_age     is not null
  and compound     is not null
  and not is_pit_out_lap
group by
    session_key,
    driver_number,
    name_acronym,
    team_name,
    stint_number,
    compound,
    year,
    circuit_name,
    session_type
having count() >= 2
