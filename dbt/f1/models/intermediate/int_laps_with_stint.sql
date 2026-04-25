{{
    config(
        materialized="table",
        engine="MergeTree()",
        tags=["intermediate"]
    )
}}

with laps as (
    select * from {{ ref('stg_laps') }}
),

stints as (
    select * from {{ ref('stg_stints') }}
)

select
    laps.meeting_key,
    laps.session_key,
    laps.driver_number,
    laps.lap_number,
    laps.date_start,
    laps.duration_sector_1,
    laps.duration_sector_2,
    laps.duration_sector_3,
    laps.i1_speed,
    laps.i2_speed,
    laps.is_pit_out_lap,
    laps.lap_duration,
    laps.segments_sector_1,
    laps.segments_sector_2,
    laps.segments_sector_3,
    laps.st_speed,
    stints.stint_number,
    stints.compound,
    stints.tyre_age_at_start + laps.lap_number - stints.lap_start as tyre_age
from laps
join stints
    on  laps.session_key   = stints.session_key
    and laps.driver_number = stints.driver_number
where laps.lap_number between stints.lap_start and stints.lap_end
