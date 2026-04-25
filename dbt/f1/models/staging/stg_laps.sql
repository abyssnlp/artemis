{{
    config(
        materialized="table",
        engine = "MergeTree()",
        tags=["staging"]
    )
}}

with laps as (
    select * from {{ source("f1", "laps") }}
)

select
    JSONExtractInt(_payload, 'meeting_key') as meeting_key,
    JSONExtractInt(_payload, 'session_key') as session_key,
    JSONExtractInt(_payload, 'driver_number') as driver_number,
    JSONExtractInt(_payload, 'lap_number') as lap_number,
    parseDateTime64BestEffort(JSONExtractString(_payload, 'date_start')) as date_start,
    JSONExtractFloat(_payload, 'duration_sector_1') as duration_sector_1,
    JSONExtractFloat(_payload, 'duration_sector_2') as duration_sector_2,
    JSONExtractFloat(_payload, 'duration_sector_3') as duration_sector_3,
    JSONExtractInt(_payload, 'i1_speed') as i1_speed,
    JSONExtractInt(_payload, 'i2_speed') as i2_speed,
    JSONExtractBool(_payload, 'is_pit_out_lap') as is_pit_out_lap,
    JSONExtractFloat(_payload, 'lap_duration') as lap_duration,
    JSONExtractArrayRaw(_payload, 'segments_sector_1') as segments_sector_1,
    JSONExtractArrayRaw(_payload, 'segments_sector_2') as segments_sector_2,
    JSONExtractArrayRaw(_payload, 'segments_sector_3') as segments_sector_3,
    JSONExtractInt(_payload, 'st_speed') as st_speed
from
    laps
