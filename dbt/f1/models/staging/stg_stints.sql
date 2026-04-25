{{
    config(
        materialized="table",
        engine = "MergeTree()",
        tags=["staging"]
    )
}}

with stints as (
    select * from {{ source("f1", "stints") }}
)

select
    JSONExtractInt(_payload, 'meeting_key') as meeting_key,
    JSONExtractInt(_payload, 'session_key') as session_key,
    JSONExtractInt(_payload, 'driver_number') as driver_number,
    JSONExtractInt(_payload, 'stint_number') as stint_number,
    JSONExtractInt(_payload, 'lap_start') as lap_start,
    JSONExtractInt(_payload, 'lap_end') as lap_end,
    JSONExtractString(_payload, 'compound') as compound,
    JSONExtractInt(_payload, 'tyre_age_at_start') as tyre_age_at_start
from
    stints
