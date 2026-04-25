{{
    config(
        materialized="table",
        engine = "MergeTree()",
        tags=["staging"]
    )
}}

with meetings as (
    select * from {{ source("f1", "meetings") }}
)

select
    JSONExtractInt(_payload, 'meeting_key') as meeting_key,
    JSONExtractString(_payload, 'meeting_name') as meeting_name,
    JSONExtractString(_payload, 'country_name') as country,
    JSONExtractString(_payload, 'location') as location,
    JSONExtractString(_payload, 'circuit_short_name') as circuit_name,
    JSONExtractInt(_payload, 'year') as year
from
    meetings
