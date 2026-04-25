{{
    config(
        materialized="table",
        engine = "MergeTree()",
        tags=["staging"]
    )
}}

with sessions as (
    select * from {{ source("f1", "sessions") }}
)

select
    JSONExtractInt(_payload, 'meeting_key') as meeting_key,
    JSONExtractInt(_payload, 'session_key') as session_key,
    JSONExtractString(_payload, 'session_type') as session_type,
    JSONExtractString(_payload, 'session_name') as session_name,
    parseDateTime64BestEffort(JSONExtractString(_payload, 'date_start')) as date_start,
    parseDateTime64BestEffort(JSONExtractString(_payload, 'date_end')) as date_end
from
    sessions
