{{
    config(
        materialized="table",
        engine = "MergeTree()",
        tags=["staging"]
    )
}}

with drivers as (
    select * from {{ source("f1", "drivers") }}
)

select
    JSONExtractInt(_payload, 'meeting_key') as meeting_key,
    JSONExtractInt(_payload, 'session_key') as session_key,
    JSONExtractInt(_payload, 'driver_number') as driver_number,
    JSONExtractString(_payload, 'broadcast_name') as broadcast_name,
    JSONExtractString(_payload, 'full_name') as full_name,
    JSONExtractString(_payload, 'name_acronym') as name_acronym,
    JSONExtractString(_payload, 'first_name') as first_name,
    JSONExtractString(_payload, 'last_name') as last_name,
    JSONExtractString(_payload, 'team_name') as team_name,
    JSONExtractString(_payload, 'team_colour') as team_colour,
    JSONExtractString(_payload, 'country_code') as country_code,
    JSONExtractString(_payload, 'headshot_url') as headshot_url
from
    drivers
