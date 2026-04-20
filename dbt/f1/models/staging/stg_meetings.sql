with meetings as (
    select * from {{ source("f1", "meetings") }}
)

select

from
    meetings
