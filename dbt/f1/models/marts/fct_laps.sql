{{
    config(
        materialized="table",
        engine="MergeTree()",
        tags=["marts"]
    )
}}

with laps as (
    select * from {{ ref('int_laps_with_stint') }}
),

drivers as (
    select
        session_key   as drv_session_key,
        driver_number as drv_driver_number,
        name_acronym,
        full_name,
        team_name,
        team_colour
    from {{ ref('stg_drivers') }}
),

sessions as (
    select
        session_key as sess_session_key,
        session_name,
        session_type
    from {{ ref('stg_sessions') }}
),

meetings as (
    select
        meeting_key as mtg_meeting_key,
        meeting_name,
        location,
        circuit_name,
        year
    from {{ ref('stg_meetings') }}
)

select
    laps.meeting_key        as meeting_key,
    laps.session_key        as session_key,
    laps.driver_number      as driver_number,
    laps.lap_number         as lap_number,
    laps.date_start         as date_start,
    laps.lap_duration       as lap_duration,
    laps.duration_sector_1  as duration_sector_1,
    laps.duration_sector_2  as duration_sector_2,
    laps.duration_sector_3  as duration_sector_3,
    laps.i1_speed           as i1_speed,
    laps.i2_speed           as i2_speed,
    laps.st_speed           as st_speed,
    laps.is_pit_out_lap     as is_pit_out_lap,
    laps.stint_number       as stint_number,
    laps.compound           as compound,
    laps.tyre_age           as tyre_age,
    drivers.name_acronym    as name_acronym,
    drivers.full_name       as full_name,
    drivers.team_name       as team_name,
    drivers.team_colour     as team_colour,
    sessions.session_name   as session_name,
    sessions.session_type   as session_type,
    meetings.meeting_name   as meeting_name,
    meetings.location       as location,
    meetings.circuit_name   as circuit_name,
    meetings.year           as year
from laps
left join drivers  on  laps.session_key   = drivers.drv_session_key
                   and laps.driver_number = drivers.drv_driver_number
left join sessions on  laps.session_key   = sessions.sess_session_key
left join meetings on  laps.meeting_key   = meetings.mtg_meeting_key
