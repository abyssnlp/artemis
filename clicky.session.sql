drop table default.drivers;
drop table default.laps;
drop table default.meetings;
drop table default.sessions;
drop table default.stints;


select JSONExtractKeys(_payload) from raw.meetings;
select
    JSONExtractInt(_payload, 'meeting_key') as meeting_key,
    JSONExtractString(_payload, 'meeting_name') as meeting_name,
    JSONExtractString(_payload, 'country_name') as country,
    JSONExtractString(_payload, 'location') as location,
    JSONExtractString(_payload, 'circuit_short_name') as circuit_name,
    JSONExtractInt(_payload, 'year') as year
from
    raw.meetings;


select JSONExtractKeys(_payload) from raw.sessions;

select
    JSONExtractInt(_payload, 'meeting_key') as meeting_key,
    JSONExtractInt(_payload, 'session_key') as session_key,
    JSONExtractString(_payload, 'session_type') as session_type,
    JSONExtractString(_payload, 'session_name') as session_name,
    parseDateTime64BestEffort(JSONExtractString(_payload, 'date_start')) as date_start,
    parseDateTime64BestEffort(JSONExtractString(_payload, 'date_end')) as date_end
from
    raw.sessions;

select JSONExtractKeys(_payload) from raw.laps;

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
    raw.laps
;
