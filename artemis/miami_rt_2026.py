from __future__ import annotations

import json
import os
import time
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urljoin

from confluent_kafka import Producer
from livef1.adapters.realtime_client import RealF1Client
from livef1.data_processing.etl import function_map

LIVEF1_TOPICS = [
    "DriverList",
    "TimingDataF1",
    "Position.z",
    "CurrentTyres",
    "TyreStintSeries",
    "TeamRadio",
    "Heartbeat",
]

KAFKA_TOPIC_BY_FEED = {
    "DriverList": "f1.driver_list",
    "TimingDataF1": "f1.driver_timing",
    "Position.z": "f1.car_position",
    "CurrentTyres": "f1.tyres",
    "TyreStintSeries": "f1.tyres",
    "TeamRadio": "f1.team_radio",
    "Heartbeat": "f1.heartbeat",
}

DEFAULT_BOOTSTRAP_SERVERS = "localhost:19092"
SESSION_BASE_URL = "https://livetiming.formula1.com/static/"


def parse_team_radio_realtime(
    data: Iterable[tuple[str | None, dict[str, Any]]],
    session_key: int | None,
    **_: Any,
) -> Iterator[dict[str, Any]]:
    for timestamp, value in data:
        captures = value.get("Captures", [])
        if isinstance(captures, dict):
            captures = captures.values()

        for capture in captures:
            path = capture.get("Path")
            yield {
                "SessionKey": session_key,
                "timestamp": timestamp,
                **capture,
                "Path": urljoin(SESSION_BASE_URL, path) if path else None,
            }


function_map["TeamRadio"] = parse_team_radio_realtime


def as_dict(record: Any) -> dict[str, Any]:
    if hasattr(record, "model_dump"):
        return record.model_dump()
    if hasattr(record, "dict"):
        return record.dict()
    if isinstance(record, dict):
        return record
    if hasattr(record, "__dict__"):
        return vars(record)
    return {"value": record}


def json_text(value: Any) -> str:
    return json.dumps(value, default=str, ensure_ascii=False, separators=(",", ":"))


def parse_event_time(value: Any, received_at: datetime) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if value in (None, ""):
        return received_at

    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"

    try:
        return datetime.fromisoformat(text).astimezone(UTC)
    except ValueError:
        return received_at


def clickhouse_dt64(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:23]


def to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_bool(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, str):
        return int(value.strip().lower() in {"1", "true", "yes"})
    return int(bool(value))


def nested_value(record: dict[str, Any], *keys: str) -> Any:
    current: Any = record
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def sector_value(record: dict[str, Any], sector: int, key: str) -> Any:
    return record.get(f"Sectors_{sector}_{key}") or nested_value(
        record, "Sectors", str(sector - 1), key
    )


def base_row(
    source_topic: str,
    record: dict[str, Any],
    received_at: datetime,
) -> dict[str, Any]:
    timestamp = record.get("timestamp") or record.get("Utc") or record.get("UTC")
    return {
        "event_time": clickhouse_dt64(parse_event_time(timestamp, received_at)),
        "source_topic": source_topic,
        "session_key": to_int(record.get("SessionKey") or record.get("session_key")),
        "raw": json_text(record),
    }


def normalize_driver_list(
    record: dict[str, Any],
    received_at: datetime,
) -> dict[str, Any]:
    driver_no = record.get("RacingNumber") or record.get("DriverNo")
    return {
        **base_row("DriverList", record, received_at),
        "driver_no": str(driver_no or ""),
        "broadcast_name": record.get("BroadcastName"),
        "full_name": record.get("FullName") or record.get("Full_Name"),
        "tla": record.get("Tla"),
        "team_name": record.get("TeamName"),
        "team_colour": record.get("TeamColour"),
    }


def normalize_timing(
    record: dict[str, Any],
    received_at: datetime,
) -> dict[str, Any]:
    interval = record.get("IntervalToPositionAhead_Value") or nested_value(
        record, "IntervalToPositionAhead", "Value"
    )
    catching = record.get("IntervalToPositionAhead_Catching") or nested_value(
        record, "IntervalToPositionAhead", "Catching"
    )
    best_lap = record.get("BestLapTime_Lap") or nested_value(
        record, "BestLapTime", "Lap"
    )
    best_lap_time = record.get("BestLapTime_Value") or nested_value(
        record, "BestLapTime", "Value"
    )
    last_lap_time = record.get("LastLapTime_Value") or nested_value(
        record, "LastLapTime", "Value"
    )

    return {
        **base_row("TimingDataF1", record, received_at),
        "driver_no": str(record.get("DriverNo") or record.get("RacingNumber") or ""),
        "position": to_int(record.get("Position")),
        "line": to_int(record.get("Line")),
        "lap": to_int(record.get("NumberOfLaps")),
        "gap_to_leader": record.get("GapToLeader"),
        "interval_to_ahead": interval,
        "catching": to_bool(catching),
        "in_pit": to_bool(record.get("InPit")),
        "retired": to_bool(record.get("Retired")),
        "pit_stops": to_int(record.get("NumberOfPitStops")),
        "last_lap_time": last_lap_time,
        "best_lap_time": best_lap_time,
        "best_lap": to_int(best_lap),
        "sector1_time": sector_value(record, 1, "Value"),
        "sector1_status": sector_value(record, 1, "Status"),
        "sector2_time": sector_value(record, 2, "Value"),
        "sector2_status": sector_value(record, 2, "Status"),
        "sector3_time": sector_value(record, 3, "Value"),
        "sector3_status": sector_value(record, 3, "Status"),
        "speed_i1": to_int(record.get("Speeds_I1_Value")),
        "speed_i2": to_int(record.get("Speeds_I2_Value")),
        "speed_fl": to_int(record.get("Speeds_FL_Value")),
        "speed_st": to_int(record.get("Speeds_ST_Value")),
    }


def normalize_position(
    record: dict[str, Any],
    received_at: datetime,
) -> dict[str, Any]:
    return {
        **base_row("Position.z", record, received_at),
        "driver_no": str(record.get("DriverNo") or ""),
        "utc": clickhouse_dt64(parse_event_time(record.get("Utc"), received_at)),
        "x": to_float(record.get("X")),
        "y": to_float(record.get("Y")),
        "z": to_float(record.get("Z")),
        "status": record.get("Status") or record.get("status"),
    }


def normalize_tyres(
    source_topic: str,
    record: dict[str, Any],
    received_at: datetime,
) -> dict[str, Any]:
    return {
        **base_row(source_topic, record, received_at),
        "feed": "stint" if source_topic == "TyreStintSeries" else "current",
        "driver_no": str(record.get("DriverNo") or ""),
        "pit_count": to_int(record.get("PitCount")),
        "compound": record.get("Compound"),
        "is_new": to_bool(record.get("New")),
        "tyres_not_changed": to_bool(record.get("TyresNotChanged")),
        "total_laps": to_int(record.get("TotalLaps")),
        "start_laps": to_int(record.get("StartLaps")),
    }


def normalize_team_radio(
    record: dict[str, Any],
    received_at: datetime,
) -> dict[str, Any]:
    driver_no = record.get("RacingNumber") or record.get("DriverNo")
    path = record.get("Path")
    return {
        **base_row("TeamRadio", record, received_at),
        "driver_no": str(driver_no or ""),
        "utc": clickhouse_dt64(parse_event_time(record.get("Utc"), received_at)),
        "message": record.get("Message") or record.get("Transcript"),
        "audio_url": path,
    }


def normalize_heartbeat(
    record: dict[str, Any],
    received_at: datetime,
) -> dict[str, Any]:
    return {
        **base_row("Heartbeat", record, received_at),
        "utc": clickhouse_dt64(parse_event_time(record.get("utc"), received_at)),
    }


def normalize_records(
    source_topic: str,
    records: list[Any],
    received_at: datetime,
) -> list[dict[str, Any]]:
    normalizers = {
        "DriverList": lambda row: normalize_driver_list(row, received_at),
        "TimingDataF1": lambda row: normalize_timing(row, received_at),
        "Position.z": lambda row: normalize_position(row, received_at),
        "CurrentTyres": lambda row: normalize_tyres("CurrentTyres", row, received_at),
        "TyreStintSeries": lambda row: normalize_tyres(
            "TyreStintSeries", row, received_at
        ),
        "TeamRadio": lambda row: normalize_team_radio(row, received_at),
        "Heartbeat": lambda row: normalize_heartbeat(row, received_at),
    }
    normalize = normalizers[source_topic]
    return [normalize(as_dict(record)) for record in records]


def delivery_report(error: Any, message: Any) -> None:
    if error is not None:
        print(f"Kafka delivery failed: {error}", flush=True)


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS
            ),
            "client.id": "livef1-miami-2026",
            "linger.ms": int(os.getenv("KAFKA_LINGER_MS", "20")),
            "compression.type": os.getenv("KAFKA_COMPRESSION_TYPE", "lz4"),
        }
    )


def main() -> None:
    producer = build_producer()
    client = RealF1Client(topics=LIVEF1_TOPICS)
    last_flush = time.monotonic()

    @client.callback("kafka_ingest")
    async def publish_to_kafka(records):
        nonlocal last_flush

        if not records:
            return

        received_at = datetime.now(UTC)
        for source_topic, source_records in records.items():
            target_topic = KAFKA_TOPIC_BY_FEED.get(source_topic)
            if not target_topic:
                continue

            for row in normalize_records(source_topic, source_records, received_at):
                key = row.get("driver_no") or row.get("source_topic")
                producer.produce(
                    target_topic,
                    key=str(key).encode(),
                    value=json_text(row).encode(),
                    callback=delivery_report,
                )

        producer.poll(0)
        if time.monotonic() - last_flush >= 0.5:
            producer.flush(0.25)
            last_flush = time.monotonic()

    print(
        f"Starting LiveF1 -> Redpanda ingest for topics: {', '.join(LIVEF1_TOPICS)}",
        flush=True,
    )
    client.run()


if __name__ == "__main__":
    main()
