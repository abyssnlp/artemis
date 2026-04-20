from __future__ import annotations

from datetime import datetime, time
from typing import Final, List, Optional

import clickhouse_connect
from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator, Connection, Context
from pydantic import BaseModel, ConfigDict, HttpUrl, TypeAdapter, ValidationError


class Meeting(BaseModel):
    model_config = ConfigDict(extra="forbid")
    circuit_key: int
    circuit_info_url: HttpUrl
    circuit_image: HttpUrl
    circuit_short_name: str
    circuit_type: str
    country_code: str
    country_flag: HttpUrl
    country_key: int
    country_name: str
    date_end: datetime
    date_start: datetime
    gmt_offset: time
    is_cancelled: bool
    location: str

    meeting_key: int
    meeting_name: str
    meeting_official_name: str
    year: int


class Session(BaseModel):
    model_config = ConfigDict(extra="forbid")
    circuit_key: int
    circuit_short_name: str
    country_code: str
    country_key: int
    country_name: str

    date_end: datetime
    date_start: datetime
    gmt_offset: time
    is_cancelled: bool
    location: str

    meeting_key: int
    session_key: int
    session_name: str
    session_type: str
    year: int


class Driver(BaseModel):
    model_config = ConfigDict(extra="forbid")

    broadcast_name: str
    driver_number: int
    first_name: str
    last_name: str
    full_name: str
    headshot_url: HttpUrl
    meeting_key: int
    session_key: int
    name_acronym: str
    team_colour: str
    team_name: str
    country_code: Optional[str]


class Lap(BaseModel):
    model_config = ConfigDict(extra="forbid")

    date_start: datetime
    driver_number: int
    duration_sector_1: Optional[float]
    duration_sector_2: Optional[float]
    duration_sector_3: Optional[float]

    i1_speed: Optional[int]
    i2_speed: Optional[int]
    st_speed: Optional[int]

    is_pit_out_lap: bool
    lap_duration: Optional[float]
    lap_number: int
    meeting_key: int
    session_key: int

    segments_sector_1: Optional[List[Optional[int]]]
    segments_sector_2: Optional[List[Optional[int]]]
    segments_sector_3: Optional[List[Optional[int]]]


class Stint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    compound: str
    driver_number: int
    lap_start: int
    lap_end: int
    meeting_key: int
    session_key: int
    stint_number: int
    tyre_age_at_start: int


ENDPOINT_SCHEMAS: Final[dict[str, type[BaseModel]]] = {
    "meetings": Meeting,
    "sessions": Session,
    "drivers": Driver,
    "laps": Lap,
    "stints": Stint,
}


class F1ValidateOperator(BaseOperator):
    def __init__(
        self,
        conn_id: str,
        endpoint: str,
        database: str,
        table: str,
        batch_size: int = 1_000,
        max_logged_errors: int = 10,
        failure_threshold: int | None = None,
        strict: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.endpoint = endpoint
        self.database = database
        self.table = table
        self.batch_size = batch_size
        self.max_logged_errors = max_logged_errors
        self.failure_threshold = failure_threshold
        self.strict = strict

    def _get_ch_client(self):
        conn = Connection.get(self.conn_id)
        return clickhouse_connect.get_client(
            host=conn.host,
            port=conn.port,
            username=conn.login,
            password=conn.password,
            database=self.database,
        )

    def _validate_batch(
        self, adapter: TypeAdapter, payloads: List[str]
    ) -> List[tuple[str, List[dict]]]:
        """Vectorized validation"""
        json_array = b"[" + b",".join(p.encode("utf-8") for p in payloads) + b"]"
        try:
            adapter.validate_json(json_array)
            return []
        except ValidationError as e:
            errors = {}
            for err in e.errors():
                loc = err.get("loc", ())
                if not loc or not isinstance(loc[0], int):
                    continue
                idx = loc[0]
                errors.setdefault(idx, []).append(err)
            return [(payloads[idx], errs) for idx, errs in errors.items()]

    def execute(self, context: Context) -> dict[str, int]:
        run_id = context.get("dag_run").run_id  # type: ignore
        self.log.info(f"Starting validation for run_id: {run_id}")

        schema = ENDPOINT_SCHEMAS.get(self.endpoint)
        adapter = TypeAdapter(List[schema])
        client = self._get_ch_client()
        client.set_client_setting("max_block_size", self.batch_size)

        sql = f"SELECT _payload FROM {self.database}.{self.table} WHERE _run_id = '{run_id}'"

        total = 0
        failed = 0
        logged = 0

        with client.query_column_block_stream(sql) as stream:
            for block in stream:
                payloads: List[str] = list(block[0])
                if not payloads:
                    continue

                total += len(payloads)
                bad = self._validate_batch(adapter, payloads)
                failed += len(bad)

                for payload, errs in bad:
                    self.log.error(
                        f"Validation error, {self.endpoint} - Payload: {payload[:500]}, Errors: {errs}"
                    )
                    logged += 1

                if (
                    self.failure_threshold is not None
                    and failed >= self.failure_threshold
                ):
                    self.log.error(
                        f"Failure threshold of {self.failure_threshold} reached. Failing task."
                    )
                    break
        self.log.info(
            f"Validation completed for run_id: {run_id}. Total: {total}, Failed: {failed}, Logged: {logged}"
        )

        if failed > 0 and self.strict:
            raise AirflowException(
                f"{failed}/{total} records failed validation for endpoint '{self.endpoint}'."
            )

        return {"checked": total, "failed": failed}
