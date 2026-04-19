from __future__ import annotations

import json
import typing
from dataclasses import dataclass
from datetime import datetime, timezone

import clickhouse_connect
import requests
import requests.adapters
from airflow.sdk import BaseOperator, Connection, Context, Variable
from urllib3.util.retry import Retry


@dataclass(slots=True)
class F1Params:
    f1_meeting_key: int
    f1_session_key: int


def ensure_raw_tables(database: str, **context) -> None:
    conn = Connection.get("clickhouse_default")
    client = clickhouse_connect.get_client(
        host=conn.host, port=conn.port, username=conn.login, password=conn.password
    )

    client.command(f"CREATE DATABASE IF NOT EXISTS {database}")

    for table in ["meetings", "sessions", "drivers", "laps", "stints"]:
        client.command(f"""
            CREATE TABLE IF NOT EXISTS {database}.{table} (
                _payload String,
                _source_endpoint String,
                _ingested_at DateTime64(3),
                _run_id String
                )
                ENGINE = MergeTree()
                ORDER BY (_ingested_at)
        """)


class F1IngestOperator(BaseOperator):
    """Ingest F1 data from the API"""

    BASE_URL: typing.Final[str] = "https://api.openf1.org/v1"

    def __init__(
        self,
        endpoint: str,
        database: str,
        table: str,
        needs_meeting: bool = False,
        needs_session: bool = True,
        extra_params: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.database = database
        self.table = table
        self.needs_meeting = needs_meeting
        self.needs_session = needs_session
        self.extra_params = extra_params or {}
        self.add_inlets([f"{self.BASE_URL}.{self.endpoint}"])
        self.add_outlets([f"{self.database}.{self.table}"])

    def _get_session(self) -> requests.Session:
        retry = Retry(
            total=5,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=True,
        )
        adapter = requests.adapters.HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({"Accept": "application/json"})
        return session

    def _get_ch_client(self):
        conn = Connection.get("clickhouse_default")
        return clickhouse_connect.get_client(
            host=conn.host,
            port=conn.port,
            username=conn.login,
            password=conn.password,
            database=self.database,
        )

    def _get_params(self) -> F1Params:
        params = Variable.get("f1", deserialize_json=True)
        return F1Params(**params)

    def _get_data(self, url: str, params: dict) -> dict:
        session = self._get_session()
        self.log.info(f"Making request to {url} with params {params}")
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        self.log.info(f"Received data: {data}")
        return data

    def _insert_data_into_ch(self, data: dict, context: Context) -> None:
        """multi-statement transactions are not supported in CH"""
        # metadata fields
        now = datetime.now(timezone.utc).isoformat()
        run_id = context.get("dag_run").run_id  # type: ignore

        client = self._get_ch_client()
        columns = ["_payload", "_source_endpoint", "_ingested_at", "_run_id"]
        values = [[json.dumps(row), self.endpoint, now, run_id] for row in data]

        # in PROD, REPLACINGMERGETREE + FINAL
        tmp_table = f"{self.table}__tmp"
        client.command(f"DROP TABLE IF EXISTS {self.database}.{tmp_table}")
        client.command(
            f"CREATE TABLE {self.database}.{tmp_table} AS {self.database}.{self.table}"
        )

        self.log.info(f"Inserting data into {self.database}.{tmp_table}")

        query_summary = client.insert(
            table=tmp_table,
            database=self.database,
            column_names=columns,
            data=values,
        )

        self.log.info(f"Query summary: {query_summary.summary}")
        self.log.info(
            f"Atomically swapping tables {self.database}.{tmp_table} and {self.database}.{self.table}"
        )
        client.command(
            f"EXCHANGE TABLES {self.database}.{tmp_table} AND {self.database}.{self.table}"
        )
        client.command(f"DROP TABLE {self.database}.{tmp_table}")
        self.log.info("DONE")

    def execute(self, context: Context):
        url = f"{self.BASE_URL}/{self.endpoint}"

        keys = self._get_params()
        params = {}
        if self.needs_meeting:
            params["meeting_key"] = keys.f1_meeting_key
        if self.needs_session:
            params["session_key"] = keys.f1_session_key

        params.update(self.extra_params)
        data: dict = self._get_data(url, params)
        self._insert_data_into_ch(data, context)
