from __future__ import annotations

import typing
from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset
from airflow.utils.trigger_rule import TriggerRule
from common.components.f1_ingest import F1IngestOperator, ensure_raw_tables
from common.components.f1_validate import F1ValidateOperator

default_args = {
    "owner": "shauryarawat",
    "start_date": datetime(2026, 4, 18, 0, 0, 0),
    "max_active_runs": 1,
    "max_active_tasks": 4,
    "catchup": False,
    "depends_on_past": False,
}

DATABASE_NAME: typing.Final[str] = "raw"


with DAG(dag_id="f1_with_asset", default_args=default_args, schedule="@once") as dag:
    test_start = EmptyOperator(task_id="test_start")

    # this can also be moved inside the endpoint loop
    setup_raw_tables = PythonOperator(
        task_id="setup_raw_tables",
        python_callable=ensure_raw_tables,
        op_kwargs={"database": DATABASE_NAME},
    )

    endpoints = ["meetings", "sessions", "drivers", "laps", "stints"]
    ingest_tasks = []
    validate_tasks = []

    for endpoint in endpoints:
        ingest_task = F1IngestOperator(
            task_id=f"ingest_{endpoint}",
            endpoint=endpoint,
            database=DATABASE_NAME,
            table=endpoint,
            needs_meeting=True if endpoint == "meetings" else False,
            needs_session=True if endpoint != "meetings" else False,
            extra_params={"year": 2026} if endpoint == "meetings" else {},
            pool="f1",
        )

        validate_task = F1ValidateOperator(
            task_id=f"validate_{endpoint}",
            conn_id="clickhouse_default",
            endpoint=endpoint,
            database=DATABASE_NAME,
            table=endpoint,
            batch_size=1_000,
            max_logged_errors=10,
            failure_threshold=100,
            strict=False,
            outlets=[Asset(f"{DATABASE_NAME}.{endpoint}")],
        )

        ingest_task >> validate_task  # type: ignore
        ingest_tasks.append(ingest_task)
        validate_tasks.append(validate_task)

    all_validated = EmptyOperator(
        task_id="all_validated",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    test_end = EmptyOperator(task_id="test_end", trigger_rule=TriggerRule.ALL_SUCCESS)

    test_start >> setup_raw_tables >> ingest_tasks  # type: ignore
    validate_tasks >> all_validated >> test_end  # type: ignore
