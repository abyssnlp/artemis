from __future__ import annotations

import typing
from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Asset
from airflow.utils.trigger_rule import TriggerRule
from common.utils.helpers import (
    EXECUTION_CONFIG,
    PROFILE_CONFIG,
    get_project_config,
    get_render_config,
)
from cosmos.airflow.task_group import DbtTaskGroup

default_args = {
    "owner": "shauryarawat",
    "start_date": datetime(2026, 4, 18, 0, 0, 0),
    "max_active_runs": 1,
    "max_active_tasks": 4,
    "catchup": False,
    "depends_on_past": False,
}

DATABASE_NAME: typing.Final[str] = "default"


with DAG(
    dag_id="staging_intermediate_asset",
    default_args=default_args,
    schedule=[
        Asset("raw.meetings"),
        Asset("raw.sessions"),
        Asset("raw.drivers"),
        Asset("raw.laps"),
        Asset("raw.stints"),
    ],
) as dag:
    test_start = EmptyOperator(task_id="test_start")

    staging = DbtTaskGroup(
        group_id="staging_f1",
        project_config=get_project_config("f1"),
        render_config=get_render_config(select=["path:models/staging"]),
        execution_config=EXECUTION_CONFIG,
        profile_config=PROFILE_CONFIG,
        operator_args={"install_deps": True},
    )

    intermediate = DbtTaskGroup(
        group_id="intermediate_f1",
        project_config=get_project_config("f1"),
        render_config=get_render_config(select=["path:models/intermediate"]),
        execution_config=EXECUTION_CONFIG,
        profile_config=PROFILE_CONFIG,
        operator_args={"install_deps": True},
    )

    test_end = EmptyOperator(task_id="test_end", trigger_rule=TriggerRule.ALL_SUCCESS)

    test_start >> staging >> intermediate >> test_end  # type: ignore
