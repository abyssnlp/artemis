from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow.sdk.definitions.asset import AssetAlias
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


with DAG(
    dag_id="marts_asset",
    default_args=default_args,
    schedule=[
        AssetAlias("staging_intermediate_asset__int_laps_with_stint__run"),
    ],
) as dag:
    test_start = EmptyOperator(task_id="test_start")

    marts = DbtTaskGroup(
        group_id="marts_f1",
        project_config=get_project_config("f1"),
        render_config=get_render_config(select=["path:models/marts"]),
        execution_config=EXECUTION_CONFIG,
        profile_config=PROFILE_CONFIG,
        operator_args={"install_deps": True},
    )

    test_end = EmptyOperator(task_id="test_end", trigger_rule=TriggerRule.ALL_SUCCESS)

    test_start >> marts >> test_end  # type: ignore
