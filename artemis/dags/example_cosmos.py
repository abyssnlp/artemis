from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode
from cosmos.profiles import ClickhouseUserPasswordProfileMapping

DBT_PROJECT_PATH = Path("/opt/airflow/dbt/f1")

profile_config = ProfileConfig(
    profile_name="clickhouse",
    target_name="dev",
    profile_mapping=ClickhouseUserPasswordProfileMapping(
        conn_id="clickhouse_default",
        profile_args={"schema": "default"},
    ),
)

render_config = RenderConfig(
    load_method=LoadMode.DBT_MANIFEST,
)

execution_config = ExecutionConfig(
    dbt_executable_path="/home/airflow/.local/bin/dbt",
)

default_args = {"owner": "shauryarawat", "start_date": datetime(2026, 4, 4, 0, 0, 0)}

with DAG(
    dag_id="example_cosmos",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")

    example = DbtTaskGroup(
        group_id="example",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            manifest_path=DBT_PROJECT_PATH / "target" / "manifest.json",
            project_name="f1",
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=["path:models/example"],
        ),
        execution_config=execution_config,
        profile_config=profile_config,
        operator_args={"install_deps": True},
    )

    example2 = DbtTaskGroup(
        group_id="example2",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            manifest_path=DBT_PROJECT_PATH / "target" / "manifest.json",
            project_name="f1",
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=["path:models/example2"],
        ),
        execution_config=execution_config,
        profile_config=profile_config,
        operator_args={"install_deps": True},
    )

    end = EmptyOperator(task_id="end")

    start >> example >> example2 >> end  # type: ignore
