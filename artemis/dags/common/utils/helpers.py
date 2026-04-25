from __future__ import annotations

from pathlib import Path

from cosmos.config import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode
from cosmos.profiles import ClickhouseUserPasswordProfileMapping

DBT_PROJECT_PATH = Path("/opt/airflow/dbt/f1")

PROFILE_CONFIG = ProfileConfig(
    profile_name="clickhouse",
    target_name="dev",
    profile_mapping=ClickhouseUserPasswordProfileMapping(
        conn_id="clickhouse_default",
        profile_args={"schema": "default", "driver": "http"},
    ),
)


EXECUTION_CONFIG = ExecutionConfig(
    dbt_executable_path="/home/airflow/.local/bin/dbt",
)


def get_render_config(select: list[str] | None = None) -> RenderConfig:
    return RenderConfig(
        load_method=LoadMode.DBT_MANIFEST,
        select=select or [],
    )


def get_project_config(project_name: str) -> ProjectConfig:
    return ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
        manifest_path=DBT_PROJECT_PATH / "target" / "manifest.json",
        project_name=project_name,
    )
