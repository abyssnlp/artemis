.PHONY: .uv .precommit lint typecheck launch-stack teardown teardown-all dbt-parse
.uv:
	@uv -V || echo "uv is not installed. Please install uv to manage dependencies."

.pre-commit: .uv
	@uv run pre-commit -V || uv pip install pre-commit

lint: .uv
	@uv run ruff check --config pyproject.toml --fix --show-fixes
	@uv run ruff format

typecheck: .uv
	@uv run pyright -p pyproject.toml

dbt-parse:
	@cd dbt/f1 && ../../.venv/bin/dbt parse --profiles-dir . --target dev

launch-stack:
	AIRFLOW_PROJ_DIR=./artemis docker compose --profile flower up -d $(if $(BUILD),--build)

f1-pool:
	@docker compose exec artemis-airflow-apiserver-1 airflow pools set f1 4 "Pool for F1 related tasks"

teardown:
	@docker compose down

teardown-all:
	@docker compose down --volumes --remove-orphans
