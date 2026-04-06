.PHONY: .uv .precommit lint typecheck launch-stack teardown teardown-all
.uv:
	@uv -V || echo "uv is not installed. Please install uv to manage dependencies."

.pre-commit: .uv
	@uv run pre-commit -V || uv pip install pre-commit

lint: .uv
	@uv run ruff check --config pyproject.toml --fix --show-fixes
	@uv run ruff format

typecheck: .uv
	@uv run pyright -p pyproject.toml

launch-stack:
	AIRFLOW_PROJ_DIR=./artemis @docker compose --profile flower up -d

teardown:
	@docker compose down

teardown-all:
	@docker compose down --volumes --remove-orphans
