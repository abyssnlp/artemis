FROM apache/airflow:3.0.6
RUN pip install uv
ADD pyproject.toml uv.lock ./
