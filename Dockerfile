FROM apache/airflow:3.0.6
RUN pip install uv
ADD pyproject.toml constraints-3.12.txt uv.lock README.md ./
RUN uv pip compile pyproject.toml --constraint constraints-3.12.txt --no-header --output-file requirements.txt
RUN pip install -r requirements.txt --constraint constraints-3.12.txt