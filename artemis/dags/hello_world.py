from datetime import datetime

from airflow.models import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Variable


default_args = {"owner": "shauryarawat", "start_date": datetime(2026, 4, 4, 0, 0, 0)}

test_variable: str = Variable.get("airflow_env")


def hello_world_loop():
    for word in ["hello", "world"]:
        print(word)
        print(test_variable)


with DAG(dag_id="hello_world", default_args=default_args, schedule="@once") as dag:

    test_start = EmptyOperator(task_id="test_start")

    test_python = PythonOperator(
        task_id="test_python", python_callable=hello_world_loop
    )

    test_bash = BashOperator(task_id="test_bash", bash_command="echo Hello World!")

test_start >> test_python >> test_bash  # type: ignore
