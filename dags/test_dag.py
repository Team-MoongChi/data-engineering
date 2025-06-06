import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator


def print_hello():
    print("Hello")


def print_goodbye():
    print("Goodbye")


with DAG(
        dag_id="test_dag",
        start_date=datetime.datetime(2025, 6, 4),
        schedule="@daily",
        catchup=False,  # 과거 실행 방지
) as dag:
    # 시작 태스크
    start = EmptyOperator(
        task_id="start"
    )

    # Python 함수를 실행하는 태스크들
    hello = PythonOperator(
        task_id="hello",
        python_callable=print_hello
    )

    goodbye = PythonOperator(
        task_id="goodbye",
        python_callable=print_goodbye
    )

    # 종료 태스크
    end = EmptyOperator(
        task_id="end"
    )

    # 태스크 의존성 설정
    start >> hello >> goodbye >> end