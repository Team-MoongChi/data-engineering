from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import os, sys

# 패키지 오류 해결 - /opt/airflow가 Python 경로에 추가되어서 /opt/airflow/plugins/ 폴더의 모듈들을 import 할 수 있게 됨
sys.path.append('/opt/airflow')
from plugins.naver_shopping.shopping_data_collector import ShoppingDataCollector


def fetch_naver_shopping_data(**context):
    """네이버 쇼핑 데이터 수집 메인 함수"""
    collector = ShoppingDataCollector()

    # 데이터 수집
    collection_result = collector.collect_all_categories()

    # 데이터 처리 및 저장
    summary = collector.process_and_save(collection_result)

    return summary


with DAG(
        dag_id="naver_shopping_pipeline",
        start_date=datetime(2025, 6, 4),
        schedule="@once",
        catchup=False,
) as dag:
    collect_task = PythonOperator(
        task_id='collect_naver_shopping_data',
        python_callable=fetch_naver_shopping_data
    )
