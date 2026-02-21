import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

AIRFLOW_HOME = Path("/opt/airflow")

# Add /opt/airflow to PYTHONPATH
if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

# import pipeline
from scripts.bronze_ingest import bronze_ingest

default_args = {
    "owner":"chayuthpong",
    "retries":0,
    "retry_delay":timedelta(minutes=1)
}

with DAG(
    dag_id="2026_election_in_thailand_pipeline",
    default_args=default_args,
    start_date = timezone.datetime(2026,2,21),
    schedule="0 0 * * *",
    catchup=False
) as dag:
    
    bronze = PythonOperator(
        task_id = "bronze_ingest",
        python_callable = bronze_ingest
    )

    bronze