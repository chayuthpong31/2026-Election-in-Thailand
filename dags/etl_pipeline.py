import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.providers.postgres.operators.postgres import PostgresOperator

AIRFLOW_HOME = Path("/opt/airflow")

# Add /opt/airflow to PYTHONPATH
if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

# import pipeline
from scripts.bronze_ingest import bronze_ingest
from scripts.silver_transform import silver_transform
from scripts.gold_aggregate import gold_aggregate
from scripts.load_to_postgres import load_to_postgres

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

    silver = PythonOperator(
        task_id = "silver_transform",
        python_callable = silver_transform
    )

    gold = PythonOperator(
        task_id = "gold_aggregate",
        python_callable = gold_aggregate
    )

    load_data = PythonOperator(
        task_id = "load_to_postgres",
        python_callable = load_to_postgres
    )
    
    truncate_tables = PostgresOperator(
        task_id="truncate_database_tables",
        postgres_conn_id="postgres_election", # ต้องไปตั้งค่า Connection ใน Airflow UI
        sql="""
            TRUNCATE 
                election_db.fact_vote_party, 
                election_db.fact_vote_constituency, 
                election_db.dim_mp_candidate, 
                election_db.dim_party_candidate,
                election_db.dim_zone, 
                election_db.dim_constituency, 
                election_db.dim_province, 
                election_db.dim_party 
            CASCADE;
        """,
    )
    bronze >> silver >> gold >> truncate_tables >> load_data