
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.config import POSTGRES_CONN_ID, POSTGRES_INGEST_TABLES

from include.alert_utils import slack_alert

default_args = {
    "owner": "airflow",
    "on_failure_callback": slack_alert,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id = "postgres_ingest_monitor",
    default_args = default_args,
    start_date = datetime(2025, 6, 1),
    schedule_interval = "*/10 * * * *",
    catchup = False,
    tags = ["postgres", "monitoring"],
) as dag:
    def check_recent_ingest(**context):
        """
        Ensure each table has new rows since last check
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        for table in POSTGRES_INGEST_TABLES:
            var_key = f"pg_prev_max_{table}"
            prev = int(context['ti'].xcom_pull(key=var_key) or 0)
            sql = f"SELECT COUNT(*) FROM {table};"
            current = hook.get_first(sql)[0] or 0
            if current <= prev:
                raise AirflowException(
                    f"No new rows in {table} (prev_max={prev}, now={current})"
                )
            
            context['ti'].xcom_push(key=var_key, value=current)
    
    t1_ingest = PythonOperator(
        task_id = "check_recent_ingest",
        python_callable = check_recent_ingest,
        provide_context = True
    )

    t1_ingest

